import FileProvider
import Foundation
import Testing

@testable import CascadeFileProviderKit

private func write(_ text: String, to url: URL) throws {
    try Data(text.utf8).write(to: url)
}

private func contents(of url: URL) -> String? {
    (try? Data(contentsOf: url)).map { String(decoding: $0, as: UTF8.self) }
}

private func exists(_ url: URL) -> Bool {
    FileManager.default.fileExists(atPath: url.path)
}

@Test(.timeLimit(.minutes(1)))
func aFetchMovesTheStagedFileIntoTheGivenTemporaryDirectory() async throws {
    let staging = try scratchDirectory()
    let temporary = try scratchDirectory()
    let staged = staging.appendingPathComponent("object-42")
    let daemon = try FakeDaemon { request in
        guard request.method == "fetch" else { return .error(code: "featureUnsupported") }
        return .result([
            "path": staged.path,
            "item": wireItem(uid: "42", parentUID: "root", name: "report.pdf", size: 10, contentVersion: "c7"),
        ])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath, stagingDirectory: staging)

    try write("whole file", to: staged)
    let (url, item) = try await client.fetch(NSFileProviderItemIdentifier("42"), requestedVersion: nil, into: temporary)

    #expect(sameDirectory(url.deletingLastPathComponent(), temporary), "\(url.path)")
    #expect(contents(of: url) == "whole file")
    #expect(!exists(staged), "the staged file is moved, not copied")
    #expect(item.itemIdentifier == NSFileProviderItemIdentifier("42"))
    #expect(item.itemVersion.contentVersion == Data("c7".utf8))
    let first = try #require(daemon.requests.last)
    #expect(first.params["uid"] as? String == "42")
    #expect((first.params["op_id"] as? String)?.isEmpty == false)
    #expect(first.params["requested_version"] as? String == nil)

    try write("whole file again", to: staged)
    let version = NSFileProviderItemVersion(contentVersion: Data("c7".utf8), metadataVersion: Data("m3".utf8))
    let (again, _) = try await client.fetch(
        NSFileProviderItemIdentifier("42"), requestedVersion: version, into: temporary
    )
    #expect(again != url, "a second fetch into the same directory keeps the first")
    #expect(contents(of: again) == "whole file again")
    #expect(contents(of: url) == "whole file")
    let second = try #require(daemon.requests.last)
    #expect(second.params["requested_version"] as? String == "c7")
    #expect(second.params["op_id"] as? String != first.params["op_id"] as? String)
}

@Test(.timeLimit(.minutes(1)))
func fetchRangeReturnsTheDaemonsAlignedRangeAndAStrictFetchOfAGoneVersionFails() async throws {
    let staging = try scratchDirectory()
    let temporary = try scratchDirectory()
    let staged = staging.appendingPathComponent("clone-42")
    let sixGiB = 6 << 30
    let daemon = try FakeDaemon { request in
        guard request.method == "fetchRange" else { return .error(code: "featureUnsupported") }
        if request.params["strict"] as? Bool == true {
            return .error(code: "versionNoLongerAvailable")
        }
        return .result([
            "path": staged.path,
            "retrieved_offset": sixGiB,
            "retrieved_length": 16 << 20,
            "item": wireItem(uid: "42", parentUID: "root", name: "movie.mkv", size: 8 << 30),
        ])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath, stagingDirectory: staging)

    try write("clone", to: staged)
    let partial = try await client.fetchRange(
        NSFileProviderItemIdentifier("42"),
        requestedVersion: nil,
        strict: false,
        range: NSRange(location: sixGiB + 4096, length: 1 << 20),
        alignment: 16384,
        into: temporary
    )

    #expect(partial.range == NSRange(location: sixGiB, length: 16 << 20))
    #expect(sameDirectory(partial.url.deletingLastPathComponent(), temporary), "\(partial.url.path)")
    #expect(contents(of: partial.url) == "clone")
    #expect(!exists(staged))
    #expect(partial.item.itemIdentifier == NSFileProviderItemIdentifier("42"))
    let request = try #require(daemon.requests.last)
    #expect(request.params["uid"] as? String == "42")
    #expect(request.params["offset"] as? Int == sixGiB + 4096)
    #expect(request.params["length"] as? Int == 1 << 20)
    #expect(request.params["alignment"] as? Int == 16384)
    #expect(request.params["strict"] as? Bool == false)

    let gone = NSFileProviderItemVersion(contentVersion: Data("c6".utf8), metadataVersion: Data("m6".utf8))
    let error = await thrown {
        _ = try await client.fetchRange(
            NSFileProviderItemIdentifier("42"),
            requestedVersion: gone,
            strict: true,
            range: NSRange(location: 0, length: 4096),
            alignment: 16384,
            into: temporary
        )
    }
    #expect(isFileProviderError(error, .versionNoLongerAvailable), "\(String(describing: error))")
    let strict = try #require(daemon.requests.last)
    #expect(strict.params["strict"] as? Bool == true)
    #expect(strict.params["requested_version"] as? String == "c6")
}

@Test(.timeLimit(.minutes(1)))
func cancellingTheProgressOfARunningFetchSendsCancelForItsOperationAndCompletesOnce() async throws {
    let temporary = try scratchDirectory()
    let daemon = try FakeDaemon { request in
        request.method == "fetch" ? .withhold : .result([:])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath, stagingDirectory: try scratchDirectory())
    let completions = Locked<[Result<Void, any Error>]>([])

    let progress = completeOnce({ result in completions.update { $0.append(result.map { _ in () }) } }) {
        try await client.fetch(NSFileProviderItemIdentifier("42"), requestedVersion: nil, into: temporary)
    }
    #expect(await eventually { daemon.requests.contains { $0.method == "fetch" } }, "the fetch reached the daemon")
    let fetch = try #require(daemon.requests.first { $0.method == "fetch" })

    progress.cancel()

    #expect(await eventually { daemon.requests.contains { $0.method == "cancel" } }, "cancel reached the daemon")
    let cancel = daemon.requests.first { $0.method == "cancel" }
    #expect(cancel?.params["op_id"] as? String == fetch.params["op_id"] as? String)
    #expect(await eventually { !completions.value.isEmpty })
    if case let .failure(error)? = completions.value.first {
        let error = error as NSError
        #expect(error.domain == NSCocoaErrorDomain && error.code == CocoaError.Code.userCancelled.rawValue, "\(error)")
    } else {
        Issue.record("the cancelled fetch completed with \(String(describing: completions.value.first))")
    }

    daemon.answer(fetch.id, with: .error(code: "userCancelled"))
    try await Task.sleep(for: .milliseconds(200))
    #expect(completions.value.count == 1, "the completion handler runs exactly once")
}

@Test(.timeLimit(.minutes(1)))
func aRenameTheDaemonRefusesReturnsTheItemUnderItsCurrentName() async throws {
    let daemon = try FakeDaemon { request in
        guard request.method == "modify" else { return .error(code: "featureUnsupported") }
        return .result([
            "item": wireItem(uid: "42", parentUID: "root", name: "report.pdf", metadataVersion: "m4"),
            "fetch_content": false,
        ])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath, stagingDirectory: try scratchDirectory())

    let item = try await client.modify(
        EditedItem(NSFileProviderItemIdentifier("42"), parent: .rootContainer, filename: "a?b"),
        changedFields: [.filename],
        contents: nil
    )

    #expect(item.itemIdentifier == NSFileProviderItemIdentifier("42"))
    #expect(item.filename == "report.pdf")
    let request = try #require(daemon.requests.last)
    #expect(request.params["uid"] as? String == "42")
    #expect(request.params["name"] as? String == "a?b", "the rename is the daemon's to refuse")
    #expect(request.params["contents_path"] as? String == nil)
}

@Test(.timeLimit(.minutes(1)))
func modifiedContentsAreStagedForTheDaemonAndTheStagedCopyRemovedAfterTheReply() async throws {
    let staging = try scratchDirectory()
    let edited = try scratchDirectory().appendingPathComponent("notes.txt")
    try write("new contents", to: edited)
    let seen = Locked<(path: String?, contents: String?)>((nil, nil))
    let daemon = try FakeDaemon { request in
        let path = request.params["contents_path"] as? String
        seen.update { $0 = (path, path.flatMap { contents(of: URL(fileURLWithPath: $0)) }) }
        return .result([
            "item": wireItem(uid: "42", parentUID: "root", name: "notes.txt", contentVersion: "c8"),
            "fetch_content": false,
        ])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath, stagingDirectory: staging)

    let item = try await client.modify(
        EditedItem(NSFileProviderItemIdentifier("42"), parent: .rootContainer, filename: "notes.txt"),
        changedFields: [.contents, .contentModificationDate],
        contents: edited
    )

    #expect(item.itemVersion.contentVersion == Data("c8".utf8))
    let stagedPath = try #require(seen.value.path, "modify carries contents_path")
    let staged = URL(fileURLWithPath: stagedPath)
    #expect(sameDirectory(staged.deletingLastPathComponent(), staging), "staged in the App Group: \(stagedPath)")
    #expect(seen.value.contents == "new contents", "the daemon reads the edited bytes at the staged path")
    #expect(!exists(staged), "the staged copy is removed after the reply")
    #expect(contents(of: edited) == "new contents", "the system's file is left in place")
}

private func isUserCancelled(_ result: Result<Void, any Error>?) -> Bool {
    guard case let .failure(error)? = result else { return false }
    let nsError = error as NSError
    return nsError.domain == NSCocoaErrorDomain && nsError.code == CocoaError.Code.userCancelled.rawValue
}

private func isEmptyDirectory(_ directory: URL) -> Bool {
    (try? FileManager.default.contentsOfDirectory(atPath: directory.path))?.isEmpty == true
}

@Test(.timeLimit(.minutes(1)))
func aFetchCancelledWhileTheConnectionOpensIsNotSentOrIsCancelledAndLeavesNoFile() async throws {
    let staging = try scratchDirectory()
    let temporary = try scratchDirectory()
    let staged = staging.appendingPathComponent("object-42")
    try write("whole file", to: staged)
    let greeting = DispatchSemaphore(value: 0)
    let daemon = try FakeDaemon(hello: { _ in
        greeting.wait()
        return .hello(version: 2)
    }) { request in
        guard request.method == "fetch" else { return .result([:]) }
        return .result([
            "path": staged.path,
            "item": wireItem(uid: "42", parentUID: "root", name: "report.pdf", size: 10),
        ])
    }
    defer { daemon.stop() }
    defer { greeting.signal() }
    let client = DaemonClient(socketPath: daemon.socketPath, stagingDirectory: staging)
    let completions = Locked<[Result<Void, any Error>]>([])

    let progress = completeOnce({ result in completions.update { $0.append(result.map { _ in () }) } }) {
        try await client.fetch(NSFileProviderItemIdentifier("42"), requestedVersion: nil, into: temporary)
    }
    #expect(await eventually { daemon.hellos.count == 1 }, "the connection is being greeted")

    progress.cancel()
    greeting.signal()
    try await Task.sleep(for: .milliseconds(300))

    let methods = daemon.requests.map(\.method)
    let fetch = daemon.requests.first { $0.method == "fetch" }
    let cancel = daemon.requests.first { $0.method == "cancel" }
    if let fetch {
        #expect(
            methods.firstIndex(of: "fetch") ?? 0 < methods.firstIndex(of: "cancel") ?? -1,
            "a sent fetch is followed by cancel: \(methods)"
        )
        #expect(cancel?.params["op_id"] as? String == fetch.params["op_id"] as? String)
    } else {
        #expect(cancel == nil, "nothing to cancel when the fetch was never sent: \(methods)")
    }
    #expect(isEmptyDirectory(temporary), "no file is left in the temporary directory")
    #expect(completions.value.count == 1)
    #expect(isUserCancelled(completions.value.first), "\(String(describing: completions.value.first))")
}

@Test(.timeLimit(.minutes(1)))
func aLateReplyForACancelledFetchRemovesTheStagedFileAndLeavesNoFile() async throws {
    let staging = try scratchDirectory()
    let temporary = try scratchDirectory()
    let staged = staging.appendingPathComponent("object-42")
    try write("whole file", to: staged)
    let daemon = try FakeDaemon { request in
        request.method == "fetch" ? .withhold : .result([:])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath, stagingDirectory: staging)
    let completions = Locked<[Result<Void, any Error>]>([])

    let progress = completeOnce({ result in completions.update { $0.append(result.map { _ in () }) } }) {
        try await client.fetch(NSFileProviderItemIdentifier("42"), requestedVersion: nil, into: temporary)
    }
    #expect(await eventually { daemon.requests.contains { $0.method == "fetch" } }, "the fetch reached the daemon")
    let fetch = try #require(daemon.requests.first { $0.method == "fetch" })

    progress.cancel()
    #expect(await eventually { daemon.requests.contains { $0.method == "cancel" } }, "cancel reached the daemon")
    daemon.answer(fetch.id, with: .result([
        "path": staged.path,
        "item": wireItem(uid: "42", parentUID: "root", name: "report.pdf", size: 10),
    ]))

    #expect(await eventually { !exists(staged) }, "the staged file of the cancelled fetch is removed")
    try await Task.sleep(for: .milliseconds(100))
    #expect(isEmptyDirectory(temporary), "no file is moved into the temporary directory")
    #expect(completions.value.count == 1)
    #expect(isUserCancelled(completions.value.first), "\(String(describing: completions.value.first))")
}
