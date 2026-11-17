import FileProvider
import Foundation
import Testing
import UniformTypeIdentifiers

@testable import CascadeFileProviderKit

private let allowedDomains: Set<String> = [NSCocoaErrorDomain, NSFileProviderError.errorDomain]

private func utc(_ year: Int, _ month: Int, _ day: Int, _ hour: Int, _ minute: Int) -> Date {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(identifier: "UTC")!
    return calendar.date(
        from: DateComponents(year: year, month: month, day: day, hour: hour, minute: minute)
    )!
}

@Test(.timeLimit(.minutes(1)))
func itemsMapVersionsPolicyCapabilitiesAndTheRootUid() async throws {
    let daemon = try FakeDaemon { request in
        switch request.params["uid"] as? String {
        case "root":
            .result(["item": wireItem(
                uid: "root", parentUID: "root", name: "", kind: "folder",
                contentVersion: "", metadataVersion: "",
                capabilities: ["allowsReading", "allowsContentEnumerating"]
            )])
        case "42":
            .result(["item": wireItem(
                uid: "42", parentUID: "root", name: "report.pdf", size: 4096,
                mtime: "2026-05-28T06:20:00.250Z", contentVersion: "c7", metadataVersion: "m3",
                pinned: true, uploaded: false,
                capabilities: ["allowsReading", "allowsWriting", "allowsRenaming", "allowsReparenting"]
            )])
        default:
            .result(["item": wireItem(
                uid: "43", parentUID: "7", name: "Photos", kind: "folder",
                contentVersion: "c9", metadataVersion: "m9", pinned: false, uploaded: true,
                capabilities: ["allowsReading", "allowsContentEnumerating", "allowsAddingSubItems"]
            )])
        }
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)

    let root = try await client.item(.rootContainer)
    #expect(daemon.requests.map(\.method) == ["item"])
    #expect(daemon.requests.first?.params["uid"] as? String == "root")
    #expect(root.itemIdentifier == .rootContainer)
    #expect(root.parentItemIdentifier == .rootContainer)

    let file = try await client.item(NSFileProviderItemIdentifier("42"))
    #expect(daemon.requests.last?.params["uid"] as? String == "42")
    #expect(file.itemIdentifier == NSFileProviderItemIdentifier("42"))
    #expect(file.parentItemIdentifier == .rootContainer)
    #expect(file.filename == "report.pdf")
    #expect(file.documentSize == 4096)
    #expect(file.contentModificationDate == utc(2026, 5, 28, 6, 20).addingTimeInterval(0.25))
    #expect(file.itemVersion.contentVersion == Data("c7".utf8))
    #expect(file.itemVersion.metadataVersion == Data("m3".utf8))
    #expect(file.contentPolicy == .downloadEagerlyAndKeepDownloaded)
    #expect(file.capabilities == [.allowsReading, .allowsWriting, .allowsRenaming, .allowsReparenting])
    #expect(file.isUploaded == false)
    #expect(file.userInfo?["pinned"] as? Bool == true)

    let folder = try await client.item(NSFileProviderItemIdentifier("43"))
    #expect(folder.parentItemIdentifier == NSFileProviderItemIdentifier("7"))
    #expect(folder.contentType == .folder)
    #expect(folder.itemVersion.contentVersion == Data("c9".utf8))
    #expect(folder.contentPolicy == .inherited)
    #expect(folder.capabilities == [.allowsReading, .allowsContentEnumerating, .allowsAddingSubItems])
    #expect(folder.isUploaded == true)
    #expect(folder.userInfo?["pinned"] as? Bool == false)
}

private final class Template: NSObject, NSFileProviderItem {
    let itemIdentifier = NSFileProviderItemIdentifier("new")
    let parentItemIdentifier: NSFileProviderItemIdentifier = .rootContainer
    let filename: String
    let contentType: UTType

    init(filename: String, contentType: UTType) {
        self.filename = filename
        self.contentType = contentType
    }
}

/// Every protocol v2 error code and the Apple error it is answered with.
private let errorTable: [(code: String, domain: String, value: Int)] = [
    ("notAuthenticated", NSFileProviderError.errorDomain, NSFileProviderError.Code.notAuthenticated.rawValue),
    ("filenameCollision", NSFileProviderError.errorDomain, NSFileProviderError.Code.filenameCollision.rawValue),
    ("syncAnchorExpired", NSFileProviderError.errorDomain, NSFileProviderError.Code.syncAnchorExpired.rawValue),
    ("pageExpired", NSFileProviderError.errorDomain, NSFileProviderError.Code.pageExpired.rawValue),
    ("serverUnreachable", NSFileProviderError.errorDomain, NSFileProviderError.Code.serverUnreachable.rawValue),
    ("noSuchItem", NSFileProviderError.errorDomain, NSFileProviderError.Code.noSuchItem.rawValue),
    ("deletionRejected", NSFileProviderError.errorDomain, NSFileProviderError.Code.deletionRejected.rawValue),
    ("directoryNotEmpty", NSFileProviderError.errorDomain, NSFileProviderError.Code.directoryNotEmpty.rawValue),
    ("cannotSynchronize", NSFileProviderError.errorDomain, NSFileProviderError.Code.cannotSynchronize.rawValue),
    ("nonEvictable", NSFileProviderError.errorDomain, NSFileProviderError.Code.nonEvictable.rawValue),
    (
        "versionNoLongerAvailable", NSFileProviderError.errorDomain,
        NSFileProviderError.Code.versionNoLongerAvailable.rawValue
    ),
    ("excludedFromSync", NSFileProviderError.errorDomain, NSFileProviderError.Code.excludedFromSync.rawValue),
    (
        "olderExtensionVersionRunning", NSFileProviderError.errorDomain,
        NSFileProviderError.Code.olderExtensionVersionRunning.rawValue
    ),
    (
        "newerExtensionVersionFound", NSFileProviderError.errorDomain,
        NSFileProviderError.Code.newerExtensionVersionFound.rawValue
    ),
    ("fileWriteOutOfSpace", NSCocoaErrorDomain, CocoaError.Code.fileWriteOutOfSpace.rawValue),
    ("userCancelled", NSCocoaErrorDomain, CocoaError.Code.userCancelled.rawValue),
    ("featureUnsupported", NSCocoaErrorDomain, CocoaError.Code.featureUnsupported.rawValue),
]

@Test(.timeLimit(.minutes(1)))
func oneConnectionCarriesRequestsMatchedByIdAndEveryErrorLandsInAnAppleDomain() async throws {
    let daemon = try FakeDaemon(batchSize: 3) { request in
        if request.method == "create" { return .error(code: "excludedFromSync") }
        let uid = request.params["uid"] as? String ?? ""
        switch uid {
        case "1", "2", "3":
            return .result(["item": wireItem(uid: uid, parentUID: "root", name: "item-\(uid)")])
        case "hangUp":
            return .hangUp
        default:
            return .error(code: uid)
        }
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)

    let answered = try await withThrowingTaskGroup(of: (String, String).self) { group in
        for uid in ["1", "2", "3"] {
            group.addTask {
                let item = try await client.item(NSFileProviderItemIdentifier(uid))
                return (uid, item.filename)
            }
        }
        return try await group.reduce(into: [String: String]()) { $0[$1.0] = $1.1 }
    }
    #expect(answered == ["1": "item-1", "2": "item-2", "3": "item-3"])
    #expect(Set(daemon.requests.map(\.id)).count == 3, "each request carries its own id")

    for expected in errorTable {
        do {
            _ = try await client.item(NSFileProviderItemIdentifier(expected.code))
            Issue.record("\(expected.code) answered without an error")
        } catch {
            let error = error as NSError
            #expect(error.domain == expected.domain, "domain of \(expected.code)")
            #expect(error.code == expected.value, "code of \(expected.code)")
        }
    }
    do {
        _ = try await client.item(NSFileProviderItemIdentifier("aCodeFromANewerDaemon"))
        Issue.record("an unknown code answered without an error")
    } catch {
        #expect(allowedDomains.contains((error as NSError).domain), "\(error)")
    }

    let excludedFromSync = NSFileProviderError.Code.excludedFromSync.rawValue
    do {
        _ = try await client.create(
            Template(filename: "link", contentType: .symbolicLink), contents: nil, mayAlreadyExist: false
        )
        Issue.record("a symlink create succeeded")
    } catch {
        let error = error as NSError
        #expect(error.domain == NSFileProviderError.errorDomain && error.code == excludedFromSync)
    }
    #expect(!daemon.requests.contains { $0.method == "create" }, "a symlink never reaches the daemon")

    do {
        _ = try await client.create(
            Template(filename: "a?b", contentType: .folder), contents: nil, mayAlreadyExist: false
        )
        Issue.record("a create of a rejected name succeeded")
    } catch {
        let error = error as NSError
        #expect(error.domain == NSFileProviderError.errorDomain && error.code == excludedFromSync)
    }
    let create = try #require(daemon.requests.last { $0.method == "create" })
    #expect(create.params["parent_uid"] as? String == "root")
    #expect(create.params["name"] as? String == "a?b")
    #expect(create.params["kind"] as? String == "folder")
    #expect(create.params["may_already_exist"] as? Bool == false)
    #expect((create.params["op_id"] as? String)?.isEmpty == false)
    #expect(daemon.connections == 1, "every request so far shares one connection")

    do {
        _ = try await client.item(NSFileProviderItemIdentifier("hangUp"))
        Issue.record("a request the daemon dropped succeeded")
    } catch {
        #expect(allowedDomains.contains((error as NSError).domain), "\(error)")
    }
}

private final class Recorder: @unchecked Sendable {
    private let lock = NSLock()
    private var results: [Result<String, any Error>] = []
    private let stream: AsyncStream<Result<String, any Error>>
    private let continuation: AsyncStream<Result<String, any Error>>.Continuation

    init() {
        (stream, continuation) = AsyncStream.makeStream()
    }

    func record(_ result: Result<String, any Error>) {
        lock.withLock { results.append(result) }
        continuation.yield(result)
    }

    var count: Int {
        lock.withLock { results.count }
    }

    /// The first completion, or nil when none arrives within five seconds.
    func first() async -> Result<String, any Error>? {
        let stream = stream
        return await withTaskGroup(of: Result<String, any Error>?.self) { group in
            group.addTask {
                var iterator = stream.makeAsyncIterator()
                return await iterator.next()
            }
            group.addTask {
                try? await Task.sleep(for: .seconds(5))
                return nil
            }
            let first = await group.next() ?? nil
            group.cancelAll()
            return first
        }
    }
}

/// An await that ignores task cancellation until opened.
private final class Gate: @unchecked Sendable {
    private let lock = NSLock()
    private var opened = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        await withCheckedContinuation { waiter in
            let resumeNow = lock.withLock {
                if opened { return true }
                waiters.append(waiter)
                return false
            }
            if resumeNow { waiter.resume() }
        }
    }

    func open() {
        let pending = lock.withLock {
            opened = true
            defer { waiters.removeAll() }
            return waiters
        }
        pending.forEach { $0.resume() }
    }
}

@Test(.timeLimit(.minutes(1)))
func aCompletionHandlerRunsExactlyOnceIncludingUnderCancellation() async throws {
    let succeeded = Recorder()
    _ = completeOnce({ succeeded.record($0) }) { "done" }
    #expect(try await succeeded.first()?.get() == "done")

    let failed = Recorder()
    struct Unmapped: Error {}
    _ = completeOnce({ failed.record($0) }) { () async throws -> String in throw Unmapped() }
    switch await failed.first() {
    case let .failure(error)?:
        #expect(allowedDomains.contains((error as NSError).domain), "\(error)")
    default:
        Issue.record("a thrown error did not fail the completion")
    }

    let cancelled = Recorder()
    let started = Gate()
    let release = Gate()
    let progress = completeOnce({ cancelled.record($0) }) {
        started.open()
        await release.wait()
        return "late"
    }
    await started.wait()
    progress.cancel()
    switch await cancelled.first() {
    case let .failure(error)?:
        let error = error as NSError
        #expect(error.domain == NSCocoaErrorDomain && error.code == CocoaError.Code.userCancelled.rawValue)
    default:
        Issue.record("cancellation did not complete with userCancelled")
    }
    release.open()

    let racing = (0 ..< 200).map { _ in Recorder() }
    for recorder in racing {
        let progress = completeOnce({ recorder.record($0) }) { "raced" }
        progress.cancel()
        _ = await recorder.first()
    }

    try await Task.sleep(for: .milliseconds(200))
    #expect(succeeded.count == 1)
    #expect(failed.count == 1)
    #expect(cancelled.count == 1, "the operation finishing after cancellation completes again")
    #expect(racing.allSatisfy { $0.count == 1 }, "a completion racing a cancel runs twice")
}

@Test(.timeLimit(.minutes(1)))
func cancelReturnsOnlyAfterTheCompletionRanWithUserCancelledAndTheOperationIsCancelled() async throws {
    for round in 0 ..< 20 {
        let recorder = Recorder()
        let started = Gate()
        let release = Gate()
        let cancelledWhenReleased = Locked<Bool?>(nil)
        let progress = completeOnce({ recorder.record($0) }) {
            started.open()
            await release.wait()
            cancelledWhenReleased.update { $0 = Task.isCancelled }
            return "late"
        }
        await started.wait()

        progress.cancel()
        let completedAtCancel = recorder.count
        release.open()

        #expect(completedAtCancel == 1, "round \(round): the completion had not run when cancel returned")
        switch await recorder.first() {
        case let .failure(error)?:
            let error = error as NSError
            #expect(error.domain == NSCocoaErrorDomain && error.code == CocoaError.Code.userCancelled.rawValue)
        default:
            Issue.record("round \(round): cancellation did not complete with userCancelled")
        }
        #expect(await eventually { cancelledWhenReleased.value != nil })
        #expect(cancelledWhenReleased.value == true, "round \(round): the operation ran on uncancelled")
    }
}
