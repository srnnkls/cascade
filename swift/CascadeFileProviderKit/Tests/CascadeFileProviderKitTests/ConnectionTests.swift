import FileProvider
import Foundation
import Testing

@testable import CascadeFileProviderKit

private func progressFrame(done: Int, total: Int, finished: Bool) -> [String: Any] {
    ["op_id": "op-1", "done": done, "total": total, "finished": finished]
}

@Test(.timeLimit(.minutes(1)))
func theProgressStreamDeliversFramesAfterTheAcknowledgementInOrderAndEndsAfterTheFinishedFrame() async throws {
    let daemon = try FakeDaemon { request in
        guard request.method == "progress" else { return .error(code: "featureUnsupported") }
        return .frames([
            [:],
            progressFrame(done: 0, total: 4096, finished: false),
            progressFrame(done: 2048, total: 4096, finished: false),
            progressFrame(done: 4096, total: 4096, finished: true),
            progressFrame(done: 9999, total: 9999, finished: false),
        ])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)

    let outcome = try await drain(client.progress(of: "op-1"))

    #expect(outcome.elements.map { "\($0.done)/\($0.total)/\($0.finished)" } == [
        "0/4096/false", "2048/4096/false", "4096/4096/true",
    ])
    #expect(outcome.ended && outcome.error == nil, "the stream ends after the finished frame")
    #expect(daemon.requests.first?.params["op_id"] as? String == "op-1")
}

@Test(.timeLimit(.minutes(1)))
func subscribeDecodesEverySignalKindAndAHangUpEndsItWithServerUnreachable() async throws {
    let daemon = try FakeDaemon { request in
        guard request.method == "subscribe" else { return .error(code: "featureUnsupported") }
        return .framesThenHangUp([
            [:],
            ["signal": "workingSet"],
            ["signal": "errorResolved"],
            ["signal": "evict", "uid": "42"],
            ["signal": "requestDownload", "uid": "root"],
            ["signal": "workingSet"],
        ])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)

    let outcome = try await drain(client.subscribe())

    #expect(outcome.elements == [
        .workingSet,
        .errorResolved,
        .evict(NSFileProviderItemIdentifier("42")),
        .requestDownload(.rootContainer),
        .workingSet,
    ])
    #expect(outcome.ended, "a hang-up ends the subscription")
    #expect(isFileProviderError(outcome.error, .serverUnreachable), "\(String(describing: outcome.error))")
}

/// The item calls of three connections whose `hello` is answered by
/// `helloReplies` and then by protocol 2.
private func greetThrough(_ helloReplies: [FakeReply]) async throws -> (errors: [NSError?], daemon: FakeDaemon) {
    let daemon = try FakeDaemon(hello: { connection in
        connection <= helloReplies.count ? helloReplies[connection - 1] : .hello(version: 2)
    }) { request in
        .result(["item": wireItem(uid: request.params["uid"] as? String ?? "", parentUID: "root", name: "x")])
    }
    let client = DaemonClient(socketPath: daemon.socketPath)
    var errors: [NSError?] = []
    for _ in 0 ... helloReplies.count {
        errors.append(await thrown { _ = try await client.item(NSFileProviderItemIdentifier("1")) })
    }
    return (errors, daemon)
}

@Test(.timeLimit(.minutes(1)))
func aDaemonOnAnOlderProtocolFailsTheCallAsNewerExtensionAndTheNextCallReconnects() async throws {
    let (errors, daemon) = try await greetThrough([.hello(version: 1), .error(code: "newerExtensionVersionFound")])
    defer { daemon.stop() }

    #expect(isFileProviderError(errors[0], .newerExtensionVersionFound), "hello answered 1: \(String(describing: errors[0]))")
    #expect(isFileProviderError(errors[1], .newerExtensionVersionFound), "hello refused: \(String(describing: errors[1]))")
    #expect(errors[2] == nil, "the next call after a failed hello reconnects: \(String(describing: errors[2]))")
    #expect(daemon.connections == 3)
    #expect(daemon.hellos.allSatisfy { $0.params["version"] as? Int == 2 }, "hello offers protocol 2")
    #expect(daemon.requests.map(\.method) == ["item"], "no request is sent before hello succeeded")
}

@Test(.timeLimit(.minutes(1)))
func aDaemonOnANewerProtocolFailsTheCallAsOlderExtensionAndTheNextCallReconnects() async throws {
    let (errors, daemon) = try await greetThrough([.hello(version: 3), .error(code: "olderExtensionVersionRunning")])
    defer { daemon.stop() }

    #expect(isFileProviderError(errors[0], .olderExtensionVersionRunning), "hello answered 3: \(String(describing: errors[0]))")
    #expect(isFileProviderError(errors[1], .olderExtensionVersionRunning), "hello refused: \(String(describing: errors[1]))")
    #expect(errors[2] == nil, "the next call after a failed hello reconnects: \(String(describing: errors[2]))")
    #expect(daemon.connections == 3)
}

@Test(.timeLimit(.minutes(1)))
func aHangUpFailsEveryPendingRequestOnceWithServerUnreachableAndTheNextRequestReconnects() async throws {
    let daemon = try FakeDaemon(batchSize: 3) { request in
        let uid = request.params["uid"] as? String ?? ""
        return uid == "after" ? .result(["item": wireItem(uid: uid, parentUID: "root", name: "after")]) : .hangUp
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)
    let completions = Locked<[String: Int]>([:])

    let errors = await withTaskGroup(of: (String, NSError?).self) { group in
        for uid in ["a", "b", "c"] {
            group.addTask {
                let error = await thrown { _ = try await client.item(NSFileProviderItemIdentifier(uid)) }
                completions.update { $0[uid, default: 0] += 1 }
                return (uid, error)
            }
        }
        return await group.reduce(into: [String: NSError?]()) { $0[$1.0] = $1.1 }
    }

    for uid in ["a", "b", "c"] {
        let error = errors[uid] ?? nil
        #expect(isFileProviderError(error, .serverUnreachable), "\(uid): \(String(describing: error))")
    }
    #expect(completions.value == ["a": 1, "b": 1, "c": 1])

    let after = try await client.item(NSFileProviderItemIdentifier("after"))
    #expect(after.filename == "after")
    #expect(daemon.connections == 2, "the request after the hang-up opens a new connection")
}

@Test(.timeLimit(.minutes(1)))
func releasingAClientWhoseDaemonHungUpLeavesOtherConnectionsIntact() async throws {
    let daemon = try FakeDaemon { request in
        let uid = request.params["uid"] as? String ?? ""
        return uid == "drop" ? .hangUp : .result(["item": wireItem(uid: uid, parentUID: "root", name: uid)])
    }
    defer { daemon.stop() }

    for round in 0 ..< 20 {
        var dropped: DaemonClient? = DaemonClient(socketPath: daemon.socketPath)
        let hungUp = await thrown { _ = try await dropped?.item(NSFileProviderItemIdentifier("drop")) }
        #expect(isFileProviderError(hungUp, .serverUnreachable), "round \(round): \(String(describing: hungUp))")

        let survivor = DaemonClient(socketPath: daemon.socketPath)
        _ = try await survivor.item(NSFileProviderItemIdentifier("before"))
        dropped = nil

        let error = await thrown { _ = try await survivor.item(NSFileProviderItemIdentifier("after")) }
        #expect(error == nil, "round \(round): a released client ended another connection: \(String(describing: error))")
        if error != nil { break }
    }
}
