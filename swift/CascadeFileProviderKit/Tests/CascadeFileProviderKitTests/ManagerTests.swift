import FileProvider
import Foundation
import Testing

@testable import CascadeFileProviderKit

private final class RecordingManager: FileProviderManaging, @unchecked Sendable {
    let log: Locked<[String]>
    private let evictFailure: NSError?

    init(log: Locked<[String]> = Locked([]), evictFailure: NSError? = nil) {
        self.log = log
        self.evictFailure = evictFailure
    }

    func signalWorkingSet() async throws {
        log.update { $0.append("signalWorkingSet") }
    }

    func signalErrorResolved() async throws {
        log.update { $0.append("signalErrorResolved") }
    }

    func evictItem(_ identifier: NSFileProviderItemIdentifier) async throws {
        log.update { $0.append("evictItem \(identifier.rawValue)") }
        if let evictFailure { throw evictFailure }
    }

    func requestDownload(_ identifier: NSFileProviderItemIdentifier) async throws {
        log.update { $0.append("requestDownload \(identifier.rawValue)") }
    }
}

@Test(.timeLimit(.minutes(1)))
func eachDaemonSignalBecomesItsManagerCallAndAHangUpResubscribes() async throws {
    let subscriptions = Locked(0)
    let daemon = try FakeDaemon { request in
        guard request.method == "subscribe" else { return .error(code: "featureUnsupported") }
        subscriptions.update { $0 += 1 }
        if subscriptions.value == 1 {
            return .framesThenHangUp([
                [:],
                ["signal": "workingSet"],
                ["signal": "errorResolved"],
                ["signal": "evict", "uid": "42"],
                ["signal": "requestDownload", "uid": "7"],
            ])
        }
        return .frames([[:], ["signal": "evict", "uid": "after"]])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)
    let manager = RecordingManager()

    let relay = Task { await relaySignals(from: client, to: manager, retryAfter: .milliseconds(20)) }
    let relayedAll = await eventually { manager.log.value.count >= 5 }
    relay.cancel()
    await relay.value

    #expect(relayedAll, "\(manager.log.value)")
    #expect(manager.log.value == [
        "signalWorkingSet",
        "signalErrorResolved",
        "evictItem 42",
        "requestDownload 7",
        "evictItem after",
    ])
    #expect(subscriptions.value == 2, "a hang-up subscribes again on a new connection")
    #expect(daemon.connections == 2)
}

@Test(.timeLimit(.minutes(1)))
func keepDownloadedPinsAndRemoveDownloadUnpinsEvictsThenDropsTheCachedObjectOnlyAfterTheSystemEvicted() async throws {
    let log = Locked<[String]>([])
    let daemon = try FakeDaemon { request in
        let uids = request.params["uids"] as? [String] ?? []
        switch request.method {
        case "setPinned":
            let pinned = request.params["pinned"] as? Bool
            log.update { $0.append("setPinned \(pinned.map(String.init) ?? "-") \(uids.joined(separator: ","))") }
        case "evict":
            log.update { $0.append("evict \(request.params["uid"] as? String ?? "-")") }
        default:
            return .error(code: "featureUnsupported")
        }
        return .result([:])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)
    let selection = [NSFileProviderItemIdentifier("a"), NSFileProviderItemIdentifier("b")]

    try await performAction(
        CascadeAction.keepDownloaded.identifier, on: selection, client: client, manager: RecordingManager(log: log)
    )
    #expect(log.value == ["setPinned true a,b"], "Keep Downloaded pins the selection and evicts nothing")

    log.update { $0.removeAll() }
    try await performAction(
        CascadeAction.removeDownload.identifier, on: selection, client: client, manager: RecordingManager(log: log)
    )
    let removal = log.value
    #expect(removal.first == "setPinned false a,b", "\(removal)")
    for uid in ["a", "b"] {
        let systemEvicted = removal.firstIndex(of: "evictItem \(uid)")
        let objectDropped = removal.firstIndex(of: "evict \(uid)")
        #expect(systemEvicted != nil && objectDropped != nil, "\(uid): \(removal)")
        if let systemEvicted, let objectDropped {
            #expect(systemEvicted < objectDropped, "\(uid): the object goes only after evictItem: \(removal)")
        }
    }
    #expect(removal.count == 5, "\(removal)")

    log.update { $0.removeAll() }
    let busy = NSError(domain: NSPOSIXErrorDomain, code: Int(EBUSY))
    let refused = await thrown {
        try await performAction(
            CascadeAction.removeDownload.identifier,
            on: [NSFileProviderItemIdentifier("open")],
            client: client,
            manager: RecordingManager(log: log, evictFailure: busy)
        )
    }
    #expect(
        refused?.domain == NSFileProviderError.errorDomain || refused?.domain == NSCocoaErrorDomain,
        "\(String(describing: refused))"
    )
    #expect(!log.value.contains("evict open"), "a failed evictItem keeps the cached object: \(log.value)")
}
