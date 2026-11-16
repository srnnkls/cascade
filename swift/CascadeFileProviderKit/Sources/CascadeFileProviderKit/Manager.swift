import FileProvider
import Foundation
import os

/// The `NSFileProviderManager` calls the daemon's signals and the custom
/// actions turn into.
public protocol FileProviderManaging: Sendable {
    func signalWorkingSet() async throws
    func signalErrorResolved() async throws
    func evictItem(_ identifier: NSFileProviderItemIdentifier) async throws
    func requestDownload(_ identifier: NSFileProviderItemIdentifier) async throws
}

/// `FileProviderManaging` over the manager of a registered domain.
public final class DomainManager: FileProviderManaging, @unchecked Sendable {
    private let manager: NSFileProviderManager

    public init(_ manager: NSFileProviderManager) {
        self.manager = manager
    }

    public func signalWorkingSet() async throws {
        try await manager.signalEnumerator(for: .workingSet)
    }

    /// Ends the back-off the system entered after `serverUnreachable`.
    public func signalErrorResolved() async throws {
        try await manager.signalErrorResolved(NSFileProviderError(.serverUnreachable))
    }

    public func evictItem(_ identifier: NSFileProviderItemIdentifier) async throws {
        try await manager.evictItem(identifier: identifier)
    }

    public func requestDownload(_ identifier: NSFileProviderItemIdentifier) async throws {
        try await manager.requestDownloadForItem(withIdentifier: identifier)
    }
}

private let relayLogger = Logger(subsystem: "io.cascade.CascadeFileProviderKit", category: "relay")

/// Relays every daemon signal to its `manager` call until cancelled; after
/// the subscription ends or fails it waits `retryAfter` and subscribes again.
public func relaySignals(from client: DaemonClient, to manager: some FileProviderManaging, retryAfter: Duration) async {
    while !Task.isCancelled {
        do {
            for try await signal in try await client.subscribe() {
                do {
                    try await relay(signal, to: manager)
                } catch {
                    relayLogger.error("relaying \(String(describing: signal)) failed: \(error)")
                }
            }
        } catch {
            relayLogger.info("signal subscription ended: \(error)")
        }
        try? await Task.sleep(for: retryAfter)
    }
}

private func relay(_ signal: DaemonSignal, to manager: some FileProviderManaging) async throws {
    switch signal {
    case .workingSet: try await manager.signalWorkingSet()
    case .errorResolved: try await manager.signalErrorResolved()
    case let .evict(identifier): try await manager.evictItem(identifier)
    case let .requestDownload(identifier): try await manager.requestDownload(identifier)
    }
}

/// The custom actions the extension declares in `NSExtensionFileProviderActions`.
public enum CascadeAction: CaseIterable, Sendable {
    case keepDownloaded
    case removeDownload

    public var identifier: NSFileProviderExtensionActionIdentifier {
        switch self {
        case .keepDownloaded: NSFileProviderExtensionActionIdentifier("io.cascade.action.keepDownloaded")
        case .removeDownload: NSFileProviderExtensionActionIdentifier("io.cascade.action.removeDownload")
        }
    }

    init?(_ identifier: NSFileProviderExtensionActionIdentifier) {
        guard let action = Self.allCases.first(where: { $0.identifier == identifier }) else { return nil }
        self = action
    }
}

/// Keep Downloaded pins the selection. Remove Download unpins it, then per
/// item asks the system to evict and drops the cached object only after the
/// eviction succeeded; the first failure is thrown once every item was tried.
public func performAction(
    _ action: NSFileProviderExtensionActionIdentifier,
    on identifiers: [NSFileProviderItemIdentifier],
    client: DaemonClient,
    manager: some FileProviderManaging
) async throws {
    switch CascadeAction(action) {
    case .keepDownloaded:
        try await client.setPinned(identifiers, pinned: true)
    case .removeDownload:
        try await client.setPinned(identifiers, pinned: false)
        var firstFailure: (any Error)?
        for identifier in identifiers {
            do {
                try await manager.evictItem(identifier)
                try await client.evict(identifier)
            } catch {
                firstFailure = firstFailure ?? error
            }
        }
        if let firstFailure {
            throw appleError(firstFailure)
        }
    case nil:
        throw CocoaError(.featureUnsupported)
    }
}
