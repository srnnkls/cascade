import FileProvider
import Foundation

/// Enumerates a container's children or, for `.workingSet`, the materialised
/// and pinned items, and the changes since an anchor.
public final class CascadeEnumerator: NSObject, NSFileProviderEnumerator, @unchecked Sendable {
    private let client: DaemonClient
    private let enumeratedItemIdentifier: NSFileProviderItemIdentifier
    private let lock = NSLock()
    private var running: [UUID: Task<Void, Never>] = [:]

    public init(client: DaemonClient, enumeratedItemIdentifier: NSFileProviderItemIdentifier) {
        self.client = client
        self.enumeratedItemIdentifier = enumeratedItemIdentifier
    }

    /// The enumerator of `container`; Cascade has no trash, which the system
    /// expects to learn from `featureUnsupported` in `NSCocoaErrorDomain`.
    public static func forContainer(
        _ container: NSFileProviderItemIdentifier,
        client: DaemonClient
    ) throws -> CascadeEnumerator {
        guard container != .trashContainer else { throw CocoaError(.featureUnsupported) }
        return CascadeEnumerator(client: client, enumeratedItemIdentifier: container)
    }

    public func invalidate() {
        let tasks = lock.withLock {
            defer { running.removeAll() }
            return Array(running.values)
        }
        tasks.forEach { $0.cancel() }
    }

    public func enumerateItems(for observer: any NSFileProviderEnumerationObserver, startingAt page: NSFileProviderPage) {
        let observer = Unchecked(observer)
        run { [client, enumeratedItemIdentifier] in
            do {
                let result = if enumeratedItemIdentifier == .workingSet {
                    try await client.workingSet(page: page)
                } else {
                    try await client.list(enumeratedItemIdentifier, page: page)
                }
                observer.value.didEnumerate(result.items)
                observer.value.finishEnumerating(upTo: result.nextPage)
            } catch {
                observer.value.finishEnumeratingWithError(appleError(error))
            }
        }
    }

    /// The working set reports every change; a container reports the
    /// updates of its children and every deletion.
    public func enumerateChanges(for observer: any NSFileProviderChangeObserver, from syncAnchor: NSFileProviderSyncAnchor) {
        let observer = Unchecked(observer)
        run { [client, enumeratedItemIdentifier] in
            do {
                let changes = try await client.changes(from: syncAnchor)
                let updated = enumeratedItemIdentifier == .workingSet
                    ? changes.updated
                    : changes.updated.filter { $0.parentItemIdentifier == enumeratedItemIdentifier }
                if !updated.isEmpty { observer.value.didUpdate(updated) }
                if !changes.deleted.isEmpty { observer.value.didDeleteItems(withIdentifiers: changes.deleted) }
                observer.value.finishEnumeratingChanges(upTo: changes.anchor, moreComing: false)
            } catch {
                observer.value.finishEnumeratingWithError(appleError(error))
            }
        }
    }

    public func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        let completion = Unchecked(completionHandler)
        run { [client] in
            completion.value(try? await client.currentAnchor())
        }
    }

    private func run(_ body: @escaping @Sendable () async -> Void) {
        let key = UUID()
        lock.withLock {
            running[key] = Task { [weak self] in
                await body()
                self?.finished(key)
            }
        }
    }

    private func finished(_ key: UUID) {
        _ = lock.withLock { running.removeValue(forKey: key) }
    }
}

/// The identifiers `enumerator` lists over all its pages.
func collectIdentifiers(_ enumerator: any NSFileProviderEnumerator) async throws -> [NSFileProviderItemIdentifier] {
    var identifiers: [NSFileProviderItemIdentifier] = []
    var page: NSFileProviderPage? = NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data)
    while let current = page {
        let collector = PageCollector()
        let pageResult = try await withCheckedThrowingContinuation { continuation in
            collector.continuation = continuation
            enumerator.enumerateItems(for: collector, startingAt: current)
        }
        identifiers += pageResult.identifiers
        page = pageResult.nextPage
    }
    return identifiers
}

private struct CollectedPage: Sendable {
    let identifiers: [NSFileProviderItemIdentifier]
    let nextPage: NSFileProviderPage?
}

private final class PageCollector: NSObject, NSFileProviderEnumerationObserver, @unchecked Sendable {
    private let lock = NSLock()
    private var identifiers: [NSFileProviderItemIdentifier] = []
    var continuation: CheckedContinuation<CollectedPage, any Error>?

    func didEnumerate(_ updatedItems: [any NSFileProviderItem]) {
        lock.withLock { identifiers += updatedItems.map(\.itemIdentifier) }
    }

    func finishEnumerating(upTo nextPage: NSFileProviderPage?) {
        let (continuation, identifiers) = take()
        continuation?.resume(returning: CollectedPage(identifiers: identifiers, nextPage: nextPage))
    }

    func finishEnumeratingWithError(_ error: any Error) {
        take().0?.resume(throwing: error)
    }

    private func take() -> (CheckedContinuation<CollectedPage, any Error>?, [NSFileProviderItemIdentifier]) {
        lock.withLock {
            defer { continuation = nil }
            return (continuation, identifiers)
        }
    }
}

/// A non-Sendable system object carried into a closure the system does not
/// run concurrently with its own use of the object.
public struct Unchecked<Value>: @unchecked Sendable {
    public let value: Value

    public init(_ value: Value) {
        self.value = value
    }
}
