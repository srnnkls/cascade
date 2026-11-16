import FileProvider
import Foundation

/// A value shared between a test and the fake daemon's threads.
final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var stored: Value

    init(_ value: Value) {
        stored = value
    }

    var value: Value {
        lock.withLock { stored }
    }

    func update(_ change: (inout Value) -> Void) {
        lock.withLock { change(&stored) }
    }
}

/// Whether `condition` holds within `limit`, polled every 10 ms.
func eventually(within limit: Duration = .seconds(5), _ condition: () -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + limit
    while ContinuousClock.now < deadline {
        if condition() { return true }
        try? await Task.sleep(for: .milliseconds(10))
    }
    return condition()
}

/// The error `body` throws, or nil when it returns.
func thrown(_ body: () async throws -> Void) async -> NSError? {
    do {
        try await body()
        return nil
    } catch {
        return error as NSError
    }
}

func isFileProviderError(_ error: NSError?, _ code: NSFileProviderError.Code) -> Bool {
    error?.domain == NSFileProviderError.errorDomain && error?.code == code.rawValue
}

/// A new empty directory under the temporary directory.
func scratchDirectory() throws -> URL {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("cfpk-\(UUID().uuidString.prefix(8))", isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    return directory
}

func sameDirectory(_ lhs: URL, _ rhs: URL) -> Bool {
    lhs.standardizedFileURL.resolvingSymlinksInPath().path == rhs.standardizedFileURL.resolvingSymlinksInPath().path
}

/// What a stream delivered within a time limit.
struct StreamOutcome<Element: Sendable>: Sendable {
    var elements: [Element] = []
    var ended = false
    var error: NSError?
}

/// Iterates `stream` for at most `limit`; `ended` is false when the stream
/// was still open at the limit.
func drain<Element: Sendable>(
    _ stream: AsyncThrowingStream<Element, any Error>,
    within limit: Duration = .seconds(5)
) async -> StreamOutcome<Element> {
    let outcome = Locked(StreamOutcome<Element>())
    await withTaskGroup(of: Void.self) { group in
        group.addTask {
            do {
                for try await element in stream {
                    outcome.update { $0.elements.append(element) }
                }
                if !Task.isCancelled {
                    outcome.update { $0.ended = true }
                }
            } catch {
                outcome.update {
                    $0.ended = true
                    $0.error = error as NSError
                }
            }
        }
        group.addTask {
            try? await Task.sleep(for: limit)
        }
        await group.next()
        group.cancelAll()
    }
    return outcome.value
}

/// The first page the system asks an enumerator for.
let initialPage = NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data)

struct EnumeratedPage: @unchecked Sendable {
    let identifiers: [NSFileProviderItemIdentifier]
    let nextPage: NSFileProviderPage?
}

/// Collects one page of an enumeration and resumes with it once.
private final class PageObserver: NSObject, NSFileProviderEnumerationObserver, @unchecked Sendable {
    private let lock = NSLock()
    private var identifiers: [NSFileProviderItemIdentifier] = []
    private var continuation: CheckedContinuation<Result<EnumeratedPage, any Error>, Never>?

    init(_ continuation: CheckedContinuation<Result<EnumeratedPage, any Error>, Never>) {
        self.continuation = continuation
    }

    func didEnumerate(_ updatedItems: [any NSFileProviderItem]) {
        lock.withLock { identifiers += updatedItems.map(\.itemIdentifier) }
    }

    func finishEnumerating(upTo nextPage: NSFileProviderPage?) {
        let (continuation, identifiers) = take()
        continuation?.resume(returning: .success(EnumeratedPage(identifiers: identifiers, nextPage: nextPage)))
    }

    func finishEnumeratingWithError(_ error: any Error) {
        take().0?.resume(returning: .failure(error))
    }

    private func take() -> (CheckedContinuation<Result<EnumeratedPage, any Error>, Never>?, [NSFileProviderItemIdentifier]) {
        lock.withLock {
            defer { continuation = nil }
            return (continuation, identifiers)
        }
    }
}

func enumeratePage(
    _ enumerator: any NSFileProviderEnumerator,
    from page: NSFileProviderPage
) async -> Result<EnumeratedPage, any Error> {
    await withCheckedContinuation { continuation in
        enumerator.enumerateItems(for: PageObserver(continuation), startingAt: page)
    }
}

/// Every identifier an enumerator lists, following its pages to the end.
func enumerateAll(_ enumerator: any NSFileProviderEnumerator, from first: NSFileProviderPage = initialPage)
    async throws -> [NSFileProviderItemIdentifier]
{
    var identifiers: [NSFileProviderItemIdentifier] = []
    var page: NSFileProviderPage? = first
    var pages = 0
    while let current = page, pages < 20 {
        let result = try await enumeratePage(enumerator, from: current).get()
        identifiers += result.identifiers
        page = result.nextPage
        pages += 1
    }
    return identifiers
}

struct EnumeratedChanges: @unchecked Sendable {
    let updated: [NSFileProviderItemIdentifier]
    let deleted: [NSFileProviderItemIdentifier]
    let anchor: NSFileProviderSyncAnchor
    let moreComing: Bool
}

private final class ChangeObserver: NSObject, NSFileProviderChangeObserver, @unchecked Sendable {
    private let lock = NSLock()
    private var updated: [NSFileProviderItemIdentifier] = []
    private var deleted: [NSFileProviderItemIdentifier] = []
    private var continuation: CheckedContinuation<Result<EnumeratedChanges, any Error>, Never>?

    init(_ continuation: CheckedContinuation<Result<EnumeratedChanges, any Error>, Never>) {
        self.continuation = continuation
    }

    func didUpdate(_ updatedItems: [any NSFileProviderItem]) {
        lock.withLock { updated += updatedItems.map(\.itemIdentifier) }
    }

    func didDeleteItems(withIdentifiers deletedItemIdentifiers: [NSFileProviderItemIdentifier]) {
        lock.withLock { deleted += deletedItemIdentifiers }
    }

    func finishEnumeratingChanges(upTo anchor: NSFileProviderSyncAnchor, moreComing: Bool) {
        let (continuation, changes) = lock.withLock {
            defer { continuation = nil }
            return (
                continuation,
                EnumeratedChanges(updated: updated, deleted: deleted, anchor: anchor, moreComing: moreComing)
            )
        }
        continuation?.resume(returning: .success(changes))
    }

    func finishEnumeratingWithError(_ error: any Error) {
        let continuation = lock.withLock {
            defer { self.continuation = nil }
            return self.continuation
        }
        continuation?.resume(returning: .failure(error))
    }
}

func enumerateChanges(
    _ enumerator: any NSFileProviderEnumerator,
    from anchor: NSFileProviderSyncAnchor
) async -> Result<EnumeratedChanges, any Error> {
    await withCheckedContinuation { continuation in
        enumerator.enumerateChanges?(for: ChangeObserver(continuation), from: anchor)
    }
}

/// An item as the system's materialised-items enumerator lists it.
final class ListedItem: NSObject, NSFileProviderItem {
    let itemIdentifier: NSFileProviderItemIdentifier
    let parentItemIdentifier: NSFileProviderItemIdentifier = .rootContainer
    let filename: String

    init(_ identifier: NSFileProviderItemIdentifier) {
        itemIdentifier = identifier
        filename = identifier.rawValue
    }
}

/// An item as the system hands it to `modify`.
final class EditedItem: NSObject, NSFileProviderItem {
    let itemIdentifier: NSFileProviderItemIdentifier
    let parentItemIdentifier: NSFileProviderItemIdentifier
    let filename: String

    init(_ identifier: NSFileProviderItemIdentifier, parent: NSFileProviderItemIdentifier, filename: String) {
        itemIdentifier = identifier
        parentItemIdentifier = parent
        self.filename = filename
    }
}
