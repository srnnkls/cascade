import FileProvider
import Foundation
import UniformTypeIdentifiers

/// A page of an enumeration.
public struct ItemPage: Sendable {
    public let items: [CascadeItem]
    public let nextPage: NSFileProviderPage?
}

/// Items changed since an anchor.
public struct ItemChanges: Sendable {
    public let updated: [CascadeItem]
    public let deleted: [NSFileProviderItemIdentifier]
    public let anchor: NSFileProviderSyncAnchor
}

/// Contents of a partial fetch, moved into the caller's directory.
public struct PartialContents: Sendable {
    public let url: URL
    public let range: NSRange
    public let item: CascadeItem
}

/// A frame of the `progress` stream.
public struct OperationProgress: Decodable, Sendable, Equatable {
    public let opId: String
    public let done: Int64
    public let total: Int64
    public let finished: Bool
}

/// A daemon signal for the host app to relay to `NSFileProviderManager`.
public enum DaemonSignal: Sendable, Equatable {
    case workingSet
    case errorResolved
    case evict(NSFileProviderItemIdentifier)
    case requestDownload(NSFileProviderItemIdentifier)

    init?(_ frame: SignalFrame) {
        switch (frame.signal, frame.uid) {
        case ("workingSet", _): self = .workingSet
        case ("errorResolved", _): self = .errorResolved
        case let ("evict", uid?): self = .evict(itemIdentifier(wireUID: uid))
        case let ("requestDownload", uid?): self = .requestDownload(itemIdentifier(wireUID: uid))
        default: return nil
        }
    }
}

/// Client of protocol v2 over one persistent connection to the daemon socket.
public final class DaemonClient: Sendable {
    private let connection: DaemonConnection
    private let stagingDirectory: URL

    /// `stagingDirectory` receives clones of contents handed to the daemon;
    /// it defaults to `staging` beside the socket in the App Group container.
    public init(socketPath: String, stagingDirectory: URL? = nil) {
        connection = DaemonConnection(socketPath: socketPath)
        self.stagingDirectory = stagingDirectory
            ?? URL(fileURLWithPath: socketPath).deletingLastPathComponent().appendingPathComponent("staging")
    }

    // MARK: Items and enumeration

    public func item(_ identifier: NSFileProviderItemIdentifier) async throws -> CascadeItem {
        let result = try await call("item", UIDParams(uid: wireUID(identifier)), as: ItemResult.self)
        return CascadeItem(result.item)
    }

    /// A page of the children of `container`; the first page lists the
    /// folder on the backend.
    public func list(_ container: NSFileProviderItemIdentifier, page: NSFileProviderPage) async throws -> ItemPage {
        let params = try ListParams(uid: wireUID(container), page: wirePage(page))
        return try await ItemPage(call("list", params, as: PageResult.self))
    }

    /// A page of the materialised and pinned items.
    public func workingSet(page: NSFileProviderPage) async throws -> ItemPage {
        try await ItemPage(call("workingSet", PageParams(page: wirePage(page)), as: PageResult.self))
    }

    public func currentAnchor() async throws -> NSFileProviderSyncAnchor {
        try await syncAnchor(wire: call("currentAnchor", NoParams(), as: AnchorResult.self).anchor)
    }

    /// Items changed since `anchor`; an anchor of another store or below the
    /// tombstone floor fails with `syncAnchorExpired`.
    public func changes(from anchor: NSFileProviderSyncAnchor) async throws -> ItemChanges {
        let result = try await call("changes", AnchorParams(anchor: wireAnchor(anchor)), as: ChangesResult.self)
        return try ItemChanges(
            updated: result.updated.map(CascadeItem.init),
            deleted: result.deleted.map { itemIdentifier(wireUID: $0) },
            anchor: syncAnchor(wire: result.anchor)
        )
    }

    // MARK: Contents

    /// Fetches the whole file and moves it from the staging directory into
    /// `directory`, typically `NSFileProviderManager.temporaryDirectoryURL()`.
    public func fetch(
        _ identifier: NSFileProviderItemIdentifier,
        requestedVersion: NSFileProviderItemVersion?,
        into directory: URL,
        progress: Progress? = nil
    ) async throws -> (URL, CascadeItem) {
        let opID = UUID().uuidString
        let params = FetchParams(
            opId: opID, uid: wireUID(identifier), requestedVersion: requestedVersion.map(wireVersion)
        )
        let result = try await operation(opID, "fetch", params, progress: progress, as: FetchResult.self)
        return try (moveStaged(result.path, into: directory, opID: opID), CascadeItem(result.item))
    }

    /// Fetches a clone covering at least `range` aligned to `alignment`.
    public func fetchRange(
        _ identifier: NSFileProviderItemIdentifier,
        requestedVersion: NSFileProviderItemVersion?,
        strict: Bool,
        range: NSRange,
        alignment: Int,
        into directory: URL,
        progress: Progress? = nil
    ) async throws -> PartialContents {
        let opID = UUID().uuidString
        let params = FetchRangeParams(
            opId: opID,
            uid: wireUID(identifier),
            requestedVersion: requestedVersion.map(wireVersion),
            strict: strict,
            offset: Int64(range.location),
            length: Int64(range.length),
            alignment: Int64(alignment)
        )
        let result = try await operation(opID, "fetchRange", params, progress: progress, as: FetchRangeResult.self)
        return try PartialContents(
            url: moveStaged(result.path, into: directory, opID: opID),
            range: NSRange(location: Int(result.retrievedOffset), length: Int(result.retrievedLength)),
            item: CascadeItem(result.item)
        )
    }

    // MARK: Mutations

    /// Creates the item `template` describes. A symlink stays local and
    /// reports `excludedFromSync` without reaching the daemon.
    public func create(
        _ template: any NSFileProviderItem,
        contents: URL?,
        mayAlreadyExist: Bool
    ) async throws -> CascadeItem {
        let contentType = template.contentType ?? .data
        if contentType.conforms(to: .symbolicLink) {
            throw fileProviderError(.excludedFromSync, "a symbolic link stays local")
        }
        let opID = UUID().uuidString
        let staged = try contents.map { try stage($0, opID: opID) }
        defer { staged.map(removeStaged) }
        let params = CreateParams(
            opId: opID,
            parentUid: wireUID(template.parentItemIdentifier),
            name: template.filename,
            kind: contentType.conforms(to: .directory) ? "folder" : "file",
            contentsPath: staged?.path,
            mayAlreadyExist: mayAlreadyExist
        )
        return try await CascadeItem(operation(opID, "create", params, as: MutationResult.self).item)
    }

    /// Applies the `changedFields` of `item`; a rename to a name the NAS
    /// cannot hold answers the item under its current name.
    public func modify(
        _ item: any NSFileProviderItem,
        changedFields: NSFileProviderItemFields,
        contents: URL?
    ) async throws -> CascadeItem {
        let opID = UUID().uuidString
        let staged = try contents.map { try stage($0, opID: opID) }
        defer { staged.map(removeStaged) }
        let params = ModifyParams(
            opId: opID,
            uid: wireUID(item.itemIdentifier),
            changedFields: wireChangedFields(changedFields),
            name: changedFields.contains(.filename) ? item.filename : nil,
            parentUid: changedFields.contains(.parentItemIdentifier) ? wireUID(item.parentItemIdentifier) : nil,
            contentsPath: staged?.path
        )
        return try await CascadeItem(operation(opID, "modify", params, as: MutationResult.self).item)
    }

    /// Deletes the item; an unknown item counts as deleted.
    public func delete(_ identifier: NSFileProviderItemIdentifier, recursive: Bool) async throws {
        let opID = UUID().uuidString
        let params = DeleteParams(opId: opID, uid: wireUID(identifier), recursive: recursive)
        _ = try await operation(opID, "delete", params, as: Empty.self)
    }

    // MARK: Cache control

    public func setPinned(_ identifiers: [NSFileProviderItemIdentifier], pinned: Bool) async throws {
        _ = try await call(
            "setPinned", SetPinnedParams(uids: identifiers.map(wireUID), pinned: pinned), as: Empty.self
        )
    }

    /// Deletes the cached object of an item the system evicted.
    public func evict(_ identifier: NSFileProviderItemIdentifier) async throws {
        _ = try await call("evict", UIDParams(uid: wireUID(identifier)), as: Empty.self)
    }

    /// Replaces the daemon's materialised set.
    public func materializedSet(_ identifiers: [NSFileProviderItemIdentifier]) async throws {
        _ = try await call("materializedSet", UIDsParams(uids: identifiers.map(wireUID)), as: Empty.self)
    }

    /// Reports the items `enumerator` lists as the materialised set; pass
    /// `NSFileProviderManager.enumeratorForMaterializedItems()` after
    /// `materializedItemsDidChange`.
    public func reportMaterializedSet(from enumerator: any NSFileProviderEnumerator) async throws {
        try await materializedSet(collectIdentifiers(enumerator))
    }

    public func cancel(opID: String) async throws {
        _ = try await call("cancel", OpParams(opId: opID), as: Empty.self)
    }

    // MARK: Streams

    /// Progress of the operation `opID` until it finished.
    public func progress(of opID: String) async throws -> AsyncThrowingStream<OperationProgress, any Error> {
        try await frameStream(connection.link(), "progress", OpParams(opId: opID)) { (frame: OperationProgress) in
            (frame, !frame.finished)
        }
    }

    /// Daemon signals until the connection ends.
    public func subscribe() async throws -> AsyncThrowingStream<DaemonSignal, any Error> {
        try await frameStream(connection.link(), "subscribe", NoParams()) { (frame: SignalFrame) in
            (DaemonSignal(frame), true)
        }
    }

    // MARK: Transport

    private func call<Result: Decodable>(
        _ method: String,
        _ params: some Encodable & Sendable,
        as _: Result.Type
    ) async throws -> Result {
        let frame = try await connection.link().call(method, params)
        return try decodeResult(Result.self, from: frame)
    }

    /// A call that starts work: a task cancelled before the request is
    /// written sends nothing, one cancelled after sends `cancel` for `opID`,
    /// and `progress` follows the daemon's progress frames.
    private func operation<Result: Decodable>(
        _ opID: String,
        _ method: String,
        _ params: some Encodable & Sendable,
        progress: Progress? = nil,
        as _: Result.Type
    ) async throws -> Result {
        if Task.isCancelled {
            throw CocoaError(.userCancelled)
        }
        let tracking = ProgressTracking(progress)
        defer { tracking.stop() }
        let sending = OperationSending()
        return try await withTaskCancellationHandler {
            let link = try await connection.link()
            if sending.isCancelled {
                throw CocoaError(.userCancelled)
            }
            let follow = tracking.start(opID)
            let frame = try await link.call(method, params) {
                follow?(link)
                if sending.markSent() { self.sendCancel(opID) }
            }
            return try decodeResult(Result.self, from: frame)
        } onCancel: {
            if sending.markCancelled() { sendCancel(opID) }
        }
    }

    private func sendCancel(_ opID: String) {
        Task { try? await self.cancel(opID: opID) }
    }

    // MARK: Staging

    private func stage(_ contents: URL, opID: String) throws -> URL {
        try FileManager.default.createDirectory(at: stagingDirectory, withIntermediateDirectories: true)
        let staged = stagingDirectory.appendingPathComponent("\(opID)-\(contents.lastPathComponent)")
        try FileManager.default.copyItem(at: contents, to: staged)
        return staged
    }

    private func removeStaged(_ staged: URL) {
        try? FileManager.default.removeItem(at: staged)
    }

    /// Moves the daemon's staged file into `directory`, or removes it when
    /// the operation was cancelled meanwhile.
    private func moveStaged(_ path: String, into directory: URL, opID: String) throws -> URL {
        let source = URL(fileURLWithPath: path)
        if Task.isCancelled {
            removeStaged(source)
            throw CocoaError(.userCancelled)
        }
        let destination = directory.appendingPathComponent("\(opID)-\(source.lastPathComponent)")
        try FileManager.default.moveItem(at: source, to: destination)
        return destination
    }
}

private func wireVersion(_ version: NSFileProviderItemVersion) -> String {
    String(decoding: version.contentVersion, as: UTF8.self)
}

extension ItemPage {
    init(_ page: PageResult) {
        items = page.items.map(CascadeItem.init)
        nextPage = page.nextPage.map { NSFileProviderPage(Data($0.utf8)) }
    }
}

/// The handler of a stream frame sequence, removed from its link when the
/// consumer stops listening.
private final class StreamRegistration: @unchecked Sendable {
    private let link: DaemonLink
    private let lock = NSLock()
    private var id: Int?
    private var acknowledged = false
    private var cancelled = false

    init(link: DaemonLink) {
        self.link = link
    }

    /// A persistent handler that skips the stream's acknowledgement frame.
    func handler(_ frames: @escaping @Sendable (Result<Data, any Error>) -> Bool) -> ReplyHandler {
        ReplyHandler(persistent: true) { result in
            if case .success = result {
                let isAcknowledgement = self.lock.withLock {
                    defer { self.acknowledged = true }
                    return !self.acknowledged
                }
                if isAcknowledgement { return true }
            }
            return frames(result)
        }
    }

    func registered(_ id: Int) {
        let cancelled = lock.withLock {
            self.id = id
            return self.cancelled
        }
        if cancelled { link.remove(id) }
    }

    func cancel() {
        let id = lock.withLock {
            cancelled = true
            return self.id
        }
        if let id { link.remove(id) }
    }
}

/// Whether an operation's request was written and whether its task was
/// cancelled; whichever comes second sends the `cancel` RPC.
private final class OperationSending: @unchecked Sendable {
    private let lock = NSLock()
    private var sent = false
    private var cancelled = false

    var isCancelled: Bool {
        lock.withLock { cancelled }
    }

    /// Whether the cancellation arrived first and `cancel` is due now.
    func markSent() -> Bool {
        lock.withLock {
            sent = true
            return cancelled
        }
    }

    /// Whether the request was written and `cancel` is due now.
    func markCancelled() -> Bool {
        lock.withLock {
            cancelled = true
            return sent
        }
    }
}

/// Follows the `progress` stream of an operation into a `Progress`.
private final class ProgressTracking: Sendable {
    private let progress: Progress?
    private let following = TaskBox()

    init(_ progress: Progress?) {
        self.progress = progress
    }

    func start(_ opID: String) -> (@Sendable (DaemonLink) -> Void)? {
        guard let progress else { return nil }
        return { [following] link in
            following.set(Task {
                let stream = frameStream(link, "progress", OpParams(opId: opID)) { (frame: OperationProgress) in
                    (frame, !frame.finished)
                }
                for try await frame in stream {
                    progress.totalUnitCount = frame.total
                    progress.completedUnitCount = frame.done
                }
            })
        }
    }

    func stop() {
        following.cancel()
    }
}

/// The frames of a streaming request on `link` after its acknowledgement;
/// `element` maps each to a value and whether more follow.
private func frameStream<Frame: Decodable & Sendable, Element: Sendable>(
    _ link: DaemonLink,
    _ method: String,
    _ params: some Encodable & Sendable,
    element: @escaping @Sendable (Frame) -> (Element?, Bool)
) -> AsyncThrowingStream<Element, any Error> {
    let (stream, continuation) = AsyncThrowingStream<Element, any Error>.makeStream()
    let registration = StreamRegistration(link: link)
    continuation.onTermination = { _ in registration.cancel() }
    do {
        let id = try link.send(method, params, handler: registration.handler { result in
            switch result {
            case let .failure(error):
                continuation.finish(throwing: error)
                return false
            case let .success(frame):
                do {
                    let (value, more) = try element(decodeResult(Frame.self, from: frame))
                    if let value { continuation.yield(value) }
                    if !more { continuation.finish() }
                    return more
                } catch {
                    continuation.finish(throwing: error)
                    return false
                }
            }
        })
        registration.registered(id)
    } catch {
        continuation.finish(throwing: error)
    }
    return stream
}

private final class TaskBox: @unchecked Sendable {
    private let lock = NSLock()
    private var task: Task<Void, any Error>?

    func set(_ task: Task<Void, any Error>) {
        lock.withLock { self.task = task }
    }

    func cancel() {
        lock.withLock { task }?.cancel()
    }
}
