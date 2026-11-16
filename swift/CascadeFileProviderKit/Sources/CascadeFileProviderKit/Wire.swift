import FileProvider
import Foundation

/// Protocol version this package speaks.
let protocolVersion = 2

/// Wire uid of `NSFileProviderItemIdentifier.rootContainer`.
let rootUID = "root"

func wireUID(_ identifier: NSFileProviderItemIdentifier) -> String {
    identifier == .rootContainer ? rootUID : identifier.rawValue
}

func itemIdentifier(wireUID uid: String) -> NSFileProviderItemIdentifier {
    uid == rootUID ? .rootContainer : NSFileProviderItemIdentifier(uid)
}

/// Bytes a sync anchor or an enumeration page may hold.
let maxCursorBytes = 500

/// The daemon's page token, or nil for one of the system's initial pages.
func wirePage(_ page: NSFileProviderPage) throws -> String? {
    let initialPages = [NSFileProviderPage.initialPageSortedByName, NSFileProviderPage.initialPageSortedByDate]
    if initialPages.contains(where: { page.rawValue == $0 as Data }) {
        return nil
    }
    guard page.rawValue.count <= maxCursorBytes, let text = String(data: page.rawValue, encoding: .utf8) else {
        throw fileProviderError(.pageExpired, "page is not a Cascade page")
    }
    return text
}

func wireAnchor(_ anchor: NSFileProviderSyncAnchor) throws -> String {
    guard anchor.rawValue.count <= maxCursorBytes,
          let text = String(data: anchor.rawValue, encoding: .utf8)
    else {
        throw fileProviderError(.syncAnchorExpired, "anchor is not a Cascade anchor")
    }
    return text
}

func syncAnchor(wire anchor: String) throws -> NSFileProviderSyncAnchor {
    let bytes = Data(anchor.utf8)
    guard bytes.count <= maxCursorBytes else {
        throw fileProviderError(.cannotSynchronize, "anchor of \(bytes.count) bytes exceeds \(maxCursorBytes)")
    }
    return NSFileProviderSyncAnchor(bytes)
}

// MARK: - Frames

struct RequestFrame<Params: Encodable>: Encodable {
    let id: Int
    let method: String
    let params: Params
}

/// Fields of a reply frame every reply carries.
struct ReplyHeader: Decodable {
    struct Failure: Decodable {
        let code: String
        let message: String
    }

    let id: Int
    let error: Failure?
}

struct ReplyFrame<Result: Decodable>: Decodable {
    let result: Result
}

struct NoParams: Encodable, Sendable {}

struct Empty: Decodable, Sendable {}

let wireEncoder: JSONEncoder = {
    let encoder = JSONEncoder()
    encoder.keyEncodingStrategy = .convertToSnakeCase
    return encoder
}()

let wireDecoder: JSONDecoder = {
    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase
    decoder.dateDecodingStrategy = .custom { decoder in
        let text = try decoder.singleValueContainer().decode(String.self)
        guard let date = parseTimestamp(text) else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: decoder.codingPath, debugDescription: "not an RFC 3339 timestamp: \(text)")
            )
        }
        return date
    }
    return decoder
}()

private func parseTimestamp(_ text: String) -> Date? {
    if let date = try? Date(text, strategy: Date.ISO8601FormatStyle(includingFractionalSeconds: true)) {
        return date
    }
    return try? Date(text, strategy: .iso8601)
}

func decodeResult<Result: Decodable>(_: Result.Type, from frame: Data) throws -> Result {
    do {
        return try wireDecoder.decode(ReplyFrame<Result>.self, from: frame).result
    } catch {
        throw fileProviderError(.cannotSynchronize, "undecodable reply: \(error)")
    }
}

// MARK: - Payloads

/// A protocol v2 item as the daemon serialises it.
struct WireItem: Decodable, Sendable {
    enum Kind: String, Decodable, Sendable {
        case file
        case folder
    }

    let uid: String
    let parentUid: String
    let name: String
    let kind: Kind
    let size: Int64?
    let btime: Date?
    let mtime: Date?
    let contentVersion: String
    let metadataVersion: String
    let pinned: Bool
    let uploaded: Bool
    let capabilities: [String]
}

struct HelloParams: Encodable, Sendable {
    let version: Int
}

struct HelloResult: Decodable, Sendable {
    let version: Int
    let rootUid: String
    let capabilities: [String]
}

struct UIDParams: Encodable, Sendable {
    let uid: String
}

struct UIDsParams: Encodable, Sendable {
    let uids: [String]
}

struct ItemResult: Decodable, Sendable {
    let item: WireItem
}

struct ListParams: Encodable, Sendable {
    let uid: String
    let page: String?
}

struct PageParams: Encodable, Sendable {
    let page: String?
}

struct PageResult: Decodable, Sendable {
    let items: [WireItem]
    let nextPage: String?
}

struct AnchorParams: Encodable, Sendable {
    let anchor: String
}

struct AnchorResult: Decodable, Sendable {
    let anchor: String
}

struct ChangesResult: Decodable, Sendable {
    let updated: [WireItem]
    let deleted: [String]
    let anchor: String
}

struct FetchParams: Encodable, Sendable {
    let opId: String
    let uid: String
    let requestedVersion: String?
}

struct FetchResult: Decodable, Sendable {
    let path: String
    let item: WireItem
}

struct FetchRangeParams: Encodable, Sendable {
    let opId: String
    let uid: String
    let requestedVersion: String?
    let strict: Bool
    let offset: Int64
    let length: Int64
    let alignment: Int64
}

struct FetchRangeResult: Decodable, Sendable {
    let path: String
    let retrievedOffset: Int64
    let retrievedLength: Int64
    let item: WireItem
}

struct CreateParams: Encodable, Sendable {
    let opId: String
    let parentUid: String
    let name: String
    let kind: String
    let contentsPath: String?
    let mayAlreadyExist: Bool
}

struct ModifyParams: Encodable, Sendable {
    let opId: String
    let uid: String
    let changedFields: [String]
    let name: String?
    let parentUid: String?
    let contentsPath: String?
}

struct MutationResult: Decodable, Sendable {
    let item: WireItem
    let fetchContent: Bool
}

struct DeleteParams: Encodable, Sendable {
    let opId: String
    let uid: String
    let recursive: Bool
}

struct SetPinnedParams: Encodable, Sendable {
    let uids: [String]
    let pinned: Bool
}

struct OpParams: Encodable, Sendable {
    let opId: String
}

struct SignalFrame: Decodable, Sendable {
    let signal: String
    let uid: String?
}

/// Wire names of the fields `modify` reports as changed.
func wireChangedFields(_ fields: NSFileProviderItemFields) -> [String] {
    let names: [(NSFileProviderItemFields, String)] = [
        (.contents, "contents"),
        (.filename, "name"),
        (.parentItemIdentifier, "parent_uid"),
        (.lastUsedDate, "last_used_date"),
        (.tagData, "tag_data"),
        (.favoriteRank, "favorite_rank"),
        (.creationDate, "creation_date"),
        (.contentModificationDate, "content_modification_date"),
        (.fileSystemFlags, "file_system_flags"),
        (.extendedAttributes, "extended_attributes"),
        (.typeAndCreator, "type_and_creator"),
    ]
    return names.filter { fields.contains($0.0) }.map(\.1)
}
