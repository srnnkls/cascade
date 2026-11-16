import FileProvider
import Foundation
import UniformTypeIdentifiers

/// An item of the Cascade domain as the daemon reports it.
public final class CascadeItem: NSObject, NSFileProviderItem, @unchecked Sendable {
    public let itemIdentifier: NSFileProviderItemIdentifier
    public let parentItemIdentifier: NSFileProviderItemIdentifier
    public let filename: String
    public let contentType: UTType
    public let documentSize: NSNumber?
    public let creationDate: Date?
    public let contentModificationDate: Date?
    public let itemVersion: NSFileProviderItemVersion
    public let contentPolicy: NSFileProviderContentPolicy
    public let capabilities: NSFileProviderItemCapabilities
    public let isUploaded: Bool
    public let userInfo: [AnyHashable: Any]?

    /// Whether the item is kept downloaded.
    public let isPinned: Bool

    init(_ item: WireItem) {
        itemIdentifier = CascadeFileProviderKit.itemIdentifier(wireUID: item.uid)
        parentItemIdentifier = CascadeFileProviderKit.itemIdentifier(wireUID: item.parentUid)
        filename = item.name
        switch item.kind {
        case .folder:
            contentType = .folder
        case .file:
            let fileExtension = (item.name as NSString).pathExtension
            contentType = UTType(filenameExtension: fileExtension) ?? .data
        }
        documentSize = item.size.map { NSNumber(value: $0) }
        creationDate = item.btime
        contentModificationDate = item.mtime
        itemVersion = NSFileProviderItemVersion(
            contentVersion: Data(item.contentVersion.utf8),
            metadataVersion: Data(item.metadataVersion.utf8)
        )
        contentPolicy = item.pinned ? .downloadEagerlyAndKeepDownloaded : .inherited
        capabilities = item.capabilities.reduce(into: []) { capabilities, name in
            if let capability = capabilityNames[name] {
                capabilities.insert(capability)
            }
        }
        isUploaded = item.uploaded
        isPinned = item.pinned
        userInfo = ["pinned": item.pinned]
    }
}

private let capabilityNames: [String: NSFileProviderItemCapabilities] = [
    "allowsReading": .allowsReading,
    "allowsWriting": .allowsWriting,
    "allowsReparenting": .allowsReparenting,
    "allowsRenaming": .allowsRenaming,
    "allowsDeleting": .allowsDeleting,
    "allowsAddingSubItems": .allowsAddingSubItems,
    "allowsContentEnumerating": .allowsContentEnumerating,
]
