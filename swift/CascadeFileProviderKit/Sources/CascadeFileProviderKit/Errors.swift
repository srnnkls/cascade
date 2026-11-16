import FileProvider
import Foundation

/// Error codes of protocol v2: the Swift case name of an `NSFileProviderError`
/// code or of an `NSCocoaErrorDomain` code.
enum WireErrorCode: String, CaseIterable {
    case notAuthenticated
    case filenameCollision
    case syncAnchorExpired
    case pageExpired
    case serverUnreachable
    case noSuchItem
    case deletionRejected
    case directoryNotEmpty
    case cannotSynchronize
    case nonEvictable
    case versionNoLongerAvailable
    case excludedFromSync
    case olderExtensionVersionRunning
    case newerExtensionVersionFound
    case fileWriteOutOfSpace
    case userCancelled
    case featureUnsupported

    func error(message: String) -> NSError {
        let userInfo = [NSDebugDescriptionErrorKey: message]
        switch self {
        case .notAuthenticated: return fileProvider(.notAuthenticated, userInfo)
        case .filenameCollision: return fileProvider(.filenameCollision, userInfo)
        case .syncAnchorExpired: return fileProvider(.syncAnchorExpired, userInfo)
        case .pageExpired: return fileProvider(.pageExpired, userInfo)
        case .serverUnreachable: return fileProvider(.serverUnreachable, userInfo)
        case .noSuchItem: return fileProvider(.noSuchItem, userInfo)
        case .deletionRejected: return fileProvider(.deletionRejected, userInfo)
        case .directoryNotEmpty: return fileProvider(.directoryNotEmpty, userInfo)
        case .cannotSynchronize: return fileProvider(.cannotSynchronize, userInfo)
        case .nonEvictable: return fileProvider(.nonEvictable, userInfo)
        case .versionNoLongerAvailable: return fileProvider(.versionNoLongerAvailable, userInfo)
        case .excludedFromSync: return fileProvider(.excludedFromSync, userInfo)
        case .olderExtensionVersionRunning: return fileProvider(.olderExtensionVersionRunning, userInfo)
        case .newerExtensionVersionFound: return fileProvider(.newerExtensionVersionFound, userInfo)
        case .fileWriteOutOfSpace: return CocoaError(.fileWriteOutOfSpace, userInfo: userInfo) as NSError
        case .userCancelled: return CocoaError(.userCancelled, userInfo: userInfo) as NSError
        case .featureUnsupported: return CocoaError(.featureUnsupported, userInfo: userInfo) as NSError
        }
    }
}

private func fileProvider(_ code: NSFileProviderError.Code, _ userInfo: [String: Any]) -> NSError {
    NSFileProviderError(code, userInfo: userInfo) as NSError
}

/// The Apple error a daemon error reply is answered with; a code this
/// extension does not know becomes `cannotSynchronize`.
func daemonError(code: String, message: String) -> NSError {
    let known = WireErrorCode(rawValue: code) ?? .cannotSynchronize
    return known.error(message: "\(code): \(message)")
}

func fileProviderError(_ code: NSFileProviderError.Code, _ message: String) -> NSError {
    fileProvider(code, [NSDebugDescriptionErrorKey: message])
}

/// `error` in `NSCocoaErrorDomain` or `NSFileProviderErrorDomain`, the only
/// domains the system accepts from a File Provider extension.
public func appleError(_ error: any Error) -> NSError {
    if error is CancellationError {
        return CocoaError(.userCancelled) as NSError
    }
    let error = error as NSError
    if error.domain == NSCocoaErrorDomain || error.domain == NSFileProviderError.errorDomain {
        return error
    }
    return NSFileProviderError(
        .cannotSynchronize,
        userInfo: [NSUnderlyingErrorKey: error, NSDebugDescriptionErrorKey: "\(error)"]
    ) as NSError
}
