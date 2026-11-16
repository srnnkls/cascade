import Foundation
import Security

/// The App Group container this process's signature is entitled to, or nil
/// for a build without the entitlement. The group is read from the signed
/// entitlements because its team prefix is known only once the build is
/// signed.
public func appGroupContainer() -> URL? {
    guard let task = SecTaskCreateFromSelf(nil),
          let groups = SecTaskCopyValueForEntitlement(
              task, "com.apple.security.application-groups" as CFString, nil
          ) as? [String],
          let group = groups.first
    else {
        return nil
    }
    return FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: group)
}

/// The daemon socket `cascade start --app-group <container>` listens on.
public func daemonSocket(in container: URL) -> URL {
    container.appendingPathComponent("fp.sock")
}

/// The file `cascade start --app-group <container>` writes its process id to.
public func daemonPIDFile(in container: URL) -> URL {
    container.appendingPathComponent("cascade.pid")
}
