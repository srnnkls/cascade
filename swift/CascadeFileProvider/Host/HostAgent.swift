// SPDX-License-Identifier: Apache-2.0

import AppKit
import CascadeFileProviderKit
import FileProvider
import os
import ServiceManagement

/// Changing it orphans a domain already registered on a machine.
private let domainIdentifier = NSFileProviderDomainIdentifier("io.cascade.fileprovider.default")

private let logger = Logger(subsystem: "io.cascade.CascadeFileProviderHost", category: "agent")

/// Registers the app as a login item and the Cascade domain, runs the
/// embedded `cascade start --app-group <container>` and relays the daemon's
/// signals to the domain's `NSFileProviderManager`.
@MainActor
final class HostAgent: NSObject, NSApplicationDelegate, ObservableObject {
    @Published private(set) var status = "Starting…"
    private var daemon: DaemonSupervisor?
    private var relay: Task<Void, Never>?

    func applicationDidFinishLaunching(_: Notification) {
        registerLoginItem()
        guard let container = appGroupContainer() else {
            status = "No App Group container: the app is not signed with the team identity."
            return
        }
        guard let executable = Bundle.main.url(forAuxiliaryExecutable: "cascade") else {
            status = "The app bundle holds no cascade binary."
            return
        }
        let supervisor = DaemonSupervisor(
            socket: daemonSocket(in: container),
            pidFile: daemonPIDFile(in: container),
            launcher: DaemonProcess(executable: executable, container: container)
        ) { [weak self] event in
            Task { @MainActor in self?.show(event) }
        }
        daemon = supervisor
        supervisor.start()
        Task { await connectDomain(socket: daemonSocket(in: container)) }
    }

    func applicationWillTerminate(_: Notification) {
        relay?.cancel()
        daemon?.stop()
    }

    private func registerLoginItem() {
        let service = SMAppService.mainApp
        guard service.status != .enabled else { return }
        do {
            try service.register()
        } catch {
            logger.error("login item registration failed: \(error)")
        }
    }

    private func show(_ event: DaemonEvent) {
        switch event {
        case .spawned, .adopted:
            break
        case .stopped:
            status = "The daemon stopped; see daemon.log in the App Group container."
        case let .restarting(code):
            status = "The daemon exited with status \(code); restarting."
        case let .launchFailed(reason):
            status = "The daemon did not start: \(reason)"
        }
    }

    private func connectDomain(socket: URL) async {
        let domain = NSFileProviderDomain(identifier: domainIdentifier, displayName: "Cascade")
        do {
            try await NSFileProviderManager.add(domain)
        } catch {
            status = "The domain was not registered: \(error.localizedDescription)"
            return
        }
        guard let manager = NSFileProviderManager(for: domain) else {
            status = "The domain has no manager."
            return
        }
        status = "Cascade is running."
        let client = DaemonClient(socketPath: socket.path)
        let domainManager = DomainManager(manager)
        relay = Task.detached {
            await relaySignals(from: client, to: domainManager, retryAfter: .seconds(2))
        }
    }
}

/// `cascade start --app-group <container>` with its output appended to
/// `daemon.log` in the container.
private struct DaemonProcess: DaemonLaunching {
    let executable: URL
    let container: URL

    func launch(exited: @escaping @Sendable (Int32, Int32) -> Void) throws -> Int32 {
        let process = Process()
        process.executableURL = executable
        process.arguments = ["start", "--app-group", container.path]
        process.terminationHandler = { exited($0.processIdentifier, $0.terminationStatus) }
        let log = try appendingLog(container.appendingPathComponent("daemon.log"))
        process.standardOutput = log
        process.standardError = log
        try process.run()
        return process.processIdentifier
    }
}

private func appendingLog(_ url: URL) throws -> FileHandle {
    if !FileManager.default.fileExists(atPath: url.path) {
        FileManager.default.createFile(atPath: url.path, contents: nil)
    }
    let handle = try FileHandle(forWritingTo: url)
    try handle.seekToEnd()
    return handle
}
