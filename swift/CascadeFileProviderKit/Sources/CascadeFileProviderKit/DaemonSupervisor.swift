import Darwin
import Foundation

/// Starts a daemon process for the container.
public protocol DaemonLaunching: Sendable {
    /// Starts the daemon and returns its process id; `exited` receives the
    /// process id and the exit status once it ends.
    func launch(exited: @escaping @Sendable (_ pid: Int32, _ status: Int32) -> Void) throws -> Int32
}

public enum DaemonEvent: Equatable, Sendable {
    case spawned(pid: Int32)
    /// A daemon already served the socket; its process id is read from the
    /// pid file and is nil when that is unreadable.
    case adopted(pid: Int32?)
    case stopped
    case restarting(status: Int32)
    case launchFailed(String)
}

/// Owns the one daemon of a container: adopts the daemon serving its socket
/// or spawns one, restarts a failed daemon unless another serves the socket,
/// and on `stop` terminates the daemon it spawned or adopted.
public final class DaemonSupervisor: @unchecked Sendable {
    private let socket: URL
    private let pidFile: URL
    private let launcher: any DaemonLaunching
    private let restartAfter: Duration
    private let terminate: @Sendable (Int32) -> Void
    private let report: @Sendable (DaemonEvent) -> Void
    private let lock = NSLock()
    private var owned: Int32?
    private var stopping = false
    private var restart: Task<Void, Never>?

    public init(
        socket: URL,
        pidFile: URL,
        launcher: some DaemonLaunching,
        restartAfter: Duration = .seconds(5),
        terminate: @escaping @Sendable (Int32) -> Void = { _ = kill($0, SIGTERM) },
        report: @escaping @Sendable (DaemonEvent) -> Void
    ) {
        self.socket = socket
        self.pidFile = pidFile
        self.launcher = launcher
        self.restartAfter = restartAfter
        self.terminate = terminate
        self.report = report
    }

    public func start() {
        guard !lock.withLock({ stopping }) else { return }
        if adoptServingDaemon() { return }
        do {
            let pid = try launcher.launch { [weak self] pid, status in self?.exited(pid, status: status) }
            lock.withLock { owned = pid }
            report(.spawned(pid: pid))
        } catch {
            report(.launchFailed(error.localizedDescription))
        }
    }

    public func stop() {
        let pid = lock.withLock {
            stopping = true
            restart?.cancel()
            defer { owned = nil }
            return owned
        }
        if let pid { terminate(pid) }
    }

    private func exited(_ pid: Int32, status: Int32) {
        let stopping = lock.withLock {
            if owned == pid { owned = nil }
            return self.stopping
        }
        guard !stopping, !adoptServingDaemon() else { return }
        guard status != 0 else {
            report(.stopped)
            return
        }
        report(.restarting(status: status))
        let delay = restartAfter
        lock.withLock {
            restart = Task { [weak self] in
                try? await Task.sleep(for: delay)
                guard !Task.isCancelled else { return }
                self?.start()
            }
        }
    }

    private func adoptServingDaemon() -> Bool {
        guard socketAcceptsConnections(socket.path) else { return false }
        let pid = (try? String(contentsOf: pidFile, encoding: .utf8))
            .flatMap { Int32($0.trimmingCharacters(in: .whitespacesAndNewlines)) }
        lock.withLock { owned = pid }
        report(.adopted(pid: pid))
        return true
    }
}
