import Foundation
import Testing

@testable import CascadeFileProviderKit

/// Hands out process ids from 100 and keeps each launch's exit callback for
/// the test to fire.
private final class FakeLauncher: DaemonLaunching, @unchecked Sendable {
    private let lock = NSLock()
    private var exits: [Int32: @Sendable (Int32, Int32) -> Void] = [:]
    private var nextPID: Int32 = 100

    var launches: Int {
        lock.withLock { Int(nextPID - 100) }
    }

    func launch(exited: @escaping @Sendable (Int32, Int32) -> Void) throws -> Int32 {
        lock.withLock {
            defer { nextPID += 1 }
            exits[nextPID] = exited
            return nextPID
        }
    }

    func exit(_ pid: Int32, status: Int32) {
        let exited = lock.withLock { exits.removeValue(forKey: pid) }
        exited?(pid, status)
    }
}

private struct Harness {
    let directory: URL
    let launcher = FakeLauncher()
    let events = Locked<[DaemonEvent]>([])
    let terminated = Locked<[Int32]>([])
    let supervisor: DaemonSupervisor

    var socket: URL { directory.appendingPathComponent("fp.sock") }
    var pidFile: URL { directory.appendingPathComponent("cascade.pid") }

    init() throws {
        directory = try scratchDirectory()
        let events = events
        let terminated = terminated
        supervisor = DaemonSupervisor(
            socket: directory.appendingPathComponent("fp.sock"),
            pidFile: directory.appendingPathComponent("cascade.pid"),
            launcher: launcher,
            restartAfter: .milliseconds(20),
            terminate: { pid in terminated.update { $0.append(pid) } },
            report: { event in events.update { $0.append(event) } }
        )
    }

    func serveSocket(pid: Int32) throws -> FakeDaemon {
        try "\(pid)\n".write(to: pidFile, atomically: true, encoding: .utf8)
        return try FakeDaemon(socketPath: socket.path) { _ in .error(code: "featureUnsupported") }
    }
}

@Test(.timeLimit(.minutes(1)))
func aLiveSocketIsAdoptedWithoutSpawningAndQuitTerminatesTheAdoptedDaemon() throws {
    let harness = try Harness()
    let daemon = try harness.serveSocket(pid: 4242)
    defer { daemon.stop() }

    harness.supervisor.start()
    harness.supervisor.stop()

    #expect(harness.launcher.launches == 0)
    #expect(harness.events.value == [.adopted(pid: 4242)])
    #expect(harness.terminated.value == [4242])
}

@Test(.timeLimit(.minutes(1)))
func withoutALiveSocketADaemonIsSpawnedAndQuitTerminatesIt() throws {
    let harness = try Harness()

    harness.supervisor.start()
    harness.supervisor.stop()

    #expect(harness.launcher.launches == 1)
    #expect(harness.events.value == [.spawned(pid: 100)])
    #expect(harness.terminated.value == [100])
}

@Test(.timeLimit(.minutes(1)))
func anExitWhileAnotherDaemonServesTheSocketAdoptsItInsteadOfRespawning() async throws {
    let harness = try Harness()
    harness.supervisor.start()
    let other = try harness.serveSocket(pid: 4242)
    defer { other.stop() }

    harness.launcher.exit(100, status: 1)
    try await Task.sleep(for: .milliseconds(200))
    harness.supervisor.stop()

    #expect(harness.launcher.launches == 1)
    #expect(harness.events.value == [.spawned(pid: 100), .adopted(pid: 4242)])
    #expect(harness.terminated.value == [4242])
}

@Test(.timeLimit(.minutes(1)))
func aFailedDaemonWithNoOtherServingTheSocketIsRestartedAndACleanExitIsNot() async throws {
    let harness = try Harness()
    harness.supervisor.start()

    harness.launcher.exit(100, status: 1)
    let restarted = await eventually { harness.launcher.launches == 2 }
    harness.launcher.exit(101, status: 0)
    try await Task.sleep(for: .milliseconds(200))
    harness.supervisor.stop()

    #expect(restarted)
    #expect(harness.launcher.launches == 2)
    #expect(harness.events.value == [.spawned(pid: 100), .restarting(status: 1), .spawned(pid: 101), .stopped])
    #expect(harness.terminated.value.isEmpty)
}
