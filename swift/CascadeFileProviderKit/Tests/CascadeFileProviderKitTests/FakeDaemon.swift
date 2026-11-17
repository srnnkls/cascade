import Darwin
import Foundation

/// A request frame as the fake daemon received it.
struct WireRequest: @unchecked Sendable {
    let id: Int
    let method: String
    let params: [String: Any]
}

enum FakeReply: @unchecked Sendable {
    case result([String: Any])
    case error(code: String)
    /// Several result frames under the request's id, as a stream answers.
    case frames([[String: Any]])
    case framesThenHangUp([[String: Any]])
    /// No reply until the test calls `FakeDaemon.answer(_:with:)`.
    case withhold
    case hangUp

    static func hello(version: Int) -> FakeReply {
        .result(["version": version, "root_uid": "root", "capabilities": [String]()])
    }
}

/// A Unix socket server inside the test process speaking the daemon's
/// framing: a 4-byte big-endian length, then a JSON object.
///
/// `hello` is answered at once with `hello(connectionNumber)`, protocol
/// version 2 by default. The first `batchSize` other requests on the first
/// connection are held until all arrived and then answered in reverse order
/// of arrival; later ones are answered one by one.
final class FakeDaemon: @unchecked Sendable {
    let socketPath: String
    private let batchSize: Int
    private let respond: @Sendable (WireRequest) -> FakeReply
    private let hello: @Sendable (Int) -> FakeReply
    private let lock = NSLock()
    private let writeLock = NSLock()
    private var received: [WireRequest] = []
    private var withheld: [Int: Int32] = [:]
    private var accepted = 0
    private var stopped = false

    init(
        socketPath: String? = nil,
        batchSize: Int = 1,
        hello: @escaping @Sendable (Int) -> FakeReply = { _ in .hello(version: 2) },
        respond: @escaping @Sendable (WireRequest) -> FakeReply
    ) throws {
        self.batchSize = batchSize
        self.hello = hello
        self.respond = respond
        self.socketPath = socketPath ?? "/tmp/cfpk-\(UUID().uuidString.prefix(8)).sock"
        let listener = try Self.listen(at: self.socketPath)
        Thread.detachNewThread { [self] in
            defer { close(listener) }
            while true {
                let connection = accept(listener, nil, nil)
                if connection < 0 { return }
                let number: Int? = lock.withLock {
                    if stopped { return nil }
                    accepted += 1
                    return accepted
                }
                guard let number else {
                    close(connection)
                    return
                }
                var noSigPipe: Int32 = 1
                setsockopt(connection, SOL_SOCKET, SO_NOSIGPIPE, &noSigPipe, socklen_t(MemoryLayout<Int32>.size))
                Thread.detachNewThread { [self] in serve(connection, number: number) }
            }
        }
    }

    /// Requests other than `hello`, in order of arrival.
    var requests: [WireRequest] {
        lock.withLock { received.filter { $0.method != "hello" } }
    }

    /// The `hello` requests, one per connection.
    var hellos: [WireRequest] {
        lock.withLock { received.filter { $0.method == "hello" } }
    }

    var connections: Int {
        lock.withLock { accepted }
    }

    /// Answers a request that was answered with `.withhold`.
    func answer(_ id: Int, with reply: FakeReply) {
        guard let connection = lock.withLock({ withheld.removeValue(forKey: id) }) else { return }
        if !deliver(reply, to: id, on: connection) {
            shutdown(connection, SHUT_RDWR)
        }
    }

    /// Ends the accept loop. Only the accept thread closes the listener: a
    /// descriptor closed here could be reused by another test's daemon while
    /// the thread is about to accept on it again. A listening socket ignores
    /// `shutdown` on Darwin, so a last connection wakes the thread.
    func stop() {
        lock.withLock { stopped = true }
        if let wake = try? Self.connect(to: socketPath) {
            close(wake)
        }
        unlink(socketPath)
    }

    private func serve(_ connection: Int32, number: Int) {
        defer {
            lock.withLock { withheld = withheld.filter { $0.value != connection } }
            close(connection)
        }
        var pending: [WireRequest] = []
        var batch = number == 1 ? batchSize : 1
        while let frame = Self.readFrame(connection) {
            let request = WireRequest(
                id: frame["id"] as? Int ?? -1,
                method: frame["method"] as? String ?? "",
                params: frame["params"] as? [String: Any] ?? [:]
            )
            lock.withLock { received.append(request) }
            if request.method == "hello" {
                guard deliver(hello(number), to: request.id, on: connection) else { return }
                continue
            }
            pending.append(request)
            guard pending.count >= batch else { continue }
            for request in pending.reversed() {
                guard deliver(respond(request), to: request.id, on: connection) else { return }
            }
            pending.removeAll()
            batch = 1
        }
    }

    /// Writes `reply`; false when the connection is to be dropped.
    private func deliver(_ reply: FakeReply, to id: Int, on connection: Int32) -> Bool {
        switch reply {
        case let .result(result):
            write(connection, ["id": id, "result": result])
        case let .error(code):
            write(connection, ["id": id, "error": ["code": code, "message": "fake daemon"]])
        case let .frames(frames):
            frames.forEach { write(connection, ["id": id, "result": $0]) }
        case let .framesThenHangUp(frames):
            frames.forEach { write(connection, ["id": id, "result": $0]) }
            return false
        case .withhold:
            lock.withLock { withheld[id] = connection }
        case .hangUp:
            return false
        }
        return true
    }

    private func write(_ connection: Int32, _ object: [String: Any]) {
        writeLock.withLock { Self.writeFrame(connection, object) }
    }

    private static func listen(at path: String) throws -> Int32 {
        let fd = try socket(at: path) { fd, address, length in bind(fd, address, length) }
        guard Darwin.listen(fd, 8) == 0 else {
            close(fd)
            throw POSIXError(.EADDRINUSE)
        }
        return fd
    }

    private static func connect(to path: String) throws -> Int32 {
        try socket(at: path) { fd, address, length in Darwin.connect(fd, address, length) }
    }

    /// A new socket that `attach` binds or connects to `path`.
    private static func socket(
        at path: String,
        _ attach: (Int32, UnsafePointer<sockaddr>, socklen_t) -> Int32
    ) throws -> Int32 {
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let bytes = Array(path.utf8) + [0]
        guard bytes.count <= MemoryLayout.size(ofValue: address.sun_path) else {
            throw POSIXError(.ENAMETOOLONG)
        }
        withUnsafeMutableBytes(of: &address.sun_path) { $0.copyBytes(from: bytes) }
        let fd = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw POSIXError(.EIO) }
        let attached = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                attach(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard attached == 0 else {
            close(fd)
            throw POSIXError(.EADDRINUSE)
        }
        return fd
    }

    private static func readExactly(_ fd: Int32, _ count: Int) -> Data? {
        var data = Data(count: count)
        var offset = 0
        while offset < count {
            let n = data.withUnsafeMutableBytes { buffer in
                read(fd, buffer.baseAddress! + offset, count - offset)
            }
            if n <= 0 { return nil }
            offset += n
        }
        return data
    }

    private static func readFrame(_ fd: Int32) -> [String: Any]? {
        guard let header = readExactly(fd, 4) else { return nil }
        let length = header.reduce(0) { ($0 << 8) | Int($1) }
        guard let body = readExactly(fd, length) else { return nil }
        return (try? JSONSerialization.jsonObject(with: body)) as? [String: Any]
    }

    private static func writeFrame(_ fd: Int32, _ object: [String: Any]) {
        guard let body = try? JSONSerialization.data(withJSONObject: object) else { return }
        let length = UInt32(body.count).bigEndian
        var frame = withUnsafeBytes(of: length) { Data($0) }
        frame.append(body)
        frame.withUnsafeBytes { buffer in
            var offset = 0
            while offset < buffer.count {
                let n = Darwin.write(fd, buffer.baseAddress! + offset, buffer.count - offset)
                if n <= 0 { return }
                offset += n
            }
        }
    }
}

/// A protocol v2 item as the daemon serialises it.
func wireItem(
    uid: String,
    parentUID: String,
    name: String,
    kind: String = "file",
    size: Int? = nil,
    mtime: String? = nil,
    contentVersion: String = "c1",
    metadataVersion: String = "m1",
    pinned: Bool = false,
    uploaded: Bool = true,
    capabilities: [String] = ["allowsReading"]
) -> [String: Any] {
    [
        "uid": uid,
        "parent_uid": parentUID,
        "name": name,
        "kind": kind,
        "size": size.map { $0 as Any } ?? NSNull(),
        "btime": NSNull(),
        "mtime": mtime.map { $0 as Any } ?? NSNull(),
        "content_version": contentVersion,
        "metadata_version": metadataVersion,
        "pinned": pinned,
        "uploaded": uploaded,
        "capabilities": capabilities,
    ]
}
