import Darwin
import FileProvider
import Foundation

/// Receives the reply frames of one request id. A persistent handler stays
/// registered for further frames until it returns false; a failure is the
/// last call it receives.
struct ReplyHandler: Sendable {
    let persistent: Bool
    let deliver: @Sendable (Result<Data, any Error>) -> Bool
}

private let maxFrameBytes = 64 << 20

private func unreachable(_ message: String) -> NSError {
    fileProviderError(.serverUnreachable, message)
}

/// One connection to the daemon socket, framed as a 4-byte big-endian length
/// and a JSON object. Requests are pipelined; a reader thread routes each
/// reply to the handler registered under its id.
final class DaemonLink: @unchecked Sendable {
    private let fd: Int32
    private let lock = NSLock()
    private let writeLock = NSLock()
    private var nextID = 1
    private var handlers: [Int: ReplyHandler] = [:]
    private var closed = false

    private init(fd: Int32) {
        self.fd = fd
    }

    static func open(socketPath: String) throws -> DaemonLink {
        let link = DaemonLink(fd: try connectSocket(socketPath))
        Thread.detachNewThread { link.readUntilHangUp() }
        return link
    }

    var isClosed: Bool {
        lock.withLock { closed }
    }

    /// Ends the connection; every pending handler fails with `serverUnreachable`.
    /// The reader thread closes the descriptor once, after marking the link
    /// closed under `lock`, so a closed link's descriptor is never touched.
    func shutDown() {
        lock.withLock {
            if !closed { Darwin.shutdown(fd, SHUT_RDWR) }
        }
    }

    /// Registers `handler` and writes the request; returns its id.
    @discardableResult
    func send(_ method: String, _ params: some Encodable, handler: ReplyHandler) throws -> Int {
        let id = lock.withLock {
            defer { nextID += 1 }
            return nextID
        }
        let body = try wireEncoder.encode(RequestFrame(id: id, method: method, params: params))
        let registered = lock.withLock {
            if closed { return false }
            handlers[id] = handler
            return true
        }
        guard registered else {
            _ = handler.deliver(.failure(unreachable("daemon connection closed")))
            return id
        }
        do {
            try writeLock.withLock {
                guard !isClosed else { throw unreachable("daemon connection closed") }
                try writeFrame(body)
            }
        } catch {
            if let handler = remove(id) {
                _ = handler.deliver(.failure(error))
            }
        }
        return id
    }

    /// The reply frame of one request.
    func call(_ method: String, _ params: some Encodable, afterSending: (@Sendable () -> Void)? = nil)
        async throws -> Data
    {
        try await withCheckedThrowingContinuation { continuation in
            let handler = ReplyHandler(persistent: false) { result in
                continuation.resume(with: result)
                return false
            }
            do {
                try send(method, params, handler: handler)
                afterSending?()
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    @discardableResult
    func remove(_ id: Int) -> ReplyHandler? {
        lock.withLock { handlers.removeValue(forKey: id) }
    }

    private func readUntilHangUp() {
        while let frame = readFrame() {
            guard let header = try? wireDecoder.decode(ReplyHeader.self, from: frame) else { continue }
            let handler: ReplyHandler? = lock.withLock {
                guard let handler = handlers[header.id] else { return nil }
                if !handler.persistent { handlers[header.id] = nil }
                return handler
            }
            guard let handler else { continue }
            let result: Result<Data, any Error> = header.error.map {
                .failure(daemonError(code: $0.code, message: $0.message))
            } ?? .success(frame)
            if !handler.deliver(result), handler.persistent {
                remove(header.id)
            }
        }
        let pending = writeLock.withLock {
            let pending = lock.withLock {
                closed = true
                defer { handlers.removeAll() }
                return Array(handlers.values)
            }
            Darwin.close(fd)
            return pending
        }
        for handler in pending {
            _ = handler.deliver(.failure(unreachable("daemon connection closed")))
        }
    }

    private func readFrame() -> Data? {
        guard let header = readExactly(4) else { return nil }
        let length = header.reduce(0) { ($0 << 8) | Int($1) }
        guard length <= maxFrameBytes else { return nil }
        return readExactly(length)
    }

    private func readExactly(_ count: Int) -> Data? {
        var data = Data(count: count)
        var offset = 0
        while offset < count {
            let read = data.withUnsafeMutableBytes { buffer in
                Darwin.read(fd, buffer.baseAddress.map { $0 + offset }, count - offset)
            }
            if read < 0, errno == EINTR { continue }
            if read <= 0 { return nil }
            offset += read
        }
        return data
    }

    private func writeFrame(_ body: Data) throws {
        var frame = withUnsafeBytes(of: UInt32(body.count).bigEndian) { Data($0) }
        frame.append(body)
        try frame.withUnsafeBytes { buffer in
            var offset = 0
            while offset < buffer.count {
                let written = Darwin.write(fd, buffer.baseAddress.map { $0 + offset }, buffer.count - offset)
                if written < 0, errno == EINTR { continue }
                if written <= 0 {
                    throw unreachable("write to daemon: \(String(cString: strerror(errno)))")
                }
                offset += written
            }
        }
    }
}

/// Whether a daemon accepts connections on the socket at `path`.
func socketAcceptsConnections(_ path: String) -> Bool {
    guard let fd = try? connectSocket(path) else { return false }
    Darwin.close(fd)
    return true
}

private func connectSocket(_ path: String) throws -> Int32 {
    let fd = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
    guard fd >= 0 else {
        throw unreachable("socket: \(String(cString: strerror(errno)))")
    }
    var noSigPipe: Int32 = 1
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &noSigPipe, socklen_t(MemoryLayout<Int32>.size))
    var address = sockaddr_un()
    address.sun_family = sa_family_t(AF_UNIX)
    let bytes = Array(path.utf8) + [0]
    guard bytes.count <= MemoryLayout.size(ofValue: address.sun_path) else {
        Darwin.close(fd)
        throw unreachable("socket path too long: \(path)")
    }
    withUnsafeMutableBytes(of: &address.sun_path) { $0.copyBytes(from: bytes) }
    let connected = withUnsafePointer(to: &address) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            Darwin.connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
    }
    guard connected == 0 else {
        let message = String(cString: strerror(errno))
        Darwin.close(fd)
        throw unreachable("connect \(path): \(message)")
    }
    return fd
}

/// The one persistent link of a client: opened on first use, greeted with
/// `hello`, and opened again after the daemon hung up.
final class DaemonConnection: @unchecked Sendable {
    private enum State {
        case idle
        case connecting(Task<DaemonLink, any Error>)
        case ready(DaemonLink)
    }

    private let socketPath: String
    private let lock = NSLock()
    private var state = State.idle

    init(socketPath: String) {
        self.socketPath = socketPath
    }

    deinit {
        if case let .ready(link) = state {
            link.shutDown()
        }
    }

    func link() async throws -> DaemonLink {
        let connecting: Task<DaemonLink, any Error> = lock.withLock {
            switch state {
            case let .ready(link) where !link.isClosed:
                return Task { link }
            case let .connecting(task):
                return task
            default:
                let task = Task { [socketPath] in try await Self.greet(DaemonLink.open(socketPath: socketPath)) }
                state = .connecting(task)
                return task
            }
        }
        do {
            let link = try await connecting.value
            lock.withLock {
                if case let .connecting(task) = state, task == connecting {
                    state = .ready(link)
                }
            }
            return link
        } catch {
            lock.withLock {
                if case let .connecting(task) = state, task == connecting {
                    state = .idle
                }
            }
            throw error
        }
    }

    private static func greet(_ link: DaemonLink) async throws -> DaemonLink {
        do {
            let frame = try await link.call("hello", HelloParams(version: protocolVersion))
            let hello = try decodeResult(HelloResult.self, from: frame)
            guard hello.version == protocolVersion else {
                let code: NSFileProviderError.Code = hello.version > protocolVersion
                    ? .olderExtensionVersionRunning : .newerExtensionVersionFound
                throw fileProviderError(code, "daemon speaks protocol \(hello.version)")
            }
            return link
        } catch {
            link.shutDown()
            throw error
        }
    }
}
