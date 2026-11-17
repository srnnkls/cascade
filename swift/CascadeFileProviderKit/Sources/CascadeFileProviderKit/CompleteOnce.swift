import Foundation

/// Runs `operation` and calls `completion` exactly once: with its result,
/// with its error converted into an Apple domain, or with `userCancelled`
/// when `progress` is cancelled first. A result arriving after cancellation
/// is dropped. Cancellation reaches the operation and the completion before
/// `progress.cancel()` returns.
@discardableResult
public func completeOnce<Value: Sendable>(
    _ completion: @escaping @Sendable (Result<Value, any Error>) -> Void,
    progress: Progress = Progress(totalUnitCount: 1),
    operation: @escaping @Sendable () async throws -> Value
) -> Progress {
    let once = Once(completion)
    let cancellation = CancellationObservation()
    let task = Task {
        defer { cancellation.end() }
        do {
            let value = try await operation()
            if once.finish(.success(value)) {
                progress.completedUnitCount = progress.totalUnitCount
            }
        } catch {
            once.finish(.failure(appleError(error)))
        }
    }
    cancellation.hold(progress.observe(\.isCancelled, options: [.initial]) { progress, _ in
        guard progress.isCancelled else { return }
        task.cancel()
        once.finish(.failure(CocoaError(.userCancelled)))
    })
    return progress
}

/// Keeps the observation of `isCancelled`, which, unlike
/// `cancellationHandler`, runs on the thread calling `cancel()`, until the
/// operation ended.
private final class CancellationObservation: @unchecked Sendable {
    private let lock = NSLock()
    private var observation: NSKeyValueObservation?
    private var ended = false

    func hold(_ observation: NSKeyValueObservation) {
        let held = lock.withLock {
            if !ended { self.observation = observation }
            return !ended
        }
        if !held { observation.invalidate() }
    }

    func end() {
        let observation = lock.withLock {
            ended = true
            defer { self.observation = nil }
            return self.observation
        }
        observation?.invalidate()
    }
}

private final class Once<Value: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var completion: (@Sendable (Result<Value, any Error>) -> Void)?

    init(_ completion: @escaping @Sendable (Result<Value, any Error>) -> Void) {
        self.completion = completion
    }

    @discardableResult
    func finish(_ result: Result<Value, any Error>) -> Bool {
        let completion = lock.withLock {
            defer { self.completion = nil }
            return self.completion
        }
        completion?(result)
        return completion != nil
    }
}
