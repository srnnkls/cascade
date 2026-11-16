import Foundation

/// Runs `operation` and calls `completion` exactly once: with its result,
/// with its error converted into an Apple domain, or with `userCancelled`
/// when `progress` is cancelled first. A result arriving after cancellation
/// is dropped.
@discardableResult
public func completeOnce<Value: Sendable>(
    _ completion: @escaping @Sendable (Result<Value, any Error>) -> Void,
    progress: Progress = Progress(totalUnitCount: 1),
    operation: @escaping @Sendable () async throws -> Value
) -> Progress {
    let once = Once(completion)
    let task = Task {
        do {
            let value = try await operation()
            if once.finish(.success(value)) {
                progress.completedUnitCount = progress.totalUnitCount
            }
        } catch {
            once.finish(.failure(appleError(error)))
        }
    }
    progress.cancellationHandler = {
        task.cancel()
        once.finish(.failure(CocoaError(.userCancelled)))
    }
    return progress
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
