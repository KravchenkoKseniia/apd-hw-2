import XCTest
@testable import TaskProcessor

// Copied from TaskProcessorPublicTests.swift (starter)
private enum SampleWorkerError: Error {
    case expected
    case workerShouldNotRun
}

private actor Gate {
    private var releasedIDs: Set<String> = []
    private var waiters: [String: CheckedContinuation<Void, Never>] = [:]
    private var waitingObservers: [String: [CheckedContinuation<Void, Never>]] = [:]
    private var passedIDs: Set<String> = []
    private var passedObservers: [String: [CheckedContinuation<Void, Never>]] = [:]

    func wait(for id: String) async {
        if releasedIDs.remove(id) != nil {
            markPassed(id)
            return
        }

        await withCheckedContinuation { continuation in
            waiters[id] = continuation
            let observers = waitingObservers.removeValue(forKey: id) ?? []
            for observer in observers {
                observer.resume()
            }
        }
        markPassed(id)
    }

    func waitUntilWaiting(for id: String) async {
        if waiters[id] != nil {
            return
        }

        await withCheckedContinuation { continuation in
            waitingObservers[id, default: []].append(continuation)
        }
    }

    func waitUntilPassed(_ id: String) async {
        if passedIDs.contains(id) {
            return
        }

        await withCheckedContinuation { continuation in
            passedObservers[id, default: []].append(continuation)
        }
    }

    func release(_ id: String) {
        if let waiter = waiters.removeValue(forKey: id) {
            waiter.resume()
        } else {
            releasedIDs.insert(id)
        }
    }

    private func markPassed(_ id: String) {
        passedIDs.insert(id)
        let observers = passedObservers.removeValue(forKey: id) ?? []
        for observer in observers {
            observer.resume()
        }
    }
}

private actor Recorder {
    private(set) var started: [String] = []
    private(set) var active = 0
    private(set) var maxActive = 0
    
    func start(_ id: String){
        started.append(id)
        active += 1
        maxActive = max(maxActive, active)
    }
    
    func finish() {
        active -= 1
    }
}

@MainActor
final class TaskProcessorTests: XCTestCase {
    func testInitRejectsNegativeMaximumConcurrency() {
        for l in [-1, -15, Int.min] {
            XCTAssertThrowsError(try TaskProcessor(maximumConcurrency: l), "limit \(l)") {
                error in XCTAssertEqual(error as? TaskProcessorError, .nonPositiveMaximumConcurrency)
            }
        }
        XCTAssertNoThrow(try TaskProcessor(maximumConcurrency: 1))
    }
    
    func testDuplicateIDThrowsFirstRepeatedIDAndStartsNoWorker() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let recorder = Recorder()
        let jobs = ["a", "b", "b", "a"].map { Job(id: $0, input: 0) }
         
        do {
            _ = try await processor.process(jobs) { job in
            await recorder.start(job.id)
            return job.input
            }
            XCTFail("Expected duplicateID error")
        } catch let error as TaskProcessorError {
            XCTAssertEqual(error, .duplicateID("b"))
        }
         
        let started = await recorder.started
        XCTAssertTrue(started.isEmpty, "No worker should start")
    }
    
    func testWorkerCancellationErrorDoesNotStopOtherJobs() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let jobs = [ Job(id: "stop", input: 0), Job(id: "next", input: 5), Job(id: "last", input: 7)]
        
        let results = try await processor.process(jobs) { job in
            if job.id == "stop" { throw CancellationError() }
            return job.input
        }
     
        XCTAssertEqual(results, [JobResult(id: "stop", outcome: .cancelled),JobResult(id: "next", outcome: .succeeded(5)), JobResult(id: "last", outcome: .succeeded(7)),])
    }
    
    func testMixedOutcomesInSingleProcessCall() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 3)
        let jobs = [ Job(id: "ok", input: 21), Job(id: "boom", input: 0), Job(id: "stop", input: 0), Job(id: "ok2", input: 4)]
 
        let results = try await processor.process(jobs) { job in
            switch job.id {
                case "boom": throw SampleWorkerError.expected
                case "stop": throw CancellationError()
                default: return job.input * 2
            }
        }
 
        XCTAssertEqual(results, [
            JobResult(id: "ok", outcome: .succeeded(42)),
            JobResult(id: "boom", outcome: .failed),
            JobResult(id: "stop", outcome: .cancelled),
            JobResult(id: "ok2", outcome: .succeeded(8)),
        ])
    }
    
    func testJobBeyondLimitStartsOnlyAfterSlotIsFreed() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let gate = Gate()
        let recorder = Recorder()
        let jobs = ["a", "b", "c"].map { Job(id: $0, input: 1) }
     
        let task = Task {
            try await processor.process(jobs) { job in
                await recorder.start(job.id)
                await gate.wait(for: job.id)
                await recorder.finish()
                return job.input
            }
        }
     
        await gate.waitUntilWaiting(for: "a")
        await gate.waitUntilWaiting(for: "b")
        let firstWave = await recorder.started
        XCTAssertEqual(Set(firstWave), ["a", "b"])
     
        await gate.release("b")
        await gate.waitUntilWaiting(for: "c")
        let active = await recorder.active
        XCTAssertEqual(active, 2)
     
        await gate.release("a")
        await gate.release("c")
        let results = try await task.value
     
        XCTAssertEqual(results.map(\.outcome), [.succeeded(1), .succeeded(1), .succeeded(1)])
        let maxActive = await recorder.maxActive
        XCTAssertEqual(maxActive, 2)
    }
     
    func testExternalCancellationKeepsQueuedJobsFromStarting() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let gate = Gate()
        let recorder = Recorder()
        let jobs = ["a", "b", "c", "d", "e"].map { Job(id: $0, input: 1) }
     
        let task = Task {
            try await processor.process(jobs) { job in
                await recorder.start(job.id)
                await gate.wait(for: job.id)
                try Task.checkCancellation()
                return job.input
            }
        }
     
        await gate.waitUntilWaiting(for: "a")
        await gate.waitUntilWaiting(for: "b")
        await gate.release("a")
        await gate.waitUntilWaiting(for: "c")
     
        task.cancel()
        for id in ["b", "c", "d", "e"] {
            await gate.release(id)
        }
        let results = try await task.value
     
        XCTAssertEqual(results, [
            JobResult(id: "a", outcome: .succeeded(1)),
            JobResult(id: "b", outcome: .cancelled),
            JobResult(id: "c", outcome: .cancelled),
            JobResult(id: "d", outcome: .cancelled),
            JobResult(id: "e", outcome: .cancelled),
        ])
        let started = await recorder.started
        XCTAssertEqual(Set(started), ["a", "b", "c"], "d and e should not start")
    }
}

