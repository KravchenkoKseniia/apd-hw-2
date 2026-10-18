public struct Job: Sendable, Equatable {
    public let id: String
    public let input: Int

    public init(id: String, input: Int) {
        self.id = id
        self.input = input
    }
}

public enum JobOutcome: Sendable, Equatable {
    case succeeded(Int)
    case failed
    case cancelled
}

public struct JobResult: Sendable, Equatable {
    public let id: String
    public let outcome: JobOutcome

    public init(id: String, outcome: JobOutcome) {
        self.id = id
        self.outcome = outcome
    }
}

public enum TaskProcessorError: Error, Equatable {
    case nonPositiveMaximumConcurrency
    case duplicateID(String)
}

public actor TaskProcessor {
    private let maximumConcurrency: Int
    
    public init(maximumConcurrency: Int) throws {
        guard maximumConcurrency > 0 else { throw TaskProcessorError.nonPositiveMaximumConcurrency }
        self.maximumConcurrency = maximumConcurrency
    }

    public func process(_ jobs: [Job], worker: @escaping @Sendable (Job) async throws -> Int) async throws -> [JobResult] {
        guard !jobs.isEmpty else {return []}
        
        var seen = Set<String>()
        
        for j in jobs {
            if !seen.insert(j.id).inserted {
                throw TaskProcessorError.duplicateID(j.id)
            }
        }
        
        let outcomes = await withTaskGroup(of: (Int, JobOutcome).self, returning: [JobOutcome?].self) { group in
            var results = [JobOutcome?](repeating: nil, count: jobs.count)
            var nextIndex = 0
            
            while nextIndex < min(maximumConcurrency, jobs.count) {
                let index = nextIndex
                let job = jobs[index]
                let started = group.addTaskUnlessCancelled {await (index, Self.processChild(of: job, using: worker))}
                guard started else {break}
                nextIndex += 1
            }
            
            while let (finishedIndex, outcome) = await group.next() {
                results[finishedIndex] = outcome
                
                if Task.isCancelled {
                    group.cancelAll()
                    continue
                }
                
                if nextIndex < jobs.count {
                    let index = nextIndex
                    let job = jobs[index]
                    let started = group.addTaskUnlessCancelled {await (index, Self.processChild(of: job, using: worker))}
                    if started { nextIndex += 1}
                }
            }
            
            return results
        }
        
        return zip(jobs, outcomes).map { job, outcome in
            JobResult(id: job.id, outcome: outcome ?? .cancelled)
        }
    }
    
    private static func processChild(of job: Job, using worker: @Sendable (Job) async throws -> Int) async -> JobOutcome {
        do {
            return .succeeded(try await worker(job))
        } catch is CancellationError {
            return .cancelled
        } catch {
            return .failed
        }
    }
}
