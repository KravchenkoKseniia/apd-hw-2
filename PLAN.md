# Implementation Plan

Complete this before implementation and update it when the plan changes.

## Approach

### Validation (before any work starts)
- `init(maximumConcurrency:)` rejects values `<= 0` with `.nonPositiveMaximumConcurrency`
- `process` returns `[]` for an empty job list, so no worker is invoked
- Duplicate IDs are found with a left-to-right pass over a `Set<String>`; the first failed `insert` is thrown as `.duplicateID(id)` before the task group is created

### Bounded scheduling
- One `withTaskGroup` per `process` call; `nextIndex` points to the next unstarted job and only increases (input-order scheduling)
- Initial fill starts up to `min(maximumConcurrency, jobs.count)` children
- `while let ... = await group.next()` stores one finished result and starts the next job, so active workers never exceed the limit

### Indexed result storage
- Each child returns `(index, JobOutcome)` and is written to a `[JobOutcome?]` slot, so completion order does not matter
- `zip(jobs, results)` builds the output in input order; a `nil` slot (never started) becomes `.cancelled`

### Outcome mapping
- A non-throwing `static` helper maps the worker result: value → `.succeeded`, `CancellationError` → `.cancelled`, any other error → `.failed`

### Cancellation
- New children are added with `addTaskUnlessCancelled`, which returns `false` once the caller is cancelled; `nextIndex` is not advanced and the loop just drains running children
- Running children are cancelled automatically with the parent; cooperative workers throw `CancellationError` → `.cancelled`
- Outcomes stored before cancellation are kept

## Milestones

- [x] Validate configuration and all job IDs before starting work.
- [x] Schedule jobs within the concurrency limit and preserve input order.
- [x] Map success, ordinary errors, and cancellation outcomes.
- [x] Handle caller cancellation and never-started jobs.
- [x] Add at least six distinct student-authored tests and run `swift test`.

## Risks and checks

| Risk | Check |
| --- | --- |
| Worker starts before a duplicate is detected / wrong "first" duplicate | Test with `a, b, b, a`: throws `.duplicateID("b")`, worker never called |
| More than `maximumConcurrency` workers at once | Gate test: job N+1 starts only after a gated job is released |
| Results in completion order | Gate test releasing jobs in reverse order |
| One failing job stops the others | Mixed test: success, ordinary error, `CancellationError` in one call |
| Caller cancels while jobs are queued | Cancel the outer `Task`; queued jobs never start and are `.cancelled` |
| Undefined cases (duplicate + already cancelled; worker ignores cancellation) | Not graded; validation runs first, and a returned value stays `.succeeded` |
