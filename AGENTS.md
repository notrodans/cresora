# Go Rules

* Prefer idiomatic Go over object-oriented rules.
* Follow DDD, DRY, and YAGNI.
* Prefer simple, explicit, correct, and maintainable designs over architectural purity; use abstractions and language features in moderation.
* Prefer composition over embedding; embed only when promoted methods belong to the public API.
* Give each package one clear primary responsibility, keep dependency direction explicit, and use short, clear package names that do not repeat the package name.
* Domain code must not depend on transports, databases, frameworks, or vendor types.
* Application code coordinates use cases through consumer-defined ports.
* Infrastructure implements ports and contains technology-specific types.
* Define interfaces where consumed.
* Keep interfaces small; do not create speculative interfaces.
* Accept interfaces and normally return concrete types.
* Export only what other packages need.
* Prefer useful zero values; use constructors when invariants or required dependencies demand them.
* Constructors may validate and initialize state but should not perform unexpected I/O.
* Prefer value semantics for small immutable values and pointers for identity, mutation, synchronization, or large structs.
* Mutation is allowed only through operations that preserve invariants.
* Avoid generic setters; prefer domain operations such as `Start`, `Pause`, and `Complete`.
* Getters are allowed; omit the `Get` prefix.
* Package functions are allowed when no receiver state is required.
* Use conventional names such as `Reader`, `Writer`, and `Sender`.
* Use type assertions and type switches when they are clearer than additional abstractions.
* Avoid reflection unless static typing, generics, or explicit code would be substantially worse.
* Use generics only for genuinely reusable algorithms or data structures.
* Validate nil at genuine external boundaries and when nil is documented or represents optional behavior.
* Trusted internal required arguments and dependencies, including constructor arguments and context.Context, must be non-nil by contract; do not add defensive nil checks solely to improve a later panic.
* Keep nil handling for HTTP/CLI input, database/SDK/interface results, typed-nil hazards, optional dependencies, and cleanup or shutdown paths that can legitimately observe nil.
* Constructors validate domain/configuration values and establish invariants; they need not validate trusted required dependencies for nil.
* Private methods may rely on established invariants.
* Nil receivers are unsupported unless explicitly documented.
* Avoid typed nil values stored in interfaces.
* Use guard clauses and early returns to keep control flow shallow.
* Format all code with `gofmt`.
* Do not impose formatting rules that conflict with `gofmt`.
* Remove duplication only when it represents the same knowledge.
* Prefer small duplication over a premature abstraction.
* Do not add extension points, configuration, infrastructure, or abstractions without a current requirement.
* Do not use mutable package-level state; pass state through callers or keep it in explicit instances.
* Concurrency belongs to the caller: do not hide unbounded or untracked goroutines; every goroutine must have an explicit owner, cancellation path, and stoppable lifecycle.

# Errors and Logging

* Return errors for expected and recoverable failures; do not use `panic`/`recover` as exception-style control flow. Panic only for broken internal invariants or unrecoverable initialization.
* Wrap errors with `%w` when preserving their identity.
* Use `errors.Is` and `errors.As`; do not inspect error strings.
* Error messages must be lowercase, contextual, and have no trailing period.
* Handle errors only when the current layer can recover, translate, retry, or add useful context.
* Do not repeatedly log and return the same error.
* Use structured logging with stable attribute names.
* Do not log secrets or unnecessary personal data.
* Pass `context.Context` as the first parameter for cancellable or request-scoped operations.
* Do not store request contexts in structs.

# Testing Rules

* Place tests beside source files as `*_test.go`.
* Test public APIs through observable behavior, not implementation details.
* Every behavior change and bug fix must have a test.
* One test should cover one coherent behavior; multiple related assertions are allowed.
* Use descriptive names such as `TestMailing_StartsPendingMailing`.
* Prefer the standard `testing` package and `go-cmp` for complex comparisons.
* Failure messages must show the operation, actual value, and expected value.
* Test semantic error identity with `errors.Is` or `errors.As`.
* Test constructors only when they validate, default, initialize, or establish invariants.
* Tests must not depend on order or share mutable state.
* Avoid mutable package-level fixtures.
* Use helpers with `t.Helper()` and cleanup with `t.Cleanup()`.
* Use `t.TempDir()` for temporary files.
* Unit tests must not require Internet access.
* Use local test servers, real temporary files, and OS-assigned ephemeral ports.
* Prefer deterministic inputs in unit tests.
* Use Unicode and boundary inputs where relevant.
* Use fuzz tests for randomized input exploration.
* Prefer small fakes and stubs; use mocks only when interactions are part of the contract.
* Do not add production APIs used only by tests.
* Concurrent code must have bounded tests and pass the race detector.
* Do not use arbitrary sleeps for synchronization.
* Do not hide flaky tests with retries.
* Use eventual assertions only for genuinely asynchronous convergence and always provide a deadline.
* Do not assert on incidental logging.
* Use real database engines for integration tests when engine-specific behavior matters.
* Separate external integration tests from ordinary unit tests.

# Architecture Patterns

* Use patterns only to solve an observed problem; do not implement a pattern by name alone.
* Model objects with identity and lifecycle as entities.
* Model validated values without identity as immutable value objects.
* Use aggregates as minimal consistency boundaries.
* Keep large collections such as deliveries outside the `Mailing` aggregate.
* Modify aggregate state only through operations preserving its invariants.
* Represent each mailing execution as a separate `MailingRun`.
* Store an immutable run snapshot when later configuration changes must not affect active work.
* Represent one recipient operation as a separate `Delivery` with its own state, attempts, and scheduling time.
* Use application services to orchestrate use cases, transactions, repositories, and external ports.
* Define repository and external-service interfaces in the consuming package.
* Use ports and adapters to keep SQL, Telegram, HTTP, and broker types outside the domain.
* Use an anti-corruption layer to translate external models into application and domain types.
* Model transport capabilities explicitly when different transports or targets support different operations.
* Keep transport session lifecycle inside the owning transport component.
* Record significant architectural choices as ADRs with context, decision, consequences, status, and rejection conditions.

# Horizontal Scaling

* Assume that multiple identical application replicas may run concurrently.
* Keep durable work, ownership, progress, and desired state outside process memory.
* Treat process-local registries and caches as rebuildable and non-authoritative.
* Do not create one long-lived goroutine per mailing.
* Use bounded worker pools whose size is independent of the number of persisted tasks.
* Separate the control plane from the data plane.
* The control plane creates runs, schedules work, and changes desired state.
* The data plane claims and executes prepared deliveries.
* Scale control-plane and data-plane components independently.
* Claim tasks atomically in small batches.
* With PostgreSQL queues, use short transactions and `FOR UPDATE SKIP LOCKED`.
* Store claim owner and expiration when work may outlive the claim transaction.
* Return expired claims to an executable state through reconciliation.
* Use competing consumers when task ordering across different keys is unnecessary.
* Partition work by a stable key when related operations require ordering or shared state.
* Route operations for the same `account_id` to the same active owner.
* Expect hot keys and prevent one account from consuming all worker capacity.
* Use per-key concurrency and rate limits where external limits belong to that key.
* Use account-oriented workers when sessions, rate limits, or ordering belong to an account.
* Use actor-like ownership for long-lived state bound to one account.
* Use a mutex only for small process-local state with short and explicit critical sections.
* Never use an in-process mutex as distributed coordination.
* Use leases for temporary ownership of resources.
* Pair leases with monotonically increasing fencing tokens.
* Reject writes made with an obsolete fencing token.
* Do not assume lease expiration alone prevents split-brain.
* Use leader election only for work that must have exactly one active executor.
* Prefer partitioning or idempotent competing consumers over a global leader.
* Ensure every replica can lose ownership without corrupting state.

# Idempotency and Delivery Guarantees

* Assume asynchronous work is delivered at least once.
* Do not promise exactly-once external effects unless the external system supports them.
* Make commands, scheduling, generation, and state transitions idempotent.
* Use deterministic idempotency keys for repeatable operations.
* Enforce idempotency with database unique constraints where possible.
* Treat uniqueness conflicts as expected concurrent outcomes.
* Use optimistic concurrency for competing state transitions.
* Include the expected state, version, or fencing token in conditional updates.
* Do not retry optimistic conflicts blindly; reload and re-evaluate the operation.
* Persist delayed work with `ready_at` instead of sleeping inside a worker.
* Keep retries as new scheduled attempts, not blocked goroutines.

# Load Control

* Bound queues, channels, claim batches, and active operations.
* Apply backpressure before memory, database connections, or external limits are exhausted.
* Stop producing new work when downstream backlog exceeds its configured limit.
* Use load shedding when waiting would make the system less reliable.
* Generate large delivery sets in chunks.
* Persist the generation cursor in the same transaction as each generated chunk.
* Keep transactions small enough to avoid excessive locks, WAL bursts, and timeouts.
* Use partial indexes for active queue states.
* Order composite-index columns according to actual filtering and ordering queries.
* Verify queue indexes with real query plans and representative data.

# Failure Handling

* Classify external failures as delayed, transient, permanent, or owner-fatal.
* Retry only errors classified as retryable.
* Use exponential backoff with jitter.
* Limit retries with an explicit retry budget.
* Respect server-provided retry delays such as flood waits.
* Move exhausted or permanently invalid tasks to a terminal or dead-letter state.
* Preserve enough structured failure information for diagnosis and controlled replay.
* Use circuit breakers only for repeated dependency failures that would otherwise exhaust resources.
* Scope circuit breakers narrowly, such as by transport endpoint or account.
* Do not use retries and circuit breakers as substitutes for error classification.

# Reconciliation and Lifecycle

* Store desired state separately from actual execution state when commands complete asynchronously.
* Use reconciliation loops to move persisted state toward the desired state.
* Make every reconciliation step idempotent and safe after restart.
* Do not rely on an in-memory timer as the source of truth for persistent schedules.
* Persist the next scheduled action and claim due work through a scheduler.
* Prevent duplicate scheduled runs with a unique occurrence key.
* During shutdown, stop accepting and claiming new work first.
* Drain active operations within a bounded timeout.
* Cancel remaining operations after the shutdown deadline.
* Release ownership when safe, but rely on lease expiration after abrupt failure.
* Supervisors must own worker startup, cancellation, restart policy, and shutdown.
* Prevent infinite restart loops with bounded restart policies.

# Scaling Verification

* Test multiple workers claiming from the same queue concurrently.
* Verify that one task is not simultaneously owned by multiple valid workers.
* Test lease expiration and takeover.
* Test that stale fencing tokens cannot update state.
* Test process failure before and after every durable state transition.
* Test failure after an external effect but before recording success.
* Test duplicate commands, retries, and scheduler executions.
* Test restart during chunked generation.
* Test backpressure with a deliberately slow consumer.
* Test graceful shutdown with active and queued work.
* Run concurrent tests with `go test -race`.
* Use failure injection for recovery paths that normal unit tests cannot reach.

# Observability

* Correlate logs, metrics, and traces with stable identifiers.
* Include relevant `mailing_id`, `run_id`, `delivery_id`, `account_id`, and `claim_id`.
* Monitor latency, traffic, errors, and saturation.
* Track queue depth, oldest-ready-task age, claim latency, retry rate, active ownership, and deliveries per second.
* Alert on sustained backlog growth, expired claims, restart loops, and hot partitions.
* Do not use logs as the only source of operational state.

# Queue Evolution

* PostgreSQL may be used initially when delivery state already resides there and transactional claims simplify consistency.
* Keep queue semantics behind application ports.
* Do not add a broker without measured need.
* Consider migration to a broker when database polling, write amplification, retention, routing, or throughput becomes a demonstrated bottleneck.
* Preserve idempotency, ownership, retry, and ordering rules when changing queue technology.

# Rejected Patterns

* Do not run one persistent goroutine or timer per mailing.
* Do not keep durable scheduling state only in memory.
* Do not coordinate replicas with mutexes or process-local maps.
* Do not use leases without fencing for safety-critical ownership.
* Do not allow Telegram or generated SDK types into domain packages.
* Do not place all deliveries inside one large aggregate.
* Do not create unbounded goroutines, channels, retries, or batches.
* Do not rely on worker shutdown to release every claim.
* Do not introduce a global leader for work that can be partitioned or made idempotent.

# Avoid Overengineering

* Implement the simplest design that satisfies current confirmed requirements.
* Do not design for hypothetical scale, transports, databases, or use cases.
* Do not add an abstraction until it removes real duplication, coupling, or complexity.
* Do not create an interface for a single implementation unless it defines an architectural boundary or enables meaningful substitution.
* Do not add factories, builders, registries, strategies, plugins, or dependency containers without a current need.
* Do not use generics when concrete types are clearer.
* Do not create value objects that add no validation, behavior, or type safety.
* Do not split code into layers that only forward calls without adding policy or translation.
* Do not introduce a message broker, distributed lock, leader election, actor system, or cache without a demonstrated requirement.
* Before optimizing, benchmark representative behavior and confirm a relevant bottleneck; do not optimize based on guesswork.
* Prefer standard-library solutions over custom frameworks and infrastructure.
* Prefer explicit code over reusable machinery used only once.
* Prefer a small amount of duplication over a premature shared abstraction.
* Extract shared code only when duplicated fragments represent the same knowledge and change for the same reason.
* Keep configuration limited to values that users or deployments currently need to change.
* Do not add extension points for imagined future requirements.
* Do not implement every known pattern from the architecture document.
* Use a pattern only when its stated problem exists in the current system.
* Every new abstraction must identify:
  * the concrete problem it solves;
  * its current consumers;
  * the simpler alternative considered;
  * the cost it introduces.
* Reject an abstraction when its maintenance cost exceeds the complexity it removes.
* Prefer reversible local decisions over early global architecture.
* Record future ideas as notes or ADR alternatives instead of implementing them.
* During review, mark unnecessary abstractions and infrastructure as overengineering, not as optional improvements.


# Required Checks

```text
gofmt
go test ./...
go vet ./...
go test -race ./...
```

Use `staticcheck` when configured by the project.

Do not suppress linter warnings unless the suppression is narrowly scoped, documented, and unavoidable.
