# Package `accountowner`

Process-local ownership of live gotd Telegram clients, one per
operator/account scope, with versioned admission fencing.

The registry is a **single-deployment, process-local component**. Session
bytes and durable lifecycle state live elsewhere (in the database); this
package owns only the currently running clients, the admission gates that
serialize their use, and the fences that prevent a stopped lifecycle version
from coming back. Because it is process-local, every replica of the
application runs its own registry; nothing here is authoritative across
replicas.

---

## 1. Responsibilities

- Construct, run, and tear down exactly one gotd client per
  `(operatorID, accountID)` key.
- Serialize operations on one account so at most one operation touches a
  client at a time.
- Admit operations against a **lifecycle version** and refuse stale or
  invalid admission targets.
- Evict idle owners and enforce a bounded runtime capacity.
- Stop and revoke accounts safely, including draining in-flight callbacks
  before tearing down a client.

It does **not** do account auth, message delivery, or telemetry itself.
Transport packages (`operatoraccountauth`, `account`) call into the registry
through `Execute` and provide their own callback logic.

---

## 2. Core concepts

### 2.1 `accountKey` and `accountSlot`

`accountKey{operatorID, accountID}` deliberately **excludes the lifecycle
version** (`account_slot.go:20`). All versions of one operator account must
pass through one serialized `accountSlot`, so a replacement owner can never
overlap an older operation.

An `accountSlot` is the per-account consistency boundary:

- `gate` — a capacity-1 channel that serializes account operations.
- `current` — the currently admitted `runtimeEntry`.
- `handles` / `active` — counts of open handles and in-flight operations.
- `closed` / `stopping` — admission state.
- `revokeRunning` / `revokeWaiters` / `revokeChanged` — the bounded
  per-account rendezvous for privileged revoke operations.
- `teardownGate` — the single teardown gate shared by stop, eviction, and
  revoke.

### 2.2 Lifecycle version

Every operation carries an `operatoraccounts.RuntimeTarget` that includes a
`Version` and a `Status`. The version is admission metadata on the account
key, **not** a second client key. Replacing a version shares the same slot and
gate (`registry.go:13`).

### 2.3 `runtimeEntry`

A `runtimeEntry` represents one concrete lifecycle version of an owner
(`runtime_entry.go:13`). It is installed as `slot.current` while building,
running, draining, and tearing down its owner. The `built` channel and
`buildErr` let callers wait for the lazy client construction.

### 2.4 Owner

An `owner` wraps one factory-created gotd client (`owner_runtime.go:48`). It
exposes:

- `Run` — starts the one-shot gotd run; the dispatcher goroutine lives inside
  it, so `Run` cannot return while an admitted callback is executing.
- `Execute` — queues a callback to the dispatcher; the client never escapes
  through the owner API, callbacks receive it only while admitted.
- `WaitReady` / `Wait` — readiness and completion waits.
- `Stop` — cancels the run.

### 2.5 Fences

`stoppedFence{version, stamp, protected}` records the greatest stopped
version for a key (`fence.go:30`). `reserve` rejects any target whose version
is `<=` the recorded fence, so a stopped or revoked account cannot restart
through the ordinary admission path. Fences are bounded by
`config.Capacity`; protected fences (revoke/stop in progress) cannot be
evicted until teardown unprotects them.

---

## 3. Public API

| Symbol | Purpose |
| --- | --- |
| `NewRegistry(RegistryConfig)` | Construct a registry without starting any client. |
| `(*Registry) Execute(ctx, target, callback)` | Run one admitted operation against an account. |
| `(*Registry) StopAccount(ctx, target)` | Stop one exact lifecycle target. |
| `(*Registry) RevokeAndStop(ctx, target, callback)` | Fence, drain, run one privileged callback, tear down. |
| `(*Registry) Stop(ctx)` | Stop every admission and join all owners. |
| `ClientCallback` | `func(context.Context, *gotdtelegram.Client) error`. |

`RegistryConfig` (`registry_config.go:21`) fields and defaults:

| Field | Default |
| --- | --- |
| `Factory` | required |
| `AppID` / `AppHash` | required |
| `Capacity` | 32 |
| `IdleTimeout` | 5 min |
| `DrainTimeout` | 5 s |

Errors (`errors.go`) are sentinels; match them with `errors.Is`:

- `ErrRegistryStopped` — process-wide runtime no longer admits.
- `ErrAccountStopped` — admission for this scope was closed.
- `ErrStaleAdmission` — operation used an older lifecycle version.
- `ErrInvalidAdmission` — requested lifecycle state cannot own a runtime.
- `ErrRuntimeCapacity` — all slots occupied by non-idle accounts.
- `ErrFenceCapacity` — a new stop fence cannot be recorded.
- `ErrAlreadyRun` / `ErrStopped` (from the owner) — owner already started or
  completed without becoming ready.

---

## 4. The admission path (`Execute`)

`Registry.Execute` (`registry.go:83`) is a complete, scoped admission per
logical operation:

1. **Validate** the target (`validateAdmission`, `admission.go:142`) — only
   `Authenticating`, `Active`, and `ReauthRequired` statuses may run.
2. **Reserve** (`reserve`, `admission.go:11`):
   - Reject if the registry is stopped or the target is fenced.
   - Reuse the existing slot/entry when the target matches and the slot is
     not stopping.
   - Reject `ErrStaleAdmission` (older version) or `ErrInvalidAdmission`
     (same version, different state).
   - For a **newer** version, keep the old entry installed while it drains
     (`admission.go:71`): a replacement never constructs or starts a second
     client for the account. Close admission, tear the old owner down, retry.
   - Otherwise publish a new `runtimeEntry`, reserve a handle, and start
     `buildEntry` (lazy client construction) in a goroutine.
3. **Wait** for build and readiness (`open`, `registry.go:99`): wait on
   `entry.built`, then `owner.WaitReady`.
4. **Re-check admission** before returning the handle.
5. `defer handle.Close()` releases the admission reference.

### The handle

`handle` (`handle.go:13`) is a scoped admission that does not expose a gotd
client. `handle.Execute` reserves the handle for the duration of one
operation and delegates to `runtimeEntry.execute`.

### `runtimeEntry.execute`

`runtime_entry.go:47`:

1. Acquire the slot `gate` (serialize account operations).
2. `slot.beginOperation` — the linearization point: either the operation
   becomes active before admission closes, or it observes the closed state and
   is rejected (`account_slot.go:115`).
3. Load the owner; if absent, `ErrAccountStopped`.
4. `owner.WaitReady(operationContext)`.
5. `owner.Execute` with a wrapper that re-checks `checkAdmission` and
   callback-context cancellation immediately before invoking the real
   callback (`runtime_entry.go:85`).
6. Release the gate and finish the operation.

### `owner.Execute` and the dispatcher

`owner.Execute` (`owner_runtime.go:241`) queues the callback to the
dispatcher goroutine owned by `client.Run` (`dispatch`, `owner_runtime.go:290`).
Guarantees:

- The client cannot be retained by the caller; it is passed only inside the
  callback.
- Once queued, `Execute` does not return on caller cancellation — the result
  or `owner.done` is the proof the dispatcher did not invoke, or finished
  invoking, the callback.
- Caller cancellation still reaches the callback because the request context
  is the callback context.

---

## 5. Stopping

### `StopAccount` (`shutdown.go:13`)

Closes admission for the **exact** target, cancels the active operation,
records a fence for the stopped version, then bounds drain + owner teardown
via `teardown`. The registry lock is never held while waiting on a callback
or gotd.

### `Stop` (`shutdown.go:68`)

Idempotent (guarded by `stopOnce`). Closes every admission, cancels all
active operations, tears down every owner within the supplied context and
`DrainTimeout`, stops the idle reaper, and cancels the registry root context
once complete.

### `teardown` (`shutdown.go:151`)

Per-account teardown: wait for any running revoke, take the `teardownGate`,
wait for build and drain, `owner.Stop()` + `owner.Wait(...)` (stopping gotd
only after callback drain completes — stopping earlier would let `Run` return
underneath an admitted callback), then `finishTeardown` and unprotect the
fence.

---

## 6. Revocation (`RevokeAndStop`)

`revoke.go:20` is the final operation allowed after the N+1 fence — a
privileged callback deliberately outside the ordinary admission path, e.g. to
terminate the session on Telegram.

1. `validateRevokeTarget` — only a `Disconnecting` status with version
   `> InitialVersion`.
2. `acquireRevoke` claims the account's single `teardownGate` and publishes
   the revoke as running; waiters are counted before blocking and use the
   gate rather than a one-shot channel so queued retries are not missed.
3. Record a **protected** fence, close admission, and reuse the previous
   owner (version N-1) if present.
4. Drain ordinary callbacks, wait for readiness, run exactly one privileged
   callback.
5. Tear the owner down unconditionally. A panic in the callback is recovered
   only long enough to stop and join the owner, then **re-raised** — owner
   teardown stays unconditional without turning programmer errors into runtime
   failures (`revoke.go:15`).

---

## 7. Eviction and capacity

- `makeCapacity` (`eviction.go:10`) evicts one **idle** slot when live count
  has reached capacity; if nothing is evictable, admission fails with
  `ErrRuntimeCapacity`.
- `reapIdle` (background goroutine started by `NewRegistry`) periodically
  evicts owners idle past `IdleTimeout` — no open handles, no active
  operations, and unused for the timeout (`account_slot.go:75`).
- Eviction tears down through the same `teardown` path as `Stop`.

---

## 8. Concurrency model

- `registry.mu` guards the account table, the fence set, and registry-level
  shutdown state.
- `accountSlot.mu` guards the fields of one slot.
- Locks nest strictly as `registry.mu` outside `slot.mu`, never the reverse.
- Helpers that read or mutate guarded state lock themselves unless their doc
  comment states the caller must already hold the lock.
- No in-process mutex is used as distributed coordination; correctness across
  replicas relies on durable state and idempotent reconciliation elsewhere.

---

## 9. Invariants

- At most one gotd client runs per operator/account at any time; a newer
  version never starts before the old owner drains.
- A fenced/stopped/revoked version cannot restart through the ordinary
  admission path.
- `Run` cannot return while an admitted callback is still executing.
- A callback panic in a revoke still results in owner teardown.
- Fences and slots are bounded by capacity; teardown is bounded by
  `DrainTimeout` and the caller's context.

---

## 10. Consumers

- `internal/transport/telegram/operatoraccountauth/adapter.go` — phone
  authentication (`SendCode`, `SignIn`, `Password`) through
  `Registry.Execute`.
- `internal/transport/telegram/account/resolver.go` — delivery commands
  through a runtime-gated port that also routes `Execute` through the
  registry.
- `cmd/app/main.go` — constructs the registry and wires shutdown.
