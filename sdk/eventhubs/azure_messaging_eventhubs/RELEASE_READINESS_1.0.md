<!-- cspell:ignore mgmt worktrees typestate -->

# `azure_messaging_eventhubs` 1.0.0 readiness

Assessment date: 2026-07-20. Tree under assessment: `main` at `b7e1b8671`, plus
the harness commits on this branch. Crate version in tree: `0.15.0`. Latest
version on crates.io: `0.15.0`.

## Verdict

**Not ready today. The list of work that remains is short, bounded, and
mechanical, apart from one defect.**

The blocker that made 1.0 impossible for structural reasons is gone. The crate
now builds against published GA core crates and packages cleanly, which is
proved by `cargo publish --dry-run` in the offline gate. Nothing in the current
tree is an unannounced API break against the published 0.15.0, which
`cargo-semver-checks` proves.

Five items block the version bump. Two are real defects, one of which makes a
documented feature a silent no-op. The other three are API and release hygiene
that become permanent the moment 1.0 ships.

| # | Blocker | Class | Evidence |
|---|---|---|---|
| 1 | `owner_level` never reaches the broker | Defect | Live wire trace |
| 2 | `mgmt_client` recovery self-deadlock | Defect | Reproduced by test |
| 3 | `#[non_exhaustive]` missing on 16 public types | API contract | Inspection |
| 4 | Four panicking `From<MessageId>` impls | API contract | Inspection |
| 5 | Release mechanics: stale CHANGELOG headers | Release | Inspection |

Blocker 1 has a consequence for sequencing. The fault is in `azure_core_amqp`,
not in this crate, and it is present in the published `1.1.0` that this crate
links against. Event Hubs 1.0.0 therefore waits on a corrected
`azure_core_amqp` release. That is a scheduling dependency between two crates,
not a return of the old prerelease coupling.

Read the evidence classes below before you act on any single line. The four
classes are kept separate on purpose. A test cannot prove an API contract item,
and inspection cannot prove the service behaves as expected. Treating them as
one list is how a GA ships with a hang in it.

## Evidence class 1: machine-verified

### Offline gate

Command: `./sdk/eventhubs/azure_messaging_eventhubs/verify-offline.sh`
Run from a normal checkout at commit `6e51bbcbf`. Result: **8 passed, 0 failed,
0 skipped.**

| Step | Result | Note |
|---|---|---|
| `cargo fmt --check` | PASS | |
| cspell with `.vscode/cspell.json` | PASS | The config CI uses |
| `cargo build --all-features --all-targets` | PASS | |
| `cargo clippy -D warnings` | PASS | |
| `cargo test --all-features` | PASS | 160 passed, 0 failed |
| `cargo doc -D warnings` | PASS | Includes `missing_docs` |
| `cargo publish --dry-run` | PASS | The crate is publishable |
| `cargo-semver-checks` vs published | PASS | No unannounced break |

Test totals from that run: 109 library unit tests, 5 checkpoint-store tests, 1
producer test, and 45 documentation tests pass. 49 live integration tests
self-skip without credentials. 13 library tests are ignored, which includes the
two deadlock tests described below.

The gate skips its test step inside a linked git worktree. A test binary built
under `.claude/worktrees` cannot start a test-proxy session, because
`#[recorded::test]` resolves its recording paths from the compile-time
`CARGO_MANIFEST_DIR`. Run the gate from a normal checkout to exercise that step.

### The `mgmt_client` self-deadlock, reproduced

Command:
`cargo test --package azure_messaging_eventhubs --lib -- --ignored ensure_amqp_management`

Two ignored tests in `src/common/recoverable/connection.rs` split the proof.

`ensure_amqp_management_holds_mgmt_lock_across_build` **passes**. It points the
connection at a local listener that accepts the socket and never sends the AMQP
protocol header, so `create_connection` blocks inside the locked region. The
test polls until `mgmt_client.try_lock()` fails, then re-checks one second later
that the guard is still held and the build task still runs. This shows by
execution that the production path holds the guard across the whole
management-client build.

`ensure_amqp_management_recovery_self_deadlocks` **fails by timeout**, which is
the expected evidence. It holds the guard exactly as `ensure_amqp_management`
does, then runs the real `recover_from_error(..., ReconnectConnection)` under a
10 second timeout. Recovery never completes. `async_lock::Mutex` is not
reentrant, so `apply_recovery_plan` waits for a guard its own task already
holds.

Both tests are `#[ignore]`d, so the default suite stays green.

The deadlock is also confirmed against a real broker, which removes the last
doubt about whether it is reachable. A live probe corrupted the shared access
key so that CBS authorization of the `$management` path fails inside the build,
then made 20 concurrent `get_eventhub_properties` calls. On `main` the recovery
hook fired on the task that held the guard, logged `ReconnectLink` and the cache
clears, and never reached the management-client drop. The process then produced
no log line for seven minutes, with spans reporting `time.idle=479s`, until the
harness stopped it. The whole client stopped, not only the calls that failed.

With PR #4806 the same probe finishes. All 20 callers report an authorization
error in 93.1 seconds, both recovery plans run to completion, and later calls
report errors in about 6 seconds, so a failed build leaves the cache retryable.
A separate probe made 100 concurrent first calls on a cold client and the log
shows one management build, so callers share a single build in flight. Warm
calls measure a median of 279 ms against 277 ms on `main`.

The repro needs the concurrent burst, and it was run once per branch, so there
is no repro rate.

### Live capability matrix

Command:
`cargo run --package azure_messaging_eventhubs --example eventhubs_smoke_test`
Run against a real namespace with 5 partitions, consumer group `$Default`.
Result: **21 passed, 2 failed, 2 skipped.**

| Phase | Capability | Result |
|---|---|---|
| P1 | Connect a producer | PASS |
| P1 | `get_eventhub_properties` | PASS, 5 partitions |
| P1 | `get_partition_properties` for every partition | PASS, 5 queried |
| P2 | `send_event`, partition-addressed | PASS |
| P2 | `send_event`, gateway-routed | PASS |
| P2 | `send_batch` | PASS, 5 events |
| P2 | A full batch refuses the next event | **FAIL** |
| P3 | Connect a consumer | PASS |
| P3 | Round trip, every event verified by marker | PASS, 5 of 5 |
| P3 | Broker system properties populated | PASS |
| P3 | Read from `StartLocation::Earliest` | PASS |
| P4 | Build an `EventProcessor` | PASS |
| P4 | Claim a partition | PASS |
| P4 | Attach the partition receiver | PASS |
| P4 | Receive and `update_checkpoint` | PASS |
| P4 | Checkpoint persisted in the store | PASS |
| P5 | Open and attach two receivers at owner levels 0 and 1 | PASS |
| P5 | The displaced receiver reports `ConsumerDisconnected` | **FAIL**, fixed by #4805 and #4807 |
| P5 | The displacing receiver receives | PASS |
| P6 | Idle recovery soak | SKIP, opt-in |
| P7 | Pre-formed SAS expiry | SKIP, opt-in |

Both failures are defects in the code under test, not in the harness. Each is
described below. Two earlier failures in the first live run, a processor that
never delivered and a displacing receiver that never received, were harness
faults and are fixed: receivers attach lazily on the first poll of
`stream_events()`, so the harness now primes each receiver before it sends
anything.

### Blocker 1: `owner_level` never reaches the broker

`OpenReceiverOptions::owner_level` is documented, public, and has no effect.
The epoch property is dropped before the AMQP Attach frame is built, so the
broker never learns that a receiver claims an owner level. Epoch-based
single-owner consumption is a documented Event Hubs guarantee. This crate
advertises it and does not deliver it.

The chain:

1. `open_receiver_on_partition` puts `com.microsoft:epoch` and
   `com.microsoft.com:receiver-name` into `AmqpReceiverOptions::properties`
   (`src/consumer/mod.rs:293` and `:299`).
2. `azure_core_amqp` builds the fe2o3 link with `.properties(...)` and then
   `.name(...)`, in that order
   (`azure_core_amqp-1.1.0/src/fe2o3/receiver.rs:63-69`).
3. In `fe2o3-amqp` 0.14.0, `Builder::name()` is a typestate transition that
   returns a rebuilt `Builder` with `properties: Default::default()`
   (`fe2o3-amqp-0.14.0/src/link/builder.rs:185`). Calling `.name()` after
   `.properties()` therefore discards the properties. `.source()` preserves
   them; `.name()` does not.

The outgoing Attach frame, captured live with `fe2o3_amqp=trace`, confirms it:

```text
frame=Attach(Attach { name: "305abb16-...", role: Receiver,
  source: Some(Source { ... filter: Some(... "x-opt-offset > '@latest'") ... }),
  ..., properties: None })
```

Causation was confirmed, not just correlation. With the two builder lines
swapped in a local copy of `azure_core_amqp` 1.1.0, injected through
`--config patch.crates-io`, the broker displaces the lower-epoch receiver at
once:

```text
condition: LinkStolen, description: "Receiver 'nil' with a higher epoch '1'
already exists. Receiver 'nil' with epoch 0 cannot be created..."
```

The `'nil'` in the broker's own message is the same bug: the receiver-name
property is missing too.

The fix is to call `.name(name)` before `.properties(...)`. The sender path has
the same ordering fault (`azure_core_amqp-1.1.0/src/fe2o3/sender.rs:70-78`).
The in-tree `azure_core_amqp` at `1.2.0-beta.1` has not fixed either one, so
this needs a real change and a release, not a version bump.

Nothing in the test suite inspects an outgoing Attach frame, which is why this
survived. A regression test should assert that the frame carries
`com.microsoft:epoch`.

### The existing epoch test cannot pass, and has probably never passed

`tests/eventhubs_processor.rs:444`
(`second_processor_displaces_first_with_consumer_disconnected`) documents itself
as the end-to-end guard for the `amqp:link:stolen` translation. Both routes to
its assertion are closed, so it can only time out and panic.

The broker route is closed because the epoch never reaches the wire (blocker 1),
so two receivers at owner level 0 are just two ordinary readers. Event Hubs
allows five concurrent readers per partition per consumer group, so the broker
has no reason to displace either one.

The local route is closed too. `revoke_partition_clients`
(`src/event_processor/processor.rs:152`) only runs for partitions the load
balancer reports as taken away. The test gives each processor its own
checkpoint store (`tests/eventhubs_processor.rs:463` and `:483`), and
`InMemoryCheckpointStore` holds per-instance state with no shared statics
(`src/in_memory_checkpoint_store.rs:17`). The two ownership tables are disjoint,
so processor A never observes processor B, nothing is reported stolen, and
`request_close()` is never called.

The test is `#[recorded::test(live)]`, so CI never runs it. PR #4439, which
introduced epoch-0 receivers and `ConsumerDisconnected`, has an unchecked box in
its own test plan for this test. The feature shipped with its only end-to-end
guard never executed, which is why blocker 1 went unnoticed.

Repairing the test needs three changes, not one:

1. Fix the link-builder ordering so the epoch reaches the wire.
2. Assert `ConsumerDisconnected(Some(d))` and that `d.condition` is
   `LinkStolen`, which no local path can produce.
3. Give both processors a shared checkpoint store, or state in the test that the
   local revoke path is excluded by construction.

A unit test in `azure_core_amqp` that asserts the properties survive into the
Attach frame is the cheaper and more reliable guard, because it needs no broker
and therefore runs in CI.

### Displacement does not translate to `ConsumerDisconnected`

This defect is masked by blocker 1 and appears once the epoch reaches the wire.
With the patched `azure_core_amqp`, a displaced receiver still does not report
`ConsumerDisconnected`. The traced sequence:

1. The broker sends `Detach` with `LinkStolen`. The in-flight receive fails as a
   `LinkStateError::RemoteClosedWithError` rather than an `AmqpDescribedError`,
   so `translate_receive_error` (`src/consumer/event_receiver.rs:29`) does not
   match it.
2. The retry layer classifies the failure as `ReconnectLink`, clears the cached
   receiver, and re-attaches at the old epoch.
3. The broker rejects the re-attach with `LinkStolen`. That error is flattened
   into `AmqpError::with_message(format!("Failed to ensure receiver: {e}"))`
   (`src/common/recoverable/receiver.rs:105`), which destroys the described
   error kind.
4. It leaves the stream through the `?` on `get_receiver`
   (`src/consumer/event_receiver.rs:202`), which never passes through
   `translate_receive_error`.

The caller sees `ErrorKind::AmqpError("Failed to ensure receiver: ...")`. The
0.15.0 CHANGELOG tells consumers to pattern-match on
`ErrorKind::ConsumerDisconnected`. Fix blocker 1 without fixing this, and the
documented pattern still does not work.

Both fixes are confirmed live. With PR #4805 patched into a copy of the
published `azure_core_amqp` 1.1.0, receiver A's Attach frame carries
`com.microsoft:epoch: Long(0)` and the broker echoes it back. A displacement by
a higher owner level then surfaces as
`ConsumerDisconnected(Some(AmqpDescribedError { condition: LinkStolen, .. }))`.
This holds in two configurations. With the published AMQP semantics the steal
arrives through the re-attach rejection. With the `RecvError` fix from PR #4807
also applied, it arrives directly from the in-flight receive, with one Attach
and no re-attach. The contract holds now and after the AMQP release.

Two gaps remain in this evidence. The probe used owner levels 0 and 1, so
same-epoch displacement is not covered, and that is what `EventProcessor` uses.
The sender path was not exercised.

## Evidence class 2: verified by inspection

### The former hard blocker is resolved

The crate used to depend on `azure_core_amqp` by path at a `1.1.0-beta.1`
prerelease. A stable crate cannot be published against a dependency that does
not exist on crates.io, so 1.0 was impossible for reasons unrelated to code
quality.

Commit `b7e1b8671` ("Align dependency policy", #4801) moved workspace
dependencies to registry versions. The root `Cargo.toml` now declares
`azure_core_amqp` at `version = "1.1.0"` with no path, and the crate's own
manifest takes `azure_core_amqp.workspace = true`. `Cargo.lock` resolves it to
`1.1.0` from the crates.io registry. The in-tree member has since moved to
`1.2.0-beta.1`, and the Event Hubs crate no longer rides it.

The consequence for the public API is that AMQP types which appear in the public
surface, such as `AmqpMessage` and the `AmqpDescribedError` inside
`ErrorKind::SendRejected` and `ErrorKind::ConsumerDisconnected`, now come from a
stable 1.x crate. That is an accepted coupling decision, not a blocker: a
breaking change in `azure_core_amqp` 2.0 would force a matching major bump here.

`cargo publish --dry-run` passing is the machine confirmation of all of this.

### The token-refresh concern no longer applies

An earlier audit recorded that the token-refresh task is never aborted and can
spin without bound. That claim no longer holds. The refresh loop exits when the
connection drops, applies a floor delay after a failed refresh
(`src/common/authorizer.rs:27`), and excludes non-renewable pre-formed SAS
tokens. The authorizer also no longer holds its scope lock across CBS input and
output. Remove this from the blocker list.

### The CBS link in the deadlock argument

The reproduction proves two of the three links by execution. The third link,
that a connection-level CBS failure inside the locked region reaches
`recover_from_error`, is argued from the call chain, because no existing test
seam can fault CBS without a live broker:

- `ensure_amqp_management` (`src/common/recoverable/connection.rs:576`) takes the
  `mgmt_client` guard and holds it across the build.
- The build calls `authorizer.authorize_path`
  (`src/common/recoverable/management.rs:66`).
- `RecoverableClaimsBasedSecurity::authorize_path` passes a recovery hook into
  its retry loop (`src/common/recoverable/claims_based_security.rs:133`).
- `should_retry_amqp_error` maps `ConnectionDropped`, `FramingError`,
  `IdleTimeoutElapsed`, and `ConnectionForced` to `ReconnectConnection`, whose
  plan sets `drop_mgmt_client: true`.
- `apply_recovery_plan` then takes `self.mgmt_client.lock().await`
  (`src/common/recoverable/connection.rs:832`), on the task that already holds
  it.

Why the existing seams cannot close this last link:
`force_error` injects in `RecoverableManagementClient::call`
(`src/common/recoverable/management.rs:116`), which runs before the guard is
taken. `disable_authorization` makes `perform_authorization` return early
(`src/common/authorizer.rs:194`), so it skips CBS instead of failing it.
`disable_connection` fails at `session.begin`
(`src/common/recoverable/management.rs:60`) with a message that classifies to
`ReturnError`, so no recovery hook runs. The `test` feature of
`azure_core_amqp` adds error constructors only; it has no mock transport.

Closing this link needs either a live broker or a new test seam. Adding a
production seam was out of scope for the harness.

## Evidence class 3: API contract items

These cannot be proved by a test. They are read off the source, and each one
becomes permanent when 1.0 ships.

### `#[non_exhaustive]` is missing on 16 public types

Only two types carry the attribute today: `ErrorKind` (`src/error.rs:12`) and
`ConnectionString` (`src/common/connection_string.rs:39`).

Options types, where the attribute matters most, because adding a field later is
otherwise a breaking change:

| Type | Location |
|---|---|
| `SendEventOptions` | `src/producer/mod.rs:72` |
| `SendBatchOptions` | `src/producer/mod.rs:32` |
| `SendMessageOptions` | `src/producer/mod.rs:81` |
| `AddEventDataOptions` | `src/producer/batch.rs:12` |
| `EventDataBatchOptions` | `src/producer/batch.rs:358` |
| `OpenReceiverOptions` | `src/consumer/mod.rs:431` |
| `StartPosition` | `src/consumer/mod.rs:530` |
| `RetryOptions` | `src/common/retry.rs:36` |
| `StartPositions` | `src/event_processor/models.rs:145` |

Enums, where the attribute matters because adding a variant is otherwise a
breaking change:

| Type | Location |
|---|---|
| `StartLocation` | `src/consumer/mod.rs:450` |
| `ProcessorStrategy` | `src/event_processor/mod.rs:96` |
| `MessageId` | `src/models/mod.rs:141` |

Response and record models, where the service can add fields:

| Type | Location |
|---|---|
| `EventHubProperties` | `src/models/mod.rs:68` |
| `EventHubPartitionProperties` | `src/models/mod.rs:110` |
| `Checkpoint` | `src/event_processor/models.rs:18` |
| `Ownership` | `src/event_processor/models.rs:83` |

Adding `#[non_exhaustive]` before 1.0 costs nothing. Adding it after 1.0 is
itself a breaking change, so every type in this list freezes its field or
variant set for the life of the major version. Note that the passing
`cargo-semver-checks` result says nothing about this risk, because it compares
against 0.15.0 and not against a future release that cannot be made.

### Four `From<MessageId>` impls panic

`src/models/mod.rs:185`, `:194`, `:203`, and `:212` convert `MessageId` into
`Uuid`, `Vec<u8>`, `String`, and `u64`. Each one panics when the variant does
not match:

```rust
impl From<MessageId> for Uuid {
    fn from(message_id: MessageId) -> Self {
        match message_id {
            MessageId::Uuid(uuid) => uuid,
            _ => panic!("Cannot convert MessageId to Uuid"),
        }
    }
}
```

A `From` impl states that the conversion always succeeds. These conversions
fail for three of the four variants, and the input is data that comes off the
wire. The correct shape is `TryFrom`. Changing it after 1.0 is a breaking
change.

### `EventDataBatchOptions.max_size_in_bytes` is ignored

Found by the live harness, phase P2. A batch created with
`max_size_in_bytes: Some(1024)` accepted 64 events of 128 bytes each.

`EventDataBatch::new` reads the caller's value (`src/producer/batch.rs:68`),
but `attach()` then overwrites it without a comparison
(`src/producer/batch.rs:87`):

```rust
self.max_size_in_bytes = sender.max_message_size().await?.ok_or_else(|| { ... })?;
```

The field is public, is documented as "The maximum size of the batch in bytes",
and has a doc example that passes `Some(1024)` (`src/producer/batch.rs:351`).
It has no effect. A caller who caps a batch to bound memory, or to satisfy a
constraint downstream of the send, gets a silently over-filled batch instead.

The fix is to keep the caller's value when the link allows it, and to reject a
larger request with an error naming both sizes. It is not to reduce the value
silently. The .NET, Go, and Java Event Hubs clients all report an error here,
verified from their source; none reduces and none ignores. That behavior is not
in their published reference documentation, so it has to be read off the
implementations.

This is a behavior change and not an API break, so it does not have to precede
the version bump. Shipping a GA release with a documented option that does
nothing is still the wrong trade.

Fixed in PR #4808.

### `SendEventOptions` does not implement `Clone`

Found while writing the harness: the type cannot be cloned, so a caller that
sends twice with the same options must rebuild them. Azure SDK options types
normally derive `Clone`, `Debug`, and `Default`. Adding a derive is not a
breaking change, so this is a nicety rather than a blocker, but it is cheap and
it is the kind of gap only a consumer finds.

## Evidence class 4: open engineering work

No test in this harness waves these through. Each needs a decision.

### Recovery does not detach links

`apply_recovery_plan` clears the cached senders, receivers, sessions, and
management client, but it never detaches them. The recoverable wrappers'
`detach()` methods are `unimplemented!()` stubs
(`src/common/recoverable/receiver.rs:62`, `sender.rs:157`,
`claims_based_security.rs:150`). Only `close_connection` detaches, and only when
it holds the last `Arc` reference. An in-flight operation that still holds an
`Arc` therefore leaves a link attached on the broker side after recovery has
moved on.

### The #4454 stale-cache window is still open

The code acknowledges it in a comment at
`src/common/recoverable/connection.rs:806`. A slow path that attaches without
the map lock can install a resource bound to a connection that recovery has
already torn down. The generation-counter fix exists on a branch and is not on
`main`.

### No buffered producer

.NET ships `EventHubBufferedProducerClient`. Go and Python reached GA without an
equivalent, so this is a documented parity gap and not a blocker. Record the
decision rather than leaving it implicit.

## Release mechanics

- `CHANGELOG.md:3` reads `## 0.15.0 (Unreleased)`, but 0.15.0 is on crates.io.
- `CHANGELOG.md:25` has a second stale `## 0.14.0 (Unreleased)` header.
- `README.md:53` has two typographic errors: "Even Hubs" and "crating".
- `README.md:100` reads "already known which partitions"; it should read "know".
- `README.md:107` has a malformed link with a doubled closing bracket.
- `README.md:305` points the license at `azure-sdk-for-cpp`.
- `src/error.rs:33` repeats the "Even Hubs" error in a doc comment.

### Misleading attach log

`src/consumer/mod.rs:315` logs `info!("Receiver attached on partition.")` from
`open_receiver_on_partition`, which does no network work. The link attaches
later, on the first poll of `stream_events()`
(`src/consumer/event_receiver.rs:202`). The message states an event that has not
happened, and it misdirects anyone debugging from logs.

## Status of the fixes

Every blocker in this report was independently verified after it was written,
and each verified defect now has a change open against it.

| Item | Where |
|---|---|
| `owner_level` never reaches the broker | Issue #4804, PR #4805 |
| Displacement does not translate to `ConsumerDisconnected` | PR #4807 |
| `mgmt_client` recovery self-deadlock | Issue #4728, PR #4806 |
| `#[non_exhaustive]` on 16 public types | PR #4722, already open |
| Panicking `From<MessageId>` impls | PR #4722, already open |
| `EventDataBatchOptions.max_size_in_bytes` ignored | PR #4808 |

Verification found three more defects, all filed and none owned yet.

| Item | Where |
|---|---|
| `receiver_settle_mode` and `target` ignored on attach | Issue #4809 |
| A hung AMQP open blocks recovery and every waiter | Issue #4810 |
| Concurrent attaches race on a duplicate `$cbs` link | Issue #4811 |
| `close()` fails after a failed CBS authorization | Issue #4812 |

One item is environmental rather than a code defect. Six of the 14 live
producer tests fail under Microsoft Entra ID with `Unauthorized access. 'Send'
claim(s) are required`, while the same sends pass with a connection string. The
test account holds Listen and Manage but not the Azure Event Hubs Data Sender
role. The Entra live suite cannot cover the send path until that role is
granted.

Still unowned: the missing `Clone` and `Debug` derives, the misleading attach
log, the CHANGELOG headers, and the README defects. `AddEventDataOptions` is
also unreachable from outside the crate, because `producer::batch` is
`pub(crate)` and `lib.rs` re-exports only `EventDataBatch` and
`EventDataBatchOptions`. That one belongs in PR #4722, because a reachable
`AddEventDataOptions` needs `#[non_exhaustive]` too.

## The pattern behind these defects

Four of the defects share one shape: a per-connection resource whose caching
does not match its AMQP cardinality, or a public option that never reaches the
wire.

- The epoch property is set and then discarded by the link builder.
- The management client is cached behind a lock held across its own build.
- The root connection is cached behind a lock held across an unbounded open.
- The CBS client is not cached at all, although AMQP permits one `$cbs` node
  per connection.

Each one is invisible to a unit test. Each needs a real broker, concurrency, or
both. The sender, session and receiver caches already use a lock-free `OnceCell`
per path, and the defects are in the resources that did not adopt that pattern.
That is where to look for the next one.

`receiver_settle_mode` and `target` (#4809) are the same class as the epoch
property: public options that the attach path never reads.

One correction to an earlier draft of this report: the two ignored tests that
first demonstrated the deadlock included one that locked the mutex by hand and
then called recovery. That is a tautology, since it would fail under any
non-reentrant lock, including a correct one. PR #4806 replaces both with tests
that drive production entry points only and stall the build against a loopback
peer.

## Sequence to 1.0

1. Fix the link-builder ordering in `azure_core_amqp`, on both the receiver and
   the sender path, and release it. Add a test that asserts the outgoing Attach
   frame carries `com.microsoft:epoch`. Event Hubs 1.0.0 cannot ship before
   this release exists.
2. Route displacement errors through `translate_receive_error` so a stolen link
   surfaces as `ConsumerDisconnected`, and stop flattening the described error
   at `src/common/recoverable/receiver.rs:105`. Then tighten
   `second_processor_displaces_first_with_consumer_disconnected` to assert
   `ConsumerDisconnected(Some(_))`.
3. Fix the `mgmt_client` self-deadlock. Folding the management client into the
   lock-free `OnceCell` pattern that the sender, session, and receiver caches
   already use removes the re-entrant lock rather than working around it. Keep
   `ensure_amqp_management_recovery_self_deadlocks` and remove its `#[ignore]`
   once it passes.
4. Add `#[non_exhaustive]` to the 16 types listed above.
5. Replace the four panicking `From<MessageId>` impls with `TryFrom`.
6. Honor `EventDataBatchOptions.max_size_in_bytes`, derive `Clone` on the
   options types, and correct the attach log message.
7. Land the #4454 generation fix, and decide on detach-during-recovery.
8. Correct the CHANGELOG headers and the README defects.
9. Run the offline gate and the live smoke test. Both must be green, which
   means P2 and P5 pass without any change to the harness assertions.
10. Bump to 1.0.0 and publish.

Items 1 through 5 cannot be deferred past the bump. Items 1 and 2 are one
feature between them: owner level does not work end to end until both are done.
Item 7 is a correctness risk that is easier to fix before the API is frozen.
The rest is hygiene.
