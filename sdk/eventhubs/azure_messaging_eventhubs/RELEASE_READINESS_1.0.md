<!-- cspell:ignore mgmt worktrees -->

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

Four items block the version bump. One is a real defect that hangs a client. The
other three are API and release hygiene that become permanent the moment 1.0
ships.

| # | Blocker | Class | Evidence |
|---|---|---|---|
| 1 | `mgmt_client` recovery self-deadlock | Defect | Reproduced by test |
| 2 | `#[non_exhaustive]` missing on 16 public types | API contract | Inspection |
| 3 | Four panicking `From<MessageId>` impls | API contract | Inspection |
| 4 | Release mechanics: stale CHANGELOG headers | Release | Inspection |

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

### Live capability matrix

Not yet run. Fill this section from
`cargo run --package azure_messaging_eventhubs --example eventhubs_smoke_test`
against a real namespace. The harness covers:

| Phase | Capability |
|---|---|
| P1 | Management: hub properties, partition properties for every partition |
| P2 | Produce: partition-addressed send, gateway-routed send, batch send, batch-full behavior |
| P3 | Consume: read from a captured tail, verify every event by marker, system properties populated, read from `Earliest` |
| P4 | Event processor: build, claim a partition, receive, checkpoint, confirm the checkpoint persisted |
| P5 | Epoch steal: a receiver displaced by a higher owner level reports `ConsumerDisconnected` |
| P6 | Idle recovery soak (opt-in) |
| P7 | Pre-formed SAS expiry (opt-in) |

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

## Sequence to 1.0

1. Fix the `mgmt_client` self-deadlock. Folding the management client into the
   lock-free `OnceCell` pattern that the sender, session, and receiver caches
   already use removes the re-entrant lock rather than working around it. Keep
   `ensure_amqp_management_recovery_self_deadlocks` and remove its `#[ignore]`
   once it passes.
2. Add `#[non_exhaustive]` to the 16 types listed above.
3. Replace the four panicking `From<MessageId>` impls with `TryFrom`.
4. Land the #4454 generation fix, and decide on detach-during-recovery.
5. Correct the CHANGELOG headers and the README defects.
6. Run the offline gate and the live smoke test. Both must be green.
7. Bump to 1.0.0 and publish.

Items 1 through 3 are the ones that cannot be deferred past the bump. Item 4 is
a correctness risk that is easier to fix before the API is frozen. Items 5 and 6
are hygiene.
