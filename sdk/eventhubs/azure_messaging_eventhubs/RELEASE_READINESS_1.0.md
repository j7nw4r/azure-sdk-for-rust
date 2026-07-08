# Event Hubs 1.0.0 Release-Readiness Report

Crate: `azure_messaging_eventhubs`, assessed at in-tree version `0.15.0`.

## Verdict

The crate is functionally solid and offline-clean, but **cannot be published as
`1.0.0` today** because of a single hard dependency blocker. Two API-contract
decisions and a handful of pre-1.0 fixes should be resolved before the version
is tagged. Details and evidence below.

## Verification tooling added

Three artifacts accompany this report so the assessment is reproducible:

1. `examples/eventhubs_smoke_test.rs` - a live end-to-end harness. It connects
   to a real namespace and exercises, printing PASS/FAIL per capability with a
   final tally and a non-zero exit on any failure:
   - producer connect, `get_eventhub_properties`, `get_partition_properties`;
   - send a batch of uniquely tagged events to one partition;
   - open a receiver at the captured tail sequence and verify every sent event
     round-trips by body match;
   - build an `EventProcessor` with an in-memory checkpoint store, claim a
     partition, deliver an event, `update_checkpoint`, and confirm it persisted.

   Supports both Microsoft Entra ID (`EVENTHUBS_HOST` + `EVENTHUB_NAME`) and
   connection-string (`EVENTHUBS_CONNECTION_STRING`) auth. Run with
   `cargo run --example eventhubs_smoke_test`.

2. `verify-offline.sh` - the always-green offline gate: rustfmt check, build
   (`--all-features`), clippy (`-D warnings`), the offline test subset, doc
   build (`-D warnings`), and an optional `cargo-semver-checks` pass.

3. This report.

## Current offline health: green

`./verify-offline.sh` passes every step. The offline test subset is **160
tests passing** (109 library unit tests, 45 doc tests, 6 integration tests);
the 49 live integration tests correctly self-skip without credentials.

`cargo-semver-checks` against the published `0.13.0` baseline (forced minor
check) reports **195 checks pass, 0 API-shape breaking changes**. The one
documented behavioral break (stolen-link no longer auto-retried) and the new
`ConsumerDisconnected` variant are absorbed without an API break because
`ErrorKind` is `#[non_exhaustive]`.

## 1.0.0 assessment

### BLOCKER: beta core dependency by path

`Cargo.toml:25`:

```
azure_core_amqp = { path = "../../core/azure_core_amqp", version = "1.1.0-beta.1" }
```

The crate has a non-optional runtime dependency on `azure_core_amqp
1.1.0-beta.1`, which is unpublished. The max published version is `1.0.0`; the
required API (`AmqpSessionOptions::with_unbounded_windows`, per the comment at
`Cargo.toml:24` and issue #4570) exists only in the unreleased beta. A crate
cannot be published to crates.io as `1.0.0` while depending on an unpublished
version by path.

Resolution: `azure_core_amqp` must ship a stable release containing
`with_unbounded_windows`, and this line must become a versioned (non-path)
dependency on that stable version. Nothing else in this report unblocks a
publish until this is done.

### DECIDE: beta-crate types in the public API

The 1.0 public surface exposes `azure_core_amqp` types directly:

- `models` re-exports `AmqpMessage`, `AmqpValue`, `AmqpSimpleValue`
  (`src/models/mod.rs:8,11,18`).
- `ReceivedEventData::raw_amqp_message() -> &AmqpMessage`
  (`src/models/event_data.rs:186`), `system_properties() -> &HashMap<String,
  AmqpValue>` (`:274`), `EventData::properties() -> Option<&HashMap<String,
  AmqpSimpleValue>>` (`:55`), `add_property(value: impl Into<AmqpSimpleValue>)`
  (`:401`).
- `EventDataBatch::try_add_amqp_message(message: impl Into<AmqpMessage>, ...)`
  (`src/producer/batch.rs:215`).
- `ErrorKind::SendRejected(Option<AmqpDescribedError>)`,
  `AmqpError(AmqpError)`, `ConsumerDisconnected(Option<AmqpDescribedError>)`
  (`src/error.rs:21,29,36`).

This is intended (the AMQP types are the low-level escape hatch), not a bug, but
it is a 1.0 contract decision: tagging 1.0 locks the public API to
`azure_core_amqp`'s types, so a future breaking `azure_core_amqp 2.x` would force
a breaking `azure_messaging_eventhubs` release. Confirm this coupling is
acceptable and that `azure_core_amqp` will be 1.x-stable at publish. (The
`force_error(AmqpError)` methods on the producer and consumer are `#[cfg(test)]`
and are not in the released API.)

### DECIDE: `#[non_exhaustive]` on options and growable enums

Only `ErrorKind` (`src/error.rs:12`) and `ConnectionString`
(`src/common/connection_string.rs:39`) carry `#[non_exhaustive]`. The options
structs and enums most likely to gain fields or variants do not, and several
have public fields, so any addition after 1.0 is a breaking change as written:

- `OpenReceiverOptions` (`src/consumer/mod.rs:402`)
- `StartPosition` (`src/consumer/mod.rs:501`), `StartLocation`
  (`src/consumer/mod.rs:421`)
- `SendEventOptions` (`src/producer/mod.rs:72`), `SendMessageOptions`
  (`src/producer/mod.rs:81`), `SendBatchOptions` (`src/producer/mod.rs:32`)
- `EventDataBatchOptions` (`src/producer/batch.rs:345`), `AddEventDataOptions`
  (`src/producer/batch.rs:12`)
- `StartPositions` (`src/event_processor/models.rs:145`), `ProcessorStrategy`
  (`src/event_processor/mod.rs:96`)

Decide `#[non_exhaustive]` per type before 1.0. Adding a field to
`OpenReceiverOptions` or a variant to `StartLocation`/`ProcessorStrategy` after
1.0 would otherwise require a major bump.

### FIX: panicking `From<MessageId>` conversions

`src/models/mod.rs:189,198,207,216` implement `From<MessageId>` for `Uuid`,
`Vec<u8>`, `String`, and `u64`, each of which `panic!`s when the `MessageId`
holds a different variant. `MessageId` is public (`src/models/mod.rs:141`), so
these are reachable from user code. Infallible `From` that can panic is a 1.0
footgun; convert to `TryFrom`.

### FIX: CHANGELOG and version hygiene

`CHANGELOG.md` marks both `0.14.0` and `0.15.0` as `(Unreleased)`, but both are
already published (0.15.0 is the current published version, corroborated by the
semver baseline resolving to a real `0.15.0` on crates.io). The in-tree
`Cargo.toml` version (`0.15.0`) equals the latest published version, so the
changes described under the 0.15.0 heading cannot ship without a version bump.
Correct the stale headers with real dates and stage the next version. There is
also no documented 1.0 plan or stability statement anywhere in the crate.

### FIX: README accuracy

`README.md` documents `ProducerClient` and `ConsumerClient` but has real gaps:

- Connection-string auth (the headline 0.15.0 feature) is undocumented.
- The `EventProcessor` has no Rust example and its only reference
  (`README.md:104`) links to the C++ SDK storage docs.
- The license link (`README.md:277`) points at `azure-sdk-for-cpp`.
- A malformed link (`README.md:108`, double `]]` with a stray space) renders
  broken; minor typos at `:27` ("crating") and `:53` ("Even Hubs").

Item-level rustdoc is complete: `#![warn(missing_docs)]` is set and the doc
build passes with `-D warnings`. The gap is README-level only.

### CLEAN

- No `TODO`/`FIXME`/`HACK`/`XXX` markers in `src/`.
- The recovery/reconnection runtime path (`src/common/recoverable/connection.rs`,
  `src/event_processor/`) has no reachable library panics; all unwraps and
  expects there are inside `#[cfg(test)]`. The `unimplemented!` stubs in
  `recoverable/{sender,receiver,management,claims_based_security}.rs` are on
  `pub(crate)` types and are unreachable from outside the crate.

## Suggested sequence to 1.0.0

1. Ship `azure_core_amqp` stable with `with_unbounded_windows`; switch the
   dependency to a versioned stable requirement (clears the blocker).
2. Decide and apply `#[non_exhaustive]` across the options/enums above.
3. Confirm the `azure_core_amqp`-typed public API is the intended 1.0 contract.
4. Convert the `From<MessageId>` impls to `TryFrom`.
5. Fix the CHANGELOG headers, stage the version bump, and fix the README.
6. Run `./verify-offline.sh` and the live `eventhubs_smoke_test` against a real
   namespace as the final gate.
