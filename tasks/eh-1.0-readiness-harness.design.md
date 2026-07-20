<!-- cspell:ignore mgmt upstreamable upstreaming upstreamed RUSTDOCFLAGS -->

# Event Hubs 1.0 readiness harness design

## Understanding

Johnathan must decide whether `azure_messaging_eventhubs` (in-tree `0.15.0`) can become `1.0.0`. The task is to design a verification harness that turns that decision into evidence. The harness has two halves. An offline gate runs with no credentials and proves build, lint, doc, packaging, and SemVer health. A live smoke and failure-injection binary runs against his real namespace (connection string in an env var) and proves the functional and failure-mode behavior a GA client must have. The third deliverable is a rewritten readiness report that the harness output feeds, ending in a go / no-go verdict with named blockers.

Done looks like: a fork branch containing (1) `verify-offline.sh` that passes every step from a normal checkout, (2) `examples/eventhubs_smoke_test.rs` that prints one PASS/FAIL/SKIP line per capability with a tally and non-zero exit on failure, self-skipping cleanly when no credentials are set, (3) `RELEASE_READINESS_1.0.md` re-verified against current main with every claim carrying a `path:line` or a harness-run citation. Acceptance: Johnathan reads the report, sees which claims are machine-verified versus inspected versus open engineering work, and can name the exact blockers.

Out of scope: fixing the defects the harness finds (the mgmt deadlock, `#[non_exhaustive]` gaps, README repairs are separate PRs), testing `azure_messaging_eventhubs_checkpointstore_blob` (needs a storage account, and it versions independently at 0.9.0), load or throughput benchmarking (the crate has `benches/` already), and the actual 1.0 version-bump mechanics.

## Findings

The landscape moved significantly since the prior harness branch. Most importantly, **the hard publish blocker is gone**:

- Commit `b7e1b8671` ("Align dependency policy", #4801) switched workspace deps to registry versions. Root `Cargo.toml:66-67` declares `azure_core_amqp` at `version = "1.1.0"`, no path. `Cargo.lock` shows `azure_messaging_eventhubs 0.15.0` depending on `azure_core_amqp 1.1.0` from `registry+https://github.com/rust-lang/crates.io-index` and `azure_core 1.1.0`. Both are published GA crates (confirmed via `cargo search`). The in-tree `sdk/core/azure_core_amqp` at `1.2.0-beta.1` is no longer what eventhubs builds against. AMQP types in the public API (`AmqpMessage`, `AmqpDescribedError` in `ErrorKind`, `From<MessageId> for AmqpMessageId` at `sdk/eventhubs/azure_messaging_eventhubs/src/models/mod.rs:232`) now come from a stable 1.x crate; the coupling is a normal SemVer decision, not a blocker.
- Connection-string / SAS auth is on main: `sdk/eventhubs/azure_messaging_eventhubs/src/producer/mod.rs:695` and `src/consumer/mod.rs:783` (`open_with_connection_string`), public `ConnectionString` at `src/common/connection_string.rs:40`. WebSocket transport is **not** on main (no `TransportType` hits in `src/`), so PR #4596 remains unmerged and is not a 1.0 surface question.
- Epoch/steal semantics are new on main: `OpenReceiverOptions.owner_level` (`src/consumer/mod.rs:433`, wired to `com.microsoft:epoch` at `:298`), the processor opens at epoch 0 (`src/event_processor/processor.rs:447`), and broker displacement surfaces as `ErrorKind::ConsumerDisconnected` (CHANGELOG 0.15.0 entry). This is directly testable live.

Defect re-verification against current main:

- **Mgmt-mutex self-deadlock: still structurally present.** `ensure_amqp_management` holds `mgmt_client.lock()` across the whole client build (`src/common/recoverable/connection.rs:576-584`). The build path calls `authorizer.authorize_path` (`src/common/recoverable/management.rs:66-75`), which performs CBS authorization through `RecoverableClaimsBasedSecurity`, whose retry loop passes a recovery hook (`src/common/recoverable/claims_based_security.rs:137`). A connection-level CBS failure there runs `apply_recovery_plan`, whose `drop_mgmt_client` arm takes `self.mgmt_client.lock().await` (`connection.rs:831-833`) on the same task that already holds it. `AsyncMutex` is non-reentrant; this is a permanent hang, not an error.
- **#4454 stale-cache window: still open and acknowledged in a code comment** at `src/common/recoverable/connection.rs:806-812` ("Closing that window ... is tracked in #4454").
- **Recovery never detaches links: still true.** The recoverable wrappers' `detach()` are `unimplemented!()` stubs (`src/common/recoverable/receiver.rs:62`, `sender.rs:157`, `claims_based_security.rs:150`); `apply_recovery_plan` clears caches without detaching; only `close_connection` detaches, and only when it holds the last `Arc` reference (`connection.rs:265-299`).
- **Token-refresh task: largely fixed.** The loop now exits cleanly when the connection drops, applies a floor delay after failed refreshes (`src/common/authorizer.rs:27-29`), and excludes non-renewable pre-formed SAS tokens (`authorizer.rs:255+`). The authorizer no longer holds its scope lock across CBS I/O (`authorizer.rs:131-139`). The old "spins forever" claim no longer holds; verify by inspection, not harness.
- API hygiene gaps persist: `#[non_exhaustive]` only on `ErrorKind` (`src/error.rs:12`) and `ConnectionString` (`src/common/connection_string.rs:39`); the ten-plus options structs and growable enums (`src/producer/mod.rs:32,72,81`, `src/producer/batch.rs:12,358`, `src/consumer/mod.rs:431,450,530`, `src/event_processor/models.rs:145`, `src/event_processor/mod.rs:96`) lack it. Four panicking `From<MessageId>` impls remain (`src/models/mod.rs:185-218`).
- Release mechanics: `CHANGELOG.md:3` says `0.15.0 (Unreleased)` while crates.io already has 0.15.0, and `:25` has a stale `0.14.0 (Unreleased)` header. README defects persist ("Even Hubs" and "crating" at `README.md:53`, C++ processor link at `:104`, malformed link at `:108`, C++ license link at `:305`, no connection-string section).
- Test infrastructure: nearly all integration tests are `#[recorded::test(live)]` (worktree-hostile per the CARGO_MANIFEST_DIR issue; they self-skip without live credentials). The `force_error` fault hooks are `#[cfg(test)]` only (`src/producer/mod.rs:505-507`), so an `examples/` binary cannot use them. Env conventions in-tree: `EVENTHUBS_HOST`, `EVENTHUB_NAME`, `EVENTHUBS_CONNECTION_STRING`.
- Prior art (fork commit `1a51115`, branch `worktree-eh-test-harness`): the three files are sound in shape. The smoke test's API usage (`ConnectionString` parse, `open_with_connection_string`, `EventProcessor` + `InMemoryCheckpointStore`) still exists on main, so it is a port, not a rewrite. Its readiness report is now wrong in its central claim (the beta-dep blocker) and must be rewritten.
- Parity note: no buffered producer exists (`src/` has only `producer`, `consumer`, `event_processor`, `models`, `common`). .NET has `EventHubBufferedProducerClient`; Go and Python GA'd without one, so this is a documented gap, not a blocker.

## Approach

Three artifacts on a fresh fork branch based on current main, superseding `worktree-eh-test-harness`:

1. **`verify-offline.sh`** (crate root, fork-local tool). Port the prior script and extend it: add a cspell step using `./.vscode/cspell.json` (the Build Analyze gate runs it; the prior script did not), keep fmt / build all-targets / clippy `-D warnings` / offline test subset / doc `-D warnings` / optional `cargo-semver-checks`, and add an optional `cargo publish --dry-run` step (this is the machine check that the dependency-policy fix actually made the crate publishable). Teach the script to detect a linked worktree (`git rev-parse --git-dir` vs `--git-common-dir`) and mark the `cargo test` step SKIP with a reason there, because the `#[recorded::test]` machinery cannot run from a worktree.

2. **`examples/eventhubs_smoke_test.rs`** (potentially upstreamable). Port the prior four phases and add failure-injection phases. Every phase runs under a hard `tokio::time::timeout` (default 120s) so a hang, the failure mode Johnathan fears most, prints `FAIL (timeout)` instead of wedging the run. Phases:
   - P1 management: `get_eventhub_properties`, `get_partition_properties` for every partition.
   - P2 produce: single tagged event via `send_event` (to a partition and gateway-routed), tagged batch via `EventDataBatch`, batch-full behavior (`try_add` returns false at size limit).
   - P3 consume: open receiver at captured tail sequence, verify every tagged event round-trips by marker, assert system properties (sequence number, offset, enqueued time) are populated, exercise `StartPosition::SequenceNumber` and `Earliest`.
   - P4 processor: `EventProcessor` + `InMemoryCheckpointStore`, claim a partition, deliver an event, `update_checkpoint`, confirm persistence.
   - P5 epoch steal (new): receiver A on partition 0 with `owner_level: Some(0)` streaming; open receiver B with `owner_level: Some(1)` on the same partition and consumer group; assert A's stream yields `ErrorKind::ConsumerDisconnected` within the timeout and B receives. This machine-verifies the headline 0.15.0 behavior change.
   - P6 idle-recovery soak (opt-in via `SMOKE_SOAK_SECS`, skipped at 0/unset): open producer, send, sit idle for the configured duration (long enough to cross the service's idle teardown), send again; PASS means transparent recovery, FAIL(timeout) is direct evidence of a recovery hang.
   - P7 pre-formed SAS expiry (opt-in via `SMOKE_SAS_EXPIRY_SECS`, connection-string mode only): the harness signs a short-lived SAS itself (add `hmac`, `sha2`, `base64` to dev-deps; they are already workspace deps), opens successfully before expiry, then after expiry asserts a new open fails with a clean auth error within the timeout rather than hanging.
   - Config: prefers `EVENTHUBS_CONNECTION_STRING` (namespace-level; `EVENTHUB_NAME` supplies the hub), falls back to `EVENTHUBS_HOST` + Entra. With neither set it prints one SKIP line and exits 0.

3. **`RELEASE_READINESS_1.0.md`** (fork-local). Full rewrite. Verdict up front, then four evidence classes kept honestly separate: machine-verified by harness (capability matrix, P1-P7, offline gate results), verified by inspection (AMQP GA-type coupling decision, token-refresher fixes), lint-adjacent contract items (missing `#[non_exhaustive]`, panicking `From`, CHANGELOG, README), and open engineering work no test can wave through (mgmt-mutex deadlock with the exact call chain, #4454 window, detach-on-recovery leaks, buffered-producer parity note). Ends with an ordered sequence to 1.0.

Optionally, a fourth artifact: an `#[ignore]`-marked in-crate unit test that reproduces the mgmt deadlock deterministically using the existing `#[cfg(test)]` `force_error` / `disable_authorization` hooks under a timeout. This is the strongest possible evidence form for the scariest defect. Flagged as a judgement call because it touches `src/` and lands an intentionally red (ignored) test.

Rejected alternatives: a `tests/` integration file instead of an example (worse output ergonomics, tangled with the recorded-test machinery, and the example runs fine from this worktree); exposing `force_error` behind a public `test` feature for live fault injection (API pollution for a pre-1.0 crate); `cargo public-api` snapshots (extra tooling; `cargo-semver-checks` covers the need); rewriting the smoke test from scratch (the prior one is sound; port it).

Judgement calls Johnathan may want to overturn: (a) harness artifacts live in the crate directory, which `cargo publish` would package; acceptable on a fork, but upstreaming the script or report requires a `package.exclude` entry or moving them out of the crate dir; (b) P5's steal check asserts the strict new contract (error surfaces, no silent re-attach), which hard-fails on any regression toward the old retry behavior; (c) the report stays fork-only rather than upstreamed as an issue set.

## Risks

- P5 and P6 depend on live broker timing. Displacement and idle teardown latencies vary; generous timeouts and one retry per phase mitigate, but occasional flakes are possible. These phases are evidence generators, not CI gates, so a flake costs a re-run, not a red pipeline.
- The service idle-teardown threshold is not pinned here by citation. P6 takes its duration from an env var instead of asserting a number; if the chosen duration never triggers teardown, the phase proves less than intended (it still proves a long-idle client works).
- `cargo-semver-checks` baselines can break when a fresh crates.io release changes the rebuilt baseline (known failure mode, recorded in memory). The script treats that step as advisory, not fatal.
- The deadlock repro depends on the `#[cfg(test)]` hooks being able to inject a CBS-path failure at the right moment; if the hook granularity is too coarse, the repro would need a new test-only seam, which grows the optional step's scope. If it proves too invasive, the report falls back to the call-chain citation above.
- API drift: main moves fast (89+ commits since the prior branch). The port step budgets for compile fixes; anything larger means re-checking this design's findings.
- `cargo publish --dry-run` may fail for reasons unrelated to readiness (workspace metadata quirks); keep it advisory on first run.

## Steps

### Step 1: Create the harness branch and port verify-offline.sh
- **Files**: `sdk/eventhubs/azure_messaging_eventhubs/verify-offline.sh` (new, from `git show 1a51115:sdk/eventhubs/azure_messaging_eventhubs/verify-offline.sh`).
- **Change**: Branch `eh-1.0-readiness-harness` off current upstream main. Port the script; add a cspell step (`npx cspell lint --config .vscode/cspell.json "sdk/eventhubs/azure_messaging_eventhubs/**"`, SKIP with a note when `npx`/cspell is unavailable); add an advisory `cargo publish --dry-run --package azure_messaging_eventhubs` step; make the `cargo test` step detect a linked worktree (`[[ "$(git rev-parse --git-dir)" != "$(git rev-parse --git-common-dir)" ]]`) and SKIP with the recorded-test explanation. Keep the existing `run_step` capture pattern (never read `$?` after a pipe). Prose in the script follows repo style: no em dashes, plain comments.
- **Depends on**: none.
- **Parallel-safe**: yes.
- **Done when**: `bash -n` passes and a run from this worktree shows fmt/build/clippy/doc/cspell PASS with the test step SKIPped for the worktree reason.

### Step 2: Port the smoke-test example (phases P1-P4)
- **Files**: `sdk/eventhubs/azure_messaging_eventhubs/examples/eventhubs_smoke_test.rs` (new, from the fork branch), `sdk/eventhubs/azure_messaging_eventhubs/Cargo.toml` (dev-deps only if the compiler demands).
- **Change**: Port the 599-line example onto current main and fix compile drift. Wrap every phase in `tokio::time::timeout` (default 120s, overridable via `SMOKE_PHASE_TIMEOUT_SECS`) reporting `FAIL (timeout after Ns)`. Extend P2 with the batch-full `try_add` check and P3 with system-property assertions and a `StartPosition::Earliest` read. Keep the PASS/FAIL/SKIP per-phase output, final tally, exit-code contract, and the no-credentials SKIP-and-exit-0 path. Keep the `cspell: words` header current.
- **Depends on**: 1.
- **Parallel-safe**: no.
- **Done when**: `cargo build --package azure_messaging_eventhubs --example eventhubs_smoke_test` succeeds and running it with no env vars prints the SKIP message and exits 0.

### Step 3: Add failure-injection phases P5-P7
- **Files**: `sdk/eventhubs/azure_messaging_eventhubs/examples/eventhubs_smoke_test.rs`, `sdk/eventhubs/azure_messaging_eventhubs/Cargo.toml` (add `hmac`, `sha2`, `base64` to `[dev-dependencies]`, all already workspace deps).
- **Change**: P5 epoch steal: stream from receiver A (`owner_level: Some(0)`) on partition 0, then open receiver B (`owner_level: Some(1)`) same partition and group; assert A yields `ErrorKind::ConsumerDisconnected` (pattern-match the error kind, not the message) and B receives a freshly sent tagged event. P6 idle soak: gated on `SMOKE_SOAK_SECS > 0`; send, idle that long, send again on the same client; any error or hang is FAIL. P7 SAS expiry: gated on `SMOKE_SAS_EXPIRY_SECS > 0` and connection-string mode; sign a SAS with that lifetime from the parsed key (sr = namespace URL, HMAC-SHA256, standard `SharedAccessSignature sr=...&sig=...&se=...&skn=...` form), open via a `SharedAccessSignature=` connection string, confirm an operation succeeds pre-expiry, sleep past `se`, assert a fresh open fails with an auth-kind error within the phase timeout.
- **Depends on**: 2.
- **Parallel-safe**: no.
- **Done when**: the example builds; a no-credential run still SKIPs cleanly; P6/P7 print `SKIP (not enabled)` when their env vars are unset.

### Step 4 (optional, judgement call): In-crate deadlock repro test
- **Files**: `sdk/eventhubs/azure_messaging_eventhubs/src/common/recoverable/connection.rs` (tests module only).
- **Change**: Add an `#[ignore = "reproduces mgmt-mutex self-deadlock; see RELEASE_READINESS_1.0.md"]` plain `#[tokio::test]` that arms a forced connection-level error on the CBS path (via the existing `force_error` / `disable_authorization` seams), calls `ensure_amqp_management`, and asserts completion within 10s via `tokio::time::timeout`; the assertion message states that a timeout means the same-task re-lock at `connection.rs:831` fired. If the existing seams cannot fault the CBS leg specifically, stop and record that limitation in the report instead of adding new seams.
- **Depends on**: 1.
- **Parallel-safe**: no.
- **Done when**: `cargo test --package azure_messaging_eventhubs --lib -- --ignored <test_name>` from a non-worktree checkout demonstrates the timeout (red under `--ignored` is the expected evidence; the default test run stays green).

### Step 5: Rewrite the readiness report
- **Files**: `sdk/eventhubs/azure_messaging_eventhubs/RELEASE_READINESS_1.0.md` (full rewrite).
- **Change**: Structure: Verdict; Evidence from the harness (offline gate tally, live phase matrix, placeholders to fill in Steps 6-7); API contract items (missing `#[non_exhaustive]` list with the exact `path:line` set from Findings, panicking `From<MessageId>` impls, GA-typed AMQP surface as an accepted-coupling decision); Open engineering work (mgmt deadlock with the call chain `connection.rs:576 -> management.rs:66 -> claims_based_security.rs:137 -> connection.rs:831`, #4454 with the in-code comment cite, detach-on-recovery leaks, buffered-producer parity note); Release mechanics (CHANGELOG stale headers, README defect list, version-bump plan); Sequence to 1.0. State explicitly that the former beta-dependency blocker is resolved by #4801 and cite `Cargo.toml:66` plus the lockfile. Follow Simplified Technical English; no em dashes.
- **Depends on**: 1, 2, 3 (and 4's outcome if taken).
- **Parallel-safe**: no.
- **Done when**: every claim in the report carries a `path:line` cite, a harness phase name, or an explicit "inspection" label, and cspell passes on the file.

### Step 6: Run the offline gate and record results
- **Files**: `sdk/eventhubs/azure_messaging_eventhubs/RELEASE_READINESS_1.0.md` (fill in numbers).
- **Change**: Run `./verify-offline.sh` from a non-worktree checkout of the branch (a plain clone, or the main checkout with the branch checked out) so the test step executes. Record test counts, semver-checks result, and publish dry-run outcome in the report. Any FAIL becomes a named blocker line, not a footnote.
- **Depends on**: 1, 5.
- **Parallel-safe**: no.
- **Done when**: the report's offline-evidence section shows real numbers from a completed run.

### Step 7: Live run and final verdict
- **Files**: `sdk/eventhubs/azure_messaging_eventhubs/RELEASE_READINESS_1.0.md` (verdict and live matrix).
- **Change**: Ask Johnathan for the connection string. Run `EVENTHUBS_CONNECTION_STRING=... EVENTHUB_NAME=... cargo run --package azure_messaging_eventhubs --example eventhubs_smoke_test` (worktree-safe), then a second run with `SMOKE_SOAK_SECS=660 SMOKE_SAS_EXPIRY_SECS=120` for P6/P7. Paste the phase matrix into the report, write the final go / no-go verdict, and push the branch as a draft PR on the fork.
- **Depends on**: 3, 5, 6.
- **Parallel-safe**: no.
- **Done when**: both live runs exit 0 (or every non-zero exit maps to a named blocker in the report) and the draft PR exists.

## Validation

From the branch, non-worktree checkout for the full suite:

- `./sdk/eventhubs/azure_messaging_eventhubs/verify-offline.sh` : every step PASS (semver-checks and publish dry-run advisory).
- Build Analyze equivalents individually: `cargo fmt --package azure_messaging_eventhubs -- --check`; `cargo clippy --package azure_messaging_eventhubs --all-targets --all-features -- -D warnings`; `RUSTDOCFLAGS="-D warnings" cargo doc --package azure_messaging_eventhubs --no-deps --all-features`; `npx cspell lint --config .vscode/cspell.json "sdk/eventhubs/azure_messaging_eventhubs/**"` : all clean.
- `cargo test --package azure_messaging_eventhubs --all-features` : offline subset green, live tests self-skip (must run outside a worktree).
- No-credential smoke run: `cargo run -p azure_messaging_eventhubs --example eventhubs_smoke_test` prints SKIP, exits 0.
- Live: the two runs in Step 7, exit 0, all phases PASS (P6/P7 PASS when enabled).

## Docs to update

- `sdk/eventhubs/azure_messaging_eventhubs/RELEASE_READINESS_1.0.md` is itself the primary doc deliverable (Steps 5-7).
- If the smoke example is later upstreamed to Azure/azure-sdk-for-rust, add a CHANGELOG "Other Changes" entry and decide `package.exclude` (or relocation) for the script and report in that PR; not needed while fork-only.
- After implementation, refresh the user memory file `eventhubs-1.0-readiness-harness.md` (its "1.0 blocked on azure_core_amqp beta dep" claim is now stale; #4801 resolved it).
- No repo README or skill changes.
