// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// cspell: words eventhub eventhubs checkpointing checkpointed skn

//! End-to-end smoke test harness for the Event Hubs client.
//!
//! This binary drives the public surface of the crate against a real Event Hubs
//! namespace and prints a PASS, FAIL, or SKIP line per capability, followed by a
//! tally. It answers one question: does this crate do the things a 1.0 client
//! must do, against the real service?
//!
//! Every phase runs under a hard timeout. A hang therefore reports
//! `FAIL (timeout)` instead of stopping the run. This matters, because the
//! failure mode this harness looks hardest for is a deadlock in the recovery
//! path, and a harness that hangs on a deadlock reports nothing.
//!
//! The process exits `0` when no phase failed, so a script can gate on it. With
//! no credentials in the environment it prints one SKIP line and exits `0`.
//!
//! # Configuration (environment variables)
//!
//! Shared Access Signature via connection string:
//! ```text
//! EVENTHUBS_CONNECTION_STRING=Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...
//! EVENTHUB_NAME=<event hub name>   # optional if the connection string has EntityPath
//! ```
//!
//! or Microsoft Entra ID (uses your developer credentials):
//! ```text
//! EVENTHUBS_HOST=<namespace>.servicebus.windows.net
//! EVENTHUB_NAME=<event hub name>
//! ```
//!
//! Optional:
//! ```text
//! EVENTHUB_CONSUMER_GROUP=$Default   # consumer group for the processor phase
//! SMOKE_EVENT_COUNT=5                # events for the round-trip phase
//! SMOKE_PHASE_TIMEOUT_SECS=120       # per-phase hard timeout
//! SMOKE_SOAK_SECS=0                  # idle-recovery soak; 0 disables the phase
//! SMOKE_SAS_EXPIRY_SECS=0            # SAS expiry probe; 0 disables the phase
//! ```
//!
//! Run with:
//! ```text
//! cargo run --example eventhubs_smoke_test
//! ```
//! (The crate's dev-dependencies enable the `in_memory_checkpoint_store`
//! feature that the processor phase needs, so no extra flags are required.)

use azure_core::credentials::Secret;
use azure_core::time::Duration;
use azure_identity::DeveloperToolsCredential;
use azure_messaging_eventhubs::{
    error::ErrorKind, models::EventData, CheckpointStore, ConnectionString, ConsumerClient,
    EventDataBatchOptions, EventProcessor, InMemoryCheckpointStore, OpenReceiverOptions,
    ProducerClient, SendEventOptions, StartLocation, StartPosition,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use futures::StreamExt;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use sha2::Sha256;
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use std::{env, error::Error, process::ExitCode};

/// Application id stamped on every AMQP connection this harness opens, so the
/// traffic is easy to spot in broker-side diagnostics.
const APP_ID: &str = "eventhubs_smoke_test";

/// How long any single receive is allowed to block before the step is failed.
const RECEIVE_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// How long a receiver gets to attach. Receivers attach lazily, on the first
/// poll of `stream_events()`. A phase that must have a live link before it
/// sends anything polls once with this budget, then continues. The poll is
/// expected to time out, because the link starts at the tail of the partition.
const ATTACH_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Default per-phase hard timeout. Overridden by `SMOKE_PHASE_TIMEOUT_SECS`.
const DEFAULT_PHASE_TIMEOUT_SECS: u64 = 120;

/// How long the broker gets to displace a receiver and propagate the AMQP
/// detach. This matches the window the in-tree live test
/// `second_processor_displaces_first_with_consumer_disconnected` allows. A
/// shorter window reports a slow broker as an SDK defect.
const STEAL_TIMEOUT: StdDuration = StdDuration::from_secs(90);

type BoxError = Box<dyn Error + Send + Sync>;

/// How the harness authenticates. Both paths land on the same public API. Only
/// the builder method that opens the client differs.
enum Mode {
    Entra {
        host: String,
        eventhub: String,
        credential: Arc<DeveloperToolsCredential>,
    },
    ConnectionString {
        connection_string: String,
        eventhub: Option<String>,
    },
}

struct Config {
    mode: Mode,
    /// Fully qualified namespace, used to look up checkpoints in the store.
    namespace: String,
    consumer_group: String,
    event_count: usize,
    phase_timeout: StdDuration,
    /// Idle duration for the recovery soak phase. Zero disables the phase.
    soak: StdDuration,
    /// Lifetime of the short-lived SAS token. Zero disables the phase.
    sas_expiry: StdDuration,
}

/// Why the harness cannot run. Missing credentials is a SKIP, not an error.
enum ConfigOutcome {
    Ready(Box<Config>),
    NoCredentials,
    Invalid(BoxError),
}

fn env_secs(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

impl Config {
    /// Builds the run configuration from the environment. Prefers a connection
    /// string when present, otherwise falls back to host plus Entra credentials.
    fn from_env() -> ConfigOutcome {
        let consumer_group =
            env::var("EVENTHUB_CONSUMER_GROUP").unwrap_or_else(|_| "$Default".to_string());
        let event_count = env::var("SMOKE_EVENT_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(5);
        let phase_timeout = StdDuration::from_secs(env_secs(
            "SMOKE_PHASE_TIMEOUT_SECS",
            DEFAULT_PHASE_TIMEOUT_SECS,
        ));
        let soak = StdDuration::from_secs(env_secs("SMOKE_SOAK_SECS", 0));
        let sas_expiry = StdDuration::from_secs(env_secs("SMOKE_SAS_EXPIRY_SECS", 0));

        if let Ok(connection_string) = env::var("EVENTHUBS_CONNECTION_STRING") {
            let parsed: ConnectionString = match connection_string.parse() {
                Ok(p) => p,
                Err(e) => return ConfigOutcome::Invalid(Box::new(e)),
            };
            let eventhub = env::var("EVENTHUB_NAME")
                .ok()
                .or_else(|| parsed.entity_path.clone());
            return ConfigOutcome::Ready(Box::new(Self {
                namespace: parsed.fully_qualified_namespace.clone(),
                mode: Mode::ConnectionString {
                    connection_string,
                    eventhub,
                },
                consumer_group,
                event_count,
                phase_timeout,
                soak,
                sas_expiry,
            }));
        }

        let Ok(host) = env::var("EVENTHUBS_HOST") else {
            return ConfigOutcome::NoCredentials;
        };
        let eventhub = match env::var("EVENTHUB_NAME") {
            Ok(name) => name,
            Err(_) => {
                return ConfigOutcome::Invalid(
                    "EVENTHUB_NAME is required with EVENTHUBS_HOST".into(),
                )
            }
        };
        let credential = match DeveloperToolsCredential::new(None) {
            Ok(c) => c,
            Err(e) => return ConfigOutcome::Invalid(Box::new(e)),
        };
        ConfigOutcome::Ready(Box::new(Self {
            namespace: host.clone(),
            mode: Mode::Entra {
                host,
                eventhub,
                credential,
            },
            consumer_group,
            event_count,
            phase_timeout,
            soak,
            sas_expiry,
        }))
    }

    /// The parsed connection string, when the harness runs in that mode. The
    /// SAS expiry phase needs the signing key, so it is unavailable under Entra.
    fn parsed_connection_string(&self) -> Option<ConnectionString> {
        match &self.mode {
            Mode::ConnectionString {
                connection_string, ..
            } => connection_string.parse().ok(),
            Mode::Entra { .. } => None,
        }
    }
}

/// The result of one check.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Pass,
    Fail,
    Skip,
}

impl Outcome {
    fn tag(self) -> &'static str {
        match self {
            Outcome::Pass => " OK ",
            Outcome::Fail => "FAIL",
            Outcome::Skip => "SKIP",
        }
    }
}

/// Accumulates per-check results and renders the final tally.
#[derive(Default)]
struct Report {
    results: Vec<(String, Outcome)>,
}

impl Report {
    fn record(&mut self, name: &str, outcome: Outcome, detail: impl AsRef<str>) {
        let detail = detail.as_ref();
        if detail.is_empty() {
            println!("[{}] {name}", outcome.tag());
        } else {
            println!("[{}] {name}  --  {detail}", outcome.tag());
        }
        self.results.push((name.to_string(), outcome));
    }

    fn ok(&mut self, name: &str, detail: impl AsRef<str>) {
        self.record(name, Outcome::Pass, detail);
    }

    fn fail(&mut self, name: &str, detail: impl AsRef<str>) {
        self.record(name, Outcome::Fail, detail);
    }

    fn skip(&mut self, name: &str, detail: impl AsRef<str>) {
        self.record(name, Outcome::Skip, detail);
    }

    fn failures(&self) -> Vec<&str> {
        self.results
            .iter()
            .filter(|(_, o)| *o == Outcome::Fail)
            .map(|(n, _)| n.as_str())
            .collect()
    }

    fn summary(&self) {
        let passed = self
            .results
            .iter()
            .filter(|(_, o)| *o == Outcome::Pass)
            .count();
        let skipped = self
            .results
            .iter()
            .filter(|(_, o)| *o == Outcome::Skip)
            .count();
        let failed = self.failures().len();
        println!("\n----------------------------------------");
        println!("SMOKE TEST: {passed} passed, {failed} failed, {skipped} skipped");
        if failed > 0 {
            println!("FAILED: {}", self.failures().join(", "));
        }
    }
}

/// Runs one phase under a hard timeout. A phase that hangs is reported as a
/// timeout failure rather than being allowed to stop the run.
///
/// This is a macro, not a function, because a phase borrows the report while it
/// records its own checks, and the timeout wrapper has to record into the same
/// report afterwards. Binding the timeout result to a local first ends the
/// phase's borrow before the wrapper takes its own.
macro_rules! run_phase {
    ($report:expr, $name:expr, $timeout:expr, $phase:expr) => {{
        println!("\n--- {} ---", $name);
        let outcome = tokio::time::timeout($timeout, $phase).await;
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => $report.fail(&format!("{}: phase error", $name), e.to_string()),
            Err(_) => $report.fail(
                &format!("{}: timeout", $name),
                format!("phase did not finish within {}s", $timeout.as_secs()),
            ),
        }
    }};
}

async fn open_producer(cfg: &Config) -> Result<ProducerClient, BoxError> {
    let builder = ProducerClient::builder().with_application_id(APP_ID.to_string());
    Ok(match &cfg.mode {
        Mode::Entra {
            host,
            eventhub,
            credential,
        } => {
            builder
                .open(host.as_str(), eventhub.as_str(), credential.clone())
                .await?
        }
        Mode::ConnectionString {
            connection_string,
            eventhub,
        } => {
            builder
                .open_with_connection_string(connection_string, eventhub.as_deref())
                .await?
        }
    })
}

async fn open_consumer(cfg: &Config) -> Result<ConsumerClient, BoxError> {
    let builder = ConsumerClient::builder()
        .with_application_id(APP_ID.to_string())
        .with_consumer_group(cfg.consumer_group.clone());
    Ok(match &cfg.mode {
        Mode::Entra {
            host,
            eventhub,
            credential,
        } => {
            // ConsumerClient::open takes an owned String for the event hub name,
            // unlike ProducerClient::open which takes &str.
            builder
                .open(host.as_str(), eventhub.clone(), credential.clone())
                .await?
        }
        Mode::ConnectionString {
            connection_string,
            eventhub,
        } => {
            builder
                .open_with_connection_string(connection_string, eventhub.as_deref())
                .await?
        }
    })
}

/// A best-effort unique tag for this run. Seeded from wall-clock time so two
/// concurrent runs against the same hub do not confuse each other's events.
fn run_marker() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("smoke-{millis}")
}

/// Parses the trailing index out of a `<marker>-<index>` event body.
fn parse_index(body: &[u8], marker: &str) -> Option<usize> {
    let text = std::str::from_utf8(body).ok()?;
    let rest = text.strip_prefix(marker)?.strip_prefix('-')?;
    rest.parse().ok()
}

/// State handed from one phase to the next.
struct RunState {
    eventhub_name: String,
    partition_id: String,
    /// Partition tail captured before this run sent anything, so the consume
    /// phase reads back only this run's events.
    start_sequence: i64,
    marker: String,
    sent: usize,
}

// ---------------------------------------------------------------------------
// P1: management operations
// ---------------------------------------------------------------------------

async fn phase_management(
    cfg: &Config,
    report: &mut Report,
    state: &mut Option<RunState>,
) -> Result<(), BoxError> {
    let producer = match open_producer(cfg).await {
        Ok(p) => {
            report.ok("P1 connect: producer", "");
            p
        }
        Err(e) => {
            report.fail("P1 connect: producer", e.to_string());
            return Ok(());
        }
    };

    let properties = match producer.get_eventhub_properties().await {
        Ok(p) => {
            report.ok(
                "P1 get_eventhub_properties",
                format!("{} partition(s)", p.partition_ids.len()),
            );
            p
        }
        Err(e) => {
            report.fail("P1 get_eventhub_properties", e.to_string());
            let _ = producer.close().await;
            return Ok(());
        }
    };

    let Some(partition_id) = properties.partition_ids.first().cloned() else {
        report.fail("P1 get_eventhub_properties", "hub has 0 partitions");
        let _ = producer.close().await;
        return Ok(());
    };

    // Query every partition, not just the one this run uses. A management path
    // that works for partition 0 and fails for partition 7 is a real defect.
    let mut queried = 0usize;
    let mut start_sequence = 0i64;
    for id in &properties.partition_ids {
        match producer.get_partition_properties(id).await {
            Ok(pp) => {
                if id == &partition_id {
                    start_sequence = pp.last_enqueued_sequence_number;
                }
                queried += 1;
            }
            Err(e) => {
                report.fail(
                    "P1 get_partition_properties",
                    format!("partition {id}: {e}"),
                );
                let _ = producer.close().await;
                return Ok(());
            }
        }
    }
    report.ok(
        "P1 get_partition_properties",
        format!(
            "{queried} partition(s) queried, tail seq {start_sequence} on partition {partition_id}"
        ),
    );

    *state = Some(RunState {
        eventhub_name: properties.name.clone(),
        partition_id,
        start_sequence,
        marker: run_marker(),
        sent: 0,
    });

    let _ = producer.close().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// P2: produce
// ---------------------------------------------------------------------------

async fn phase_produce(
    cfg: &Config,
    report: &mut Report,
    state: &mut Option<RunState>,
) -> Result<(), BoxError> {
    let Some(state) = state.as_mut() else {
        report.skip("P2 produce", "no partition from P1");
        return Ok(());
    };

    let producer = open_producer(cfg).await?;

    // Single event, addressed to a specific partition.
    match producer
        .send_event(
            format!("{}-single", state.marker),
            Some(SendEventOptions {
                partition_id: Some(state.partition_id.clone()),
            }),
        )
        .await
    {
        Ok(()) => report.ok(
            "P2 send_event (partition-addressed)",
            format!("partition {}", state.partition_id),
        ),
        Err(e) => report.fail("P2 send_event (partition-addressed)", e.to_string()),
    }

    // Single event with no partition, so the gateway routes it. This exercises
    // a different link than the partition-addressed send.
    match producer
        .send_event(format!("{}-gateway", state.marker), None)
        .await
    {
        Ok(()) => report.ok("P2 send_event (gateway-routed)", ""),
        Err(e) => report.fail("P2 send_event (gateway-routed)", e.to_string()),
    }

    // Tagged batch to the partition the consume phase reads.
    match send_batch(
        &producer,
        &state.partition_id,
        &state.marker,
        cfg.event_count,
    )
    .await
    {
        Ok(n) => {
            state.sent = n;
            report.ok(
                "P2 send_batch",
                format!("{n} event(s) to partition {}", state.partition_id),
            );
        }
        Err(e) => report.fail("P2 send_batch", e.to_string()),
    }

    // Batch-full behavior. A batch capped at the smallest size the service
    // accepts must eventually refuse an event by returning false, not by
    // returning an error and not by silently over-filling.
    match check_batch_full(&producer, &state.partition_id).await {
        Ok(detail) => report.ok("P2 batch full returns false", detail),
        Err(e) => report.fail("P2 batch full returns false", e.to_string()),
    }

    let _ = producer.close().await;
    Ok(())
}

/// Fills a single-partition batch with uniquely tagged events and sends it.
/// Returns the number of events actually sent.
async fn send_batch(
    producer: &ProducerClient,
    partition_id: &str,
    marker: &str,
    count: usize,
) -> Result<usize, BoxError> {
    let batch = producer
        .create_batch(Some(EventDataBatchOptions {
            partition_id: Some(partition_id.to_string()),
            ..Default::default()
        }))
        .await?;

    let mut added = 0usize;
    for i in 0..count {
        let body = format!("{marker}-{i}");
        let fit = batch.try_add_event_data(
            EventData::builder()
                .with_body(body)
                .add_property("smoke_index".to_string(), i as i64)
                .with_message_id(i as u64)
                .build(),
            None,
        )?;
        if !fit {
            // Batch is full. Send what we have rather than failing.
            break;
        }
        added += 1;
    }

    producer.send_batch(batch, None).await?;
    Ok(added)
}

/// Adds events to a deliberately small batch until one is refused. The batch is
/// never sent, so this costs nothing on the service side.
async fn check_batch_full(
    producer: &ProducerClient,
    partition_id: &str,
) -> Result<String, BoxError> {
    const SMALL_BATCH_BYTES: u64 = 1024;
    let batch = producer
        .create_batch(Some(EventDataBatchOptions {
            max_size_in_bytes: Some(SMALL_BATCH_BYTES),
            partition_id: Some(partition_id.to_string()),
            ..Default::default()
        }))
        .await?;

    // Each event carries a 128-byte body, so a 1 KiB cap is reached quickly.
    let body = "x".repeat(128);
    for i in 0..64 {
        let fit =
            batch.try_add_event_data(EventData::builder().with_body(body.clone()).build(), None)?;
        if !fit {
            return Ok(format!(
                "refused event {i} at {} byte(s), {} event(s) held",
                batch.size(),
                batch.len()
            ));
        }
    }
    Err(format!(
        "batch capped at {SMALL_BATCH_BYTES} bytes accepted 64 events of {} bytes each",
        body.len()
    )
    .into())
}

// ---------------------------------------------------------------------------
// P3: consume
// ---------------------------------------------------------------------------

async fn phase_consume(
    cfg: &Config,
    report: &mut Report,
    state: &Option<RunState>,
) -> Result<(), BoxError> {
    let Some(state) = state.as_ref() else {
        report.skip("P3 consume", "no partition from P1");
        return Ok(());
    };
    if state.sent == 0 {
        report.skip("P3 consume", "P2 sent no batch events");
        return Ok(());
    }

    let consumer = match open_consumer(cfg).await {
        Ok(c) => {
            report.ok("P3 connect: consumer", "");
            c
        }
        Err(e) => {
            report.fail("P3 connect: consumer", e.to_string());
            return Ok(());
        }
    };

    // Read from the tail captured in P1, so only this run's events are in view.
    let receiver = consumer
        .open_receiver_on_partition(
            state.partition_id.clone(),
            Some(OpenReceiverOptions {
                start_position: Some(StartPosition {
                    location: StartLocation::SequenceNumber(state.start_sequence),
                    ..Default::default()
                }),
                receive_timeout: Some(Duration::seconds(RECEIVE_TIMEOUT.as_secs() as i64)),
                ..Default::default()
            }),
        )
        .await?;

    let mut found = vec![false; state.sent];
    let mut remaining = state.sent;
    let mut system_properties_ok = true;
    let mut system_properties_detail = String::new();
    let mut first_event_checked = false;

    {
        let mut stream = receiver.stream_events();
        while remaining > 0 {
            let next = tokio::time::timeout(RECEIVE_TIMEOUT, stream.next()).await;
            let event = match next {
                Ok(Some(Ok(event))) => event,
                Ok(Some(Err(e))) => {
                    report.fail("P3 receive", format!("receive error: {e}"));
                    break;
                }
                // The stream ended, or the outer timeout tripped.
                Ok(None) | Err(_) => break,
            };

            // Broker-populated system properties must be present on a received
            // event. A consumer cannot checkpoint without a sequence number.
            if !first_event_checked {
                first_event_checked = true;
                let mut missing = Vec::new();
                if event.sequence_number().is_none() {
                    missing.push("sequence_number");
                }
                if event.offset().is_none() {
                    missing.push("offset");
                }
                if event.enqueued_time().is_none() {
                    missing.push("enqueued_time");
                }
                if missing.is_empty() {
                    system_properties_detail = format!(
                        "seq {:?}, offset present, enqueued time present",
                        event.sequence_number()
                    );
                } else {
                    system_properties_ok = false;
                    system_properties_detail = format!("missing: {}", missing.join(", "));
                }
            }

            let Some(body) = event.event_data().body() else {
                continue;
            };
            if let Some(idx) = parse_index(body, &state.marker) {
                if idx < state.sent && !found[idx] {
                    found[idx] = true;
                    remaining -= 1;
                }
            }
        }
    }
    receiver.close().await?;

    let got = state.sent - remaining;
    report.record(
        "P3 round-trip receive and verify",
        if remaining == 0 {
            Outcome::Pass
        } else {
            Outcome::Fail
        },
        format!("received {got}/{} of this run's events", state.sent),
    );

    if first_event_checked {
        report.record(
            "P3 system properties populated",
            if system_properties_ok {
                Outcome::Pass
            } else {
                Outcome::Fail
            },
            system_properties_detail,
        );
    } else {
        report.skip("P3 system properties populated", "no event received");
    }

    // Reading from Earliest exercises a different start-position encoding than
    // the sequence-number read above.
    match read_one_from_earliest(&consumer, &state.partition_id).await {
        Ok(detail) => report.ok("P3 StartLocation::Earliest", detail),
        Err(e) => report.fail("P3 StartLocation::Earliest", e.to_string()),
    }

    let _ = consumer.close().await;
    Ok(())
}

/// Opens a receiver at the start of the partition and reads a single event.
async fn read_one_from_earliest(
    consumer: &ConsumerClient,
    partition_id: &str,
) -> Result<String, BoxError> {
    let receiver = consumer
        .open_receiver_on_partition(
            partition_id.to_string(),
            Some(OpenReceiverOptions {
                start_position: Some(StartPosition {
                    location: StartLocation::Earliest,
                    ..Default::default()
                }),
                receive_timeout: Some(Duration::seconds(RECEIVE_TIMEOUT.as_secs() as i64)),
                ..Default::default()
            }),
        )
        .await?;

    let outcome = {
        let mut stream = receiver.stream_events();
        match tokio::time::timeout(RECEIVE_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(event))) => Ok(format!("first event seq {:?}", event.sequence_number())),
            Ok(Some(Err(e))) => Err(BoxError::from(format!("receive error: {e}"))),
            Ok(None) => Err("stream ended before any event".into()),
            Err(_) => Err("timed out waiting for the first event".into()),
        }
    };
    receiver.close().await?;
    outcome
}

// ---------------------------------------------------------------------------
// P4: event processor
// ---------------------------------------------------------------------------

async fn phase_processor(
    cfg: &Config,
    report: &mut Report,
    state: &Option<RunState>,
) -> Result<(), BoxError> {
    let Some(state) = state.as_ref() else {
        report.skip("P4 processor", "no event hub name from P1");
        return Ok(());
    };

    let consumer = match open_consumer(cfg).await {
        Ok(c) => c,
        Err(e) => {
            report.fail("P4 processor: open consumer", e.to_string());
            return Ok(());
        }
    };

    let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    let processor = match EventProcessor::builder()
        .build(consumer, checkpoint_store.clone())
        .await
    {
        Ok(p) => {
            report.ok("P4 processor: build", "");
            p
        }
        Err(e) => {
            report.fail("P4 processor: build", e.to_string());
            return Ok(());
        }
    };

    // Run the load-balancing loop in the background.
    let background = tokio::spawn({
        let processor = processor.clone();
        async move { processor.run().await }
    });

    let partition_client =
        match tokio::time::timeout(RECEIVE_TIMEOUT, processor.next_partition_client()).await {
            Ok(Ok(pc)) => {
                report.ok(
                    "P4 processor: claim partition",
                    format!("partition {}", pc.get_partition_id()),
                );
                pc
            }
            Ok(Err(e)) => {
                report.fail("P4 processor: claim partition", e.to_string());
                stop_processor(&processor, background).await;
                return Ok(());
            }
            Err(_) => {
                report.fail("P4 processor: claim partition", "timed out");
                stop_processor(&processor, background).await;
                return Ok(());
            }
        };

    // The partition receiver attaches lazily, on the first poll of
    // `stream_events()`, and it starts at the tail. Poll it once before the
    // trigger event is sent so the link exists first. Without this step the
    // event is enqueued before the attach, the `@latest` filter is evaluated
    // after it, and the event is never delivered.
    let partition_id = partition_client.get_partition_id().to_string();
    let mut stream = partition_client.stream_events();
    let primed = tokio::time::timeout(ATTACH_TIMEOUT, stream.next()).await;
    if let Ok(Some(Err(e))) = primed {
        report.fail("P4 processor: attach receiver", e.to_string());
        stop_processor(&processor, background).await;
        return Ok(());
    }
    report.ok("P4 processor: attach receiver", "");

    if let Err(e) = send_trigger_event(cfg, &partition_id).await {
        report.fail("P4 processor: send trigger event", e.to_string());
        stop_processor(&processor, background).await;
        return Ok(());
    }
    report.ok("P4 processor: send trigger event", "");

    let checkpoint_seq = {
        match tokio::time::timeout(RECEIVE_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(event))) => match partition_client.update_checkpoint(&event).await {
                Ok(()) => Some(event.sequence_number()),
                Err(e) => {
                    report.fail("P4 processor: update_checkpoint", e.to_string());
                    None
                }
            },
            Ok(Some(Err(e))) => {
                report.fail("P4 processor: receive event", e.to_string());
                None
            }
            Ok(None) => {
                report.fail("P4 processor: receive event", "stream ended");
                None
            }
            Err(_) => {
                report.fail("P4 processor: receive event", "timed out");
                None
            }
        }
    };

    if let Some(seq) = checkpoint_seq {
        report.ok(
            "P4 processor: update_checkpoint",
            format!("checkpointed seq {seq:?}"),
        );

        match checkpoint_store
            .list_checkpoints(&cfg.namespace, &state.eventhub_name, &cfg.consumer_group)
            .await
        {
            Ok(cps) if !cps.is_empty() => report.ok(
                "P4 processor: checkpoint persisted",
                format!("{} checkpoint(s) in store", cps.len()),
            ),
            Ok(_) => report.fail("P4 processor: checkpoint persisted", "store is empty"),
            Err(e) => report.fail("P4 processor: checkpoint persisted", e.to_string()),
        }
    }

    stop_processor(&processor, background).await;
    Ok(())
}

async fn send_trigger_event(cfg: &Config, partition_id: &str) -> Result<(), BoxError> {
    let producer = open_producer(cfg).await?;
    producer
        .send_event(
            "smoke-processor-trigger".to_string(),
            Some(SendEventOptions {
                partition_id: Some(partition_id.to_string()),
            }),
        )
        .await?;
    let _ = producer.close().await;
    Ok(())
}

async fn stop_processor(
    processor: &Arc<EventProcessor>,
    background: tokio::task::JoinHandle<azure_messaging_eventhubs::Result<()>>,
) {
    let _ = processor.shutdown().await;
    let _ = background.await;
}

// ---------------------------------------------------------------------------
// P5: epoch steal
// ---------------------------------------------------------------------------

/// Opens a receiver at owner level 0, then opens a second receiver at owner
/// level 1 on the same partition and consumer group. The broker must displace
/// the first receiver, and the client must surface that as
/// `ErrorKind::ConsumerDisconnected` rather than silently re-attaching.
///
/// This is the behavior contract introduced in 0.15.0. It is asserted strictly:
/// a regression toward the old retry-on-stolen behavior fails this phase.
///
/// Both receivers are polled once before they are used, because
/// `open_receiver_on_partition` does not attach the AMQP link. The link is
/// attached on the first poll of `stream_events()`.
async fn phase_epoch_steal(
    cfg: &Config,
    report: &mut Report,
    state: &Option<RunState>,
) -> Result<(), BoxError> {
    let Some(state) = state.as_ref() else {
        report.skip("P5 epoch steal", "no partition from P1");
        return Ok(());
    };

    let consumer_a = open_consumer(cfg).await?;
    let receiver_a = consumer_a
        .open_receiver_on_partition(
            state.partition_id.clone(),
            Some(OpenReceiverOptions {
                owner_level: Some(0),
                start_position: Some(StartPosition {
                    location: StartLocation::Latest,
                    ..Default::default()
                }),
                receive_timeout: Some(Duration::seconds(RECEIVE_TIMEOUT.as_secs() as i64)),
                ..Default::default()
            }),
        )
        .await?;
    report.ok(
        "P5 open receiver A (owner level 0)",
        format!("partition {}", state.partition_id),
    );

    // `open_receiver_on_partition` does not attach the AMQP link. The link is
    // attached on the first poll of `stream_events()`. Poll receiver A now, so
    // there is a link for receiver B to displace. Hold the stream: a later poll
    // of the same stream resumes the same receive.
    let mut stream_a = receiver_a.stream_events();
    if let Ok(Some(Err(e))) = tokio::time::timeout(ATTACH_TIMEOUT, stream_a.next()).await {
        report.fail("P5 attach receiver A", e.to_string());
        return Ok(());
    }
    report.ok("P5 attach receiver A", "");

    // Open the displacing receiver at a higher epoch.
    let consumer_b = open_consumer(cfg).await?;
    let receiver_b = consumer_b
        .open_receiver_on_partition(
            state.partition_id.clone(),
            Some(OpenReceiverOptions {
                owner_level: Some(1),
                start_position: Some(StartPosition {
                    location: StartLocation::Latest,
                    ..Default::default()
                }),
                receive_timeout: Some(Duration::seconds(RECEIVE_TIMEOUT.as_secs() as i64)),
                ..Default::default()
            }),
        )
        .await?;
    report.ok("P5 open receiver B (owner level 1)", "");

    // Attach receiver B as well, for the same reason: the displacement happens
    // when B's link attaches, and B must be attached before the marker event is
    // sent or B starts after the event.
    let mut stream_b = receiver_b.stream_events();
    if let Ok(Some(Err(e))) = tokio::time::timeout(ATTACH_TIMEOUT, stream_b.next()).await {
        report.fail("P5 attach receiver B", e.to_string());
        return Ok(());
    }
    report.ok("P5 attach receiver B", "");

    // Receiver A must now report the displacement. Match on the error kind, not
    // on the message text.
    {
        match tokio::time::timeout(STEAL_TIMEOUT, stream_a.next()).await {
            Ok(Some(Err(e))) => {
                if matches!(e.kind, ErrorKind::ConsumerDisconnected(_)) {
                    report.ok("P5 receiver A reports ConsumerDisconnected", "");
                } else {
                    report.fail(
                        "P5 receiver A reports ConsumerDisconnected",
                        format!("got a different error kind: {e}"),
                    );
                }
            }
            Ok(Some(Ok(_))) => report.fail(
                "P5 receiver A reports ConsumerDisconnected",
                "receiver A still delivered an event after being displaced",
            ),
            Ok(None) => report.fail(
                "P5 receiver A reports ConsumerDisconnected",
                "receiver A's stream ended without an error",
            ),
            Err(_) => report.fail(
                "P5 receiver A reports ConsumerDisconnected",
                "receiver A neither errored nor ended before the timeout",
            ),
        }
    }

    // The displacing receiver must work.
    let marker = format!("{}-steal", state.marker);
    let producer = open_producer(cfg).await?;
    producer
        .send_event(
            marker.clone(),
            Some(SendEventOptions {
                partition_id: Some(state.partition_id.clone()),
            }),
        )
        .await?;
    let _ = producer.close().await;

    {
        let deadline = tokio::time::Instant::now() + RECEIVE_TIMEOUT;
        let mut delivered = false;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, stream_b.next()).await {
                Ok(Some(Ok(event))) => {
                    if event.event_data().body().map(|b| b == marker.as_bytes()) == Some(true) {
                        delivered = true;
                        break;
                    }
                }
                Ok(Some(Err(e))) => {
                    report.fail("P5 receiver B receives", e.to_string());
                    break;
                }
                Ok(None) | Err(_) => break,
            }
        }
        if delivered {
            report.ok(
                "P5 receiver B receives",
                "displacing receiver got the event",
            );
        } else {
            report.fail(
                "P5 receiver B receives",
                "displacing receiver did not get the event",
            );
        }
    }

    // The streams borrow the receivers, and `close` takes the receiver by value.
    drop(stream_a);
    drop(stream_b);
    let _ = receiver_a.close().await;
    let _ = receiver_b.close().await;
    let _ = consumer_a.close().await;
    let _ = consumer_b.close().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// P6: idle-recovery soak
// ---------------------------------------------------------------------------

/// Sends, sits idle long enough for the service to tear the connection down,
/// then sends again on the same client. A PASS means recovery is transparent. A
/// timeout is direct evidence of a hang in the recovery path.
async fn phase_idle_soak(
    cfg: &Config,
    report: &mut Report,
    state: &Option<RunState>,
) -> Result<(), BoxError> {
    if cfg.soak.is_zero() {
        report.skip("P6 idle recovery soak", "set SMOKE_SOAK_SECS to enable");
        return Ok(());
    }
    let Some(state) = state.as_ref() else {
        report.skip("P6 idle recovery soak", "no partition from P1");
        return Ok(());
    };

    let producer = open_producer(cfg).await?;
    // `SendEventOptions` is not `Clone` on this version of the crate, so the
    // options are rebuilt for the second send. This is noted in the readiness
    // report as an API hygiene item.
    let to_partition = || {
        Some(SendEventOptions {
            partition_id: Some(state.partition_id.clone()),
        })
    };

    producer
        .send_event(format!("{}-soak-before", state.marker), to_partition())
        .await?;
    report.ok("P6 send before idle", "");

    println!("    idling for {}s ...", cfg.soak.as_secs());
    tokio::time::sleep(cfg.soak).await;

    match producer
        .send_event(format!("{}-soak-after", state.marker), to_partition())
        .await
    {
        Ok(()) => report.ok(
            "P6 send after idle",
            format!("recovered after {}s idle", cfg.soak.as_secs()),
        ),
        Err(e) => report.fail("P6 send after idle", e.to_string()),
    }

    let _ = producer.close().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// P7: pre-formed SAS expiry
// ---------------------------------------------------------------------------

/// Percent-encoding set matching the crate's own SAS signer.
const SAS_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Signs a SAS token the same way the crate does: the resource is percent
/// encoded then lowercased, the string to sign is `resource\nexpiry`, and the
/// HMAC-SHA256 runs over the raw key bytes (the key is not base64-decoded).
fn sign_sas(audience: &str, key_name: &str, key: &Secret, expiry: i64) -> Result<String, BoxError> {
    let resource = utf8_percent_encode(audience, SAS_ENCODE_SET)
        .to_string()
        .to_lowercase();
    let string_to_sign = format!("{resource}\n{expiry}");
    let mut mac = Hmac::<Sha256>::new_from_slice(key.secret().as_bytes())
        .map_err(|e| BoxError::from(format!("invalid SAS signing key: {e}")))?;
    mac.update(string_to_sign.as_bytes());
    let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    let signature = utf8_percent_encode(&signature, SAS_ENCODE_SET).to_string();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={key_name}"
    ))
}

/// Opens with a short-lived pre-formed SAS token, confirms it works, waits for
/// the token to expire, then confirms a fresh open reports a clean error
/// instead of hanging. A pre-formed token cannot be renewed, so expiry is a
/// real terminal state the client must handle.
async fn phase_sas_expiry(cfg: &Config, report: &mut Report) -> Result<(), BoxError> {
    if cfg.sas_expiry.is_zero() {
        report.skip("P7 SAS expiry", "set SMOKE_SAS_EXPIRY_SECS to enable");
        return Ok(());
    }
    let Some(parsed) = cfg.parsed_connection_string() else {
        report.skip("P7 SAS expiry", "needs EVENTHUBS_CONNECTION_STRING");
        return Ok(());
    };
    let (Some(key_name), Some(key)) = (
        parsed.shared_access_key_name.clone(),
        parsed.shared_access_key.clone(),
    ) else {
        report.skip("P7 SAS expiry", "connection string carries no signing key");
        return Ok(());
    };
    let eventhub = match &cfg.mode {
        Mode::ConnectionString { eventhub, .. } => eventhub.clone(),
        Mode::Entra { .. } => None,
    };
    let Some(eventhub) = eventhub else {
        report.skip("P7 SAS expiry", "no event hub name");
        return Ok(());
    };

    let expiry_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_add(cfg.sas_expiry.as_secs()) as i64;
    let audience = format!("amqps://{}/{}", parsed.fully_qualified_namespace, eventhub);
    let token = sign_sas(&audience, &key_name, &key, expiry_at)?;
    let short_lived = format!(
        "Endpoint=sb://{}/;SharedAccessSignature={}",
        parsed.fully_qualified_namespace, token
    );

    // Before expiry the token must authorize a management call.
    match ProducerClient::builder()
        .with_application_id(APP_ID.to_string())
        .open_with_connection_string(&short_lived, Some(&eventhub))
        .await
    {
        Ok(producer) => match producer.get_eventhub_properties().await {
            Ok(_) => {
                report.ok(
                    "P7 pre-formed SAS works before expiry",
                    format!("token valid for {}s", cfg.sas_expiry.as_secs()),
                );
                let _ = producer.close().await;
            }
            Err(e) => report.fail("P7 pre-formed SAS works before expiry", e.to_string()),
        },
        Err(e) => report.fail("P7 pre-formed SAS works before expiry", e.to_string()),
    }

    // Wait past the expiry, with a small margin for clock skew.
    let wait = cfg.sas_expiry + StdDuration::from_secs(5);
    println!(
        "    waiting {}s for the token to expire ...",
        wait.as_secs()
    );
    tokio::time::sleep(wait).await;

    // After expiry a fresh open must fail, and must fail rather than hang. The
    // enclosing phase timeout catches a hang.
    let opened = ProducerClient::builder()
        .with_application_id(APP_ID.to_string())
        .open_with_connection_string(&short_lived, Some(&eventhub))
        .await;
    match opened {
        Ok(producer) => match producer.get_eventhub_properties().await {
            Ok(_) => report.fail(
                "P7 expired SAS is rejected",
                "an expired token still authorized a management call",
            ),
            Err(e) => report.ok("P7 expired SAS is rejected", format!("rejected: {e}")),
        },
        Err(e) => report.ok(
            "P7 expired SAS is rejected",
            format!("rejected at open: {e}"),
        ),
    }

    Ok(())
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> ExitCode {
    // Honor RUST_LOG if set, otherwise stay quiet so the report is readable.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cfg = match Config::from_env() {
        ConfigOutcome::Ready(cfg) => cfg,
        ConfigOutcome::NoCredentials => {
            println!(
                "[SKIP] Event Hubs smoke test  --  no credentials in the environment. \
                 Set EVENTHUBS_CONNECTION_STRING (plus EVENTHUB_NAME), or EVENTHUBS_HOST \
                 plus EVENTHUB_NAME for Microsoft Entra ID."
            );
            return ExitCode::SUCCESS;
        }
        ConfigOutcome::Invalid(e) => {
            eprintln!("configuration error: {e}");
            return ExitCode::from(2);
        }
    };

    println!("Event Hubs SDK smoke test");
    println!("namespace: {}", cfg.namespace);
    println!("consumer group: {}", cfg.consumer_group);
    println!("phase timeout: {}s", cfg.phase_timeout.as_secs());
    println!("----------------------------------------");

    let mut report = Report::default();
    let mut state: Option<RunState> = None;
    let timeout = cfg.phase_timeout;

    run_phase!(
        report,
        "P1 management",
        timeout,
        phase_management(&cfg, &mut report, &mut state)
    );
    run_phase!(
        report,
        "P2 produce",
        timeout,
        phase_produce(&cfg, &mut report, &mut state)
    );
    run_phase!(
        report,
        "P3 consume",
        timeout,
        phase_consume(&cfg, &mut report, &state)
    );
    run_phase!(
        report,
        "P4 processor",
        timeout,
        phase_processor(&cfg, &mut report, &state)
    );
    // The steal phase waits on the broker, so it gets the steal window on top of
    // the standard phase budget.
    run_phase!(
        report,
        "P5 epoch steal",
        timeout + STEAL_TIMEOUT,
        phase_epoch_steal(&cfg, &mut report, &state)
    );
    // The soak phase is longer than a normal phase by design, so it gets its own
    // timeout: the idle window plus the standard phase budget.
    run_phase!(
        report,
        "P6 idle recovery soak",
        timeout + cfg.soak,
        phase_idle_soak(&cfg, &mut report, &state)
    );
    run_phase!(
        report,
        "P7 SAS expiry",
        timeout + cfg.sas_expiry * 2,
        phase_sas_expiry(&cfg, &mut report)
    );

    report.summary();

    if report.failures().is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
