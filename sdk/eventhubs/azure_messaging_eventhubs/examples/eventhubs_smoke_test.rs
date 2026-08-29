// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// cspell: words eventhub eventhubs checkpointing

//! End-to-end smoke test harness for the Event Hubs client.
//!
//! This binary drives the public surface of the crate against a *real* Event
//! Hubs namespace and prints a PASS/FAIL line per capability, followed by a
//! tally. It is meant as a fast "is `main` actually working?" gate that
//! exercises the producer, consumer, and event-processor paths in one run.
//!
//! It exits `0` only if every check passed, so it can be used in a script.
//!
//! # Configuration (environment variables)
//!
//! Microsoft Entra ID (recommended, uses your developer credentials):
//! ```text
//! EVENTHUBS_HOST=<namespace>.servicebus.windows.net
//! EVENTHUB_NAME=<event hub name>
//! ```
//!
//! or Shared Access Signature via connection string:
//! ```text
//! EVENTHUBS_CONNECTION_STRING=Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...
//! EVENTHUB_NAME=<event hub name>   # optional if the connection string has EntityPath
//! ```
//!
//! Optional:
//! ```text
//! EVENTHUB_CONSUMER_GROUP=$Default   # consumer group for the processor phase
//! SMOKE_EVENT_COUNT=5                # number of events for the round-trip phase
//! ```
//!
//! Run with:
//! ```text
//! cargo run --example eventhubs_smoke_test
//! ```
//! (The crate's dev-dependencies enable the `in_memory_checkpoint_store`
//! feature that the processor phase needs, so no extra flags are required.)

use azure_core::time::Duration;
use azure_identity::DeveloperToolsCredential;
use azure_messaging_eventhubs::{
    models::EventData, CheckpointStore, ConnectionString, ConsumerClient, EventDataBatchOptions,
    EventProcessor, InMemoryCheckpointStore, OpenReceiverOptions, ProducerClient, SendEventOptions,
    StartLocation, StartPosition,
};
use futures::StreamExt;
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use std::{env, error::Error, process::ExitCode};

/// Application id stamped on every AMQP connection this harness opens, so the
/// traffic is easy to spot in broker-side diagnostics.
const APP_ID: &str = "eventhubs_smoke_test";

/// How long any single receive is allowed to block before the phase is
/// considered failed.
const RECEIVE_TIMEOUT: StdDuration = StdDuration::from_secs(30);

type BoxError = Box<dyn Error + Send + Sync>;

/// How the harness authenticates. Both paths land on the same public API; the
/// only difference is which builder method opens the client.
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
}

impl Config {
    /// Builds the run configuration from the environment. Prefers a connection
    /// string when present, otherwise falls back to host + Entra credentials.
    fn from_env() -> Result<Self, BoxError> {
        let consumer_group =
            env::var("EVENTHUB_CONSUMER_GROUP").unwrap_or_else(|_| "$Default".to_string());
        let event_count = env::var("SMOKE_EVENT_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(5);

        if let Ok(connection_string) = env::var("EVENTHUBS_CONNECTION_STRING") {
            let parsed: ConnectionString = connection_string.parse()?;
            let eventhub = env::var("EVENTHUB_NAME")
                .ok()
                .or_else(|| parsed.entity_path.clone());
            return Ok(Self {
                namespace: parsed.fully_qualified_namespace.clone(),
                mode: Mode::ConnectionString {
                    connection_string,
                    eventhub,
                },
                consumer_group,
                event_count,
            });
        }

        let host = env::var("EVENTHUBS_HOST")
            .map_err(|_| "set EVENTHUBS_HOST + EVENTHUB_NAME, or EVENTHUBS_CONNECTION_STRING")?;
        let eventhub = env::var("EVENTHUB_NAME")
            .map_err(|_| "EVENTHUB_NAME is required with EVENTHUBS_HOST")?;
        let credential = DeveloperToolsCredential::new(None)?;
        Ok(Self {
            namespace: host.clone(),
            mode: Mode::Entra {
                host,
                eventhub,
                credential,
            },
            consumer_group,
            event_count,
        })
    }
}

/// Accumulates per-check results and renders the final tally.
#[derive(Default)]
struct Report {
    results: Vec<(String, bool)>,
}

impl Report {
    fn record(&mut self, name: &str, ok: bool, detail: impl AsRef<str>) {
        let detail = detail.as_ref();
        let tag = if ok { " OK " } else { "FAIL" };
        if detail.is_empty() {
            println!("[{tag}] {name}");
        } else {
            println!("[{tag}] {name}  --  {detail}");
        }
        self.results.push((name.to_string(), ok));
    }

    fn all_passed(&self) -> bool {
        !self.results.is_empty() && self.results.iter().all(|(_, ok)| *ok)
    }

    fn summary(&self) {
        let passed = self.results.iter().filter(|(_, ok)| *ok).count();
        println!("\n----------------------------------------");
        println!("SMOKE TEST: {passed}/{} checks passed", self.results.len());
        if !self.all_passed() {
            let failed: Vec<&str> = self
                .results
                .iter()
                .filter(|(_, ok)| !*ok)
                .map(|(n, _)| n.as_str())
                .collect();
            println!("FAILED: {}", failed.join(", "));
        }
    }
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
    let builder = ConsumerClient::builder().with_application_id(APP_ID.to_string());
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
/// concurrent runs against the same hub don't confuse each other's events.
fn run_marker() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("smoke-{millis}")
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
            // Batch is full; send what we have rather than failing.
            break;
        }
        added += 1;
    }

    producer.send_batch(batch, None).await?;
    Ok(added)
}

/// Receives from `start_sequence` and verifies that every event this run sent
/// (identified by `marker`) comes back with a matching body. Returns
/// `(all_found, detail)`.
async fn receive_and_verify(
    consumer: &ConsumerClient,
    partition_id: &str,
    start_sequence: i64,
    marker: &str,
    expected: usize,
) -> Result<(bool, String), BoxError> {
    let receiver = consumer
        .open_receiver_on_partition(
            partition_id.to_string(),
            Some(OpenReceiverOptions {
                start_position: Some(StartPosition {
                    location: StartLocation::SequenceNumber(start_sequence),
                    ..Default::default()
                }),
                receive_timeout: Some(Duration::seconds(RECEIVE_TIMEOUT.as_secs() as i64)),
                ..Default::default()
            }),
        )
        .await?;

    let mut found = vec![false; expected];
    let mut remaining = expected;

    {
        let mut stream = receiver.stream_events();
        while remaining > 0 {
            let next = tokio::time::timeout(RECEIVE_TIMEOUT, stream.next()).await;
            let event = match next {
                Ok(Some(Ok(event))) => event,
                Ok(Some(Err(e))) => return Err(format!("receive error: {e}").into()),
                Ok(None) => break, // stream ended (receive timeout inside the client)
                Err(_) => break,   // our outer timeout tripped
            };
            let Some(body) = event.event_data().body() else {
                continue;
            };
            // Match "<marker>-<index>" and tick off that index.
            if let Some(idx) = parse_index(body, marker) {
                if idx < expected && !found[idx] {
                    found[idx] = true;
                    remaining -= 1;
                }
            }
        }
    }

    receiver.close().await?;

    let got = expected - remaining;
    let detail = format!("received {got}/{expected} of this run's events");
    Ok((remaining == 0, detail))
}

/// Parses the trailing index out of a `<marker>-<index>` event body.
fn parse_index(body: &[u8], marker: &str) -> Option<usize> {
    let text = std::str::from_utf8(body).ok()?;
    let rest = text.strip_prefix(marker)?.strip_prefix('-')?;
    rest.parse().ok()
}

/// Drives the event processor: build it, claim a partition, deliver one event
/// to that partition, checkpoint it, and confirm the checkpoint persisted.
async fn processor_phase(cfg: &Config, eventhub_name: &str, report: &mut Report) {
    let consumer = match open_consumer(cfg).await {
        Ok(c) => c,
        Err(e) => {
            report.record("processor: open consumer", false, e.to_string());
            return;
        }
    };

    let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    let processor = match EventProcessor::builder()
        .build(consumer, checkpoint_store.clone())
        .await
    {
        Ok(p) => {
            report.record("processor: build", true, "");
            p
        }
        Err(e) => {
            report.record("processor: build", false, e.to_string());
            return;
        }
    };

    // Run the load-balancing loop in the background.
    let background = tokio::spawn({
        let processor = processor.clone();
        async move { processor.run().await }
    });

    // Wait for the load balancer to assign us a partition.
    let partition_client =
        match tokio::time::timeout(RECEIVE_TIMEOUT, processor.next_partition_client()).await {
            Ok(Ok(pc)) => {
                report.record(
                    "processor: claim partition",
                    true,
                    format!("partition {}", pc.get_partition_id()),
                );
                pc
            }
            Ok(Err(e)) => {
                report.record("processor: claim partition", false, e.to_string());
                stop_processor(&processor, background).await;
                return;
            }
            Err(_) => {
                report.record("processor: claim partition", false, "timed out");
                stop_processor(&processor, background).await;
                return;
            }
        };

    // The processor's receiver starts at the tail, so push one event to the
    // claimed partition to guarantee something to process.
    let partition_id = partition_client.get_partition_id().to_string();
    if let Err(e) = send_trigger_event(cfg, &partition_id).await {
        report.record("processor: send trigger event", false, e.to_string());
        stop_processor(&processor, background).await;
        return;
    }
    report.record("processor: send trigger event", true, "");

    // Receive one event and checkpoint it.
    let checkpoint_seq = {
        let mut stream = partition_client.stream_events();
        match tokio::time::timeout(RECEIVE_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(event))) => match partition_client.update_checkpoint(&event).await {
                Ok(()) => Some(event.sequence_number()),
                Err(e) => {
                    report.record("processor: update_checkpoint", false, e.to_string());
                    None
                }
            },
            Ok(Some(Err(e))) => {
                report.record("processor: receive event", false, e.to_string());
                None
            }
            Ok(None) => {
                report.record("processor: receive event", false, "stream ended");
                None
            }
            Err(_) => {
                report.record("processor: receive event", false, "timed out");
                None
            }
        }
    };

    if let Some(seq) = checkpoint_seq {
        report.record(
            "processor: update_checkpoint",
            true,
            format!("checkpointed seq {seq:?}"),
        );

        // Confirm the checkpoint round-tripped into the store.
        match checkpoint_store
            .list_checkpoints(&cfg.namespace, eventhub_name, &cfg.consumer_group)
            .await
        {
            Ok(cps) if !cps.is_empty() => report.record(
                "processor: checkpoint persisted",
                true,
                format!("{} checkpoint(s) in store", cps.len()),
            ),
            Ok(_) => report.record("processor: checkpoint persisted", false, "store is empty"),
            Err(e) => report.record("processor: checkpoint persisted", false, e.to_string()),
        }
    }

    stop_processor(&processor, background).await;
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

/// Runs the produce/consume round trip and returns the event-hub name (needed
/// by the processor phase for checkpoint lookup).
async fn round_trip_phase(cfg: &Config, report: &mut Report) -> Option<String> {
    let producer = match open_producer(cfg).await {
        Ok(p) => {
            report.record("connect: producer", true, "");
            p
        }
        Err(e) => {
            report.record("connect: producer", false, e.to_string());
            return None;
        }
    };

    let properties = match producer.get_eventhub_properties().await {
        Ok(p) => {
            report.record(
                "get_eventhub_properties",
                true,
                format!("{} partition(s)", p.partition_ids.len()),
            );
            p
        }
        Err(e) => {
            report.record("get_eventhub_properties", false, e.to_string());
            let _ = producer.close().await;
            return None;
        }
    };

    let Some(partition_id) = properties.partition_ids.first().cloned() else {
        report.record("get_eventhub_properties", false, "hub has 0 partitions");
        let _ = producer.close().await;
        return None;
    };
    let eventhub_name = properties.name.clone();

    // Capture the partition tail so we read back only what we send next.
    let start_sequence = match producer.get_partition_properties(&partition_id).await {
        Ok(pp) => {
            report.record(
                "get_partition_properties",
                true,
                format!(
                    "partition {partition_id}, tail seq {}",
                    pp.last_enqueued_sequence_number
                ),
            );
            pp.last_enqueued_sequence_number
        }
        Err(e) => {
            report.record("get_partition_properties", false, e.to_string());
            let _ = producer.close().await;
            return None;
        }
    };

    let marker = run_marker();
    match send_batch(&producer, &partition_id, &marker, cfg.event_count).await {
        Ok(n) => report.record(
            "send batch",
            true,
            format!("{n} event(s) to partition {partition_id}"),
        ),
        Err(e) => {
            report.record("send batch", false, e.to_string());
            let _ = producer.close().await;
            return None;
        }
    }

    // Consume and verify.
    match open_consumer(cfg).await {
        Ok(consumer) => {
            report.record("connect: consumer", true, "");
            match receive_and_verify(
                &consumer,
                &partition_id,
                start_sequence,
                &marker,
                cfg.event_count,
            )
            .await
            {
                Ok((ok, detail)) => report.record("round-trip: receive & verify", ok, detail),
                Err(e) => report.record("round-trip: receive & verify", false, e.to_string()),
            }
            let _ = consumer.close().await;
        }
        Err(e) => report.record("connect: consumer", false, e.to_string()),
    }

    let _ = producer.close().await;
    Some(eventhub_name)
}

#[tokio::main]
async fn main() -> ExitCode {
    // Honor RUST_LOG if set; otherwise stay quiet so the report is readable.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            eprintln!(
                "\nSet EVENTHUBS_HOST + EVENTHUB_NAME (Entra) or EVENTHUBS_CONNECTION_STRING."
            );
            return ExitCode::from(2);
        }
    };

    println!("Event Hubs SDK smoke test");
    println!("namespace: {}", cfg.namespace);
    println!("consumer group: {}", cfg.consumer_group);
    println!("----------------------------------------");

    let mut report = Report::default();

    if let Some(eventhub_name) = round_trip_phase(&cfg, &mut report).await {
        processor_phase(&cfg, &eventhub_name, &mut report).await;
    }

    report.summary();

    if report.all_passed() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
