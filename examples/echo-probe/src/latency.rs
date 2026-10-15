use std::{env, time::Instant};

use anyhow::{Context, ensure};
use relaygate_sdk::Destination;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::watch,
    task::JoinSet,
    time::timeout,
};

use crate::{
    config::{
        DESTINATION_WAIT, DESTINATIONS, ECHO_DEADLINE, access_token_source, destination,
        gateway_addresses,
    },
    probe::{connect, dial_when_available},
};

#[derive(Clone, Copy, Debug)]
struct Settings {
    warmup: usize,
    samples_per_pipe: usize,
    payload_bytes: usize,
    concurrency: usize,
    idle_pipes: usize,
}

#[derive(Clone, Copy)]
enum Phase {
    Warmup,
    Measurement,
}

impl Settings {
    fn from_environment() -> anyhow::Result<Self> {
        let settings = Self {
            warmup: setting("RELAYGATE_LATENCY_WARMUP", 100, 0, 10_000)?,
            samples_per_pipe: setting("RELAYGATE_LATENCY_SAMPLES", 1_000, 1, 100_000)?,
            payload_bytes: setting("RELAYGATE_LATENCY_PAYLOAD_BYTES", 64, 1, 65_536)?,
            concurrency: setting("RELAYGATE_LATENCY_CONCURRENCY", 1, 1, 64)?,
            idle_pipes: setting("RELAYGATE_LATENCY_IDLE_PIPES", 0, 0, 1_024)?,
        };
        settings.requested_samples()?;
        Ok(settings)
    }

    fn requested_samples(self) -> anyhow::Result<usize> {
        let total = self
            .samples_per_pipe
            .checked_mul(self.concurrency)
            .context("total requested latency samples overflowed")?;
        ensure!(
            total <= 100_000,
            "RELAYGATE_LATENCY_SAMPLES * RELAYGATE_LATENCY_CONCURRENCY must be <= 100000"
        );
        Ok(total)
    }
}

pub(crate) async fn run() -> anyhow::Result<()> {
    let access_token_source = access_token_source()?;
    let settings = Settings::from_environment()?;
    let single = env::var("RELAYGATE_ADDR").ok();
    let addresses = match &single {
        Some(address) => vec![address.clone()],
        None => gateway_addresses()?,
    };
    let destinations = if single.is_some() {
        vec![destination()?]
    } else {
        DESTINATIONS
            .iter()
            .map(|value| (*value).to_owned())
            .collect()
    };
    let mut cases = Vec::new();
    let mut failed = false;
    for (entry, address) in addresses.iter().enumerate() {
        let setup = Instant::now();
        let relay = connect(address).await?;
        let setup_seconds = setup.elapsed().as_secs_f64();
        let entry_result = async {
            for (owner, destination) in destinations.iter().enumerate() {
                // Registration convergence is a preflight, outside the timed dial and DATA samples.
                drop(
                    dial_when_available(
                        &relay,
                        destination,
                        &access_token_source,
                        DESTINATION_WAIT,
                    )
                    .await?,
                );
                let destination_address = destination.parse::<Destination>()?;
                let dial_started = Instant::now();
                // Idle streams precede active streams on the same Relay and peer transport.
                let mut idle_pipes = Vec::with_capacity(settings.idle_pipes);
                for _ in 0..settings.idle_pipes {
                    idle_pipes.push(
                        relay
                            .dial(destination_address.clone(), access_token_source.clone())
                            .await?,
                    );
                }
                let mut active_pipes = Vec::with_capacity(settings.concurrency);
                for _ in 0..settings.concurrency {
                    active_pipes.push(
                        relay
                            .dial(destination_address.clone(), access_token_source.clone())
                            .await?,
                    );
                }
                let dial_seconds = dial_started.elapsed().as_secs_f64();
                let mut result = measure(active_pipes, idle_pipes, settings).await?;
                failed |= result["errors"].as_u64().unwrap_or(1) != 0;
                result["path"] = json!(if single.is_some() {
                    "single_target"
                } else if entry == owner {
                    "local"
                } else {
                    "one_hop"
                });
                result["entry_index"] = json!(entry);
                result["owner_index"] = json!(owner);
                result["session_connect_seconds"] = json!(setup_seconds);
                result["dial_seconds"] = json!(dial_seconds);
                cases.push(result);
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        // Explicitly close on setup/warm-up failure as well as successful measurement.
        relay.close();
        entry_result?;
    }
    println!(
        "{}",
        json!({"probe": "established_pipe_rtt", "concurrency": settings.concurrency, "idle_pipes": settings.idle_pipes, "cases": cases})
    );
    ensure!(
        !failed,
        "DATA latency probe had failed exchanges; see JSON results"
    );
    Ok(())
}

async fn measure<S>(pipes: Vec<S>, idle_pipes: Vec<S>, settings: Settings) -> anyhow::Result<Value>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    ensure!(
        pipes.len() == settings.concurrency,
        "active Pipe count differs from latency concurrency"
    );
    ensure!(
        idle_pipes.len() == settings.idle_pipes,
        "idle Pipe count differs from latency settings"
    );
    let mut idle_monitors = JoinSet::new();
    let (stop_idle_tx, stop_idle_rx) = watch::channel(false);
    for (idle, mut pipe) in idle_pipes.into_iter().enumerate() {
        let mut stop_idle_rx = stop_idle_rx.clone();
        idle_monitors.spawn(async move {
            let mut buffer = [0; 1];
            tokio::select! {
                biased;
                result = pipe.read(&mut buffer) => Err(match result {
                    Ok(0) => anyhow::anyhow!("idle Pipe {idle} closed during latency probe"),
                    Ok(_) => anyhow::anyhow!(
                        "idle Pipe {idle} received unexpected DATA during latency probe"
                    ),
                    Err(error) => anyhow::anyhow!("idle Pipe {idle} failed during latency probe: {error}"),
                }),
                _ = stop_idle_rx.changed() => Ok(()),
            }
        });
    }
    let mut workers = JoinSet::new();
    let mut result = tokio::select! {
        biased;
        failure = idle_monitors.join_next(), if !idle_monitors.is_empty() => {
            Err(match failure {
                Some(Ok(Err(error))) => error,
                Some(Err(error)) => anyhow::anyhow!("idle Pipe monitor failed to join: {error}"),
                _ => anyhow::anyhow!("idle Pipe monitor ended unexpectedly"),
            })
        }
        result = measure_active(pipes, settings, &mut workers) => result,
    };
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    // Give every monitor a final read-first poll, then inspect its completion.
    // Aborting here could discard an EOF/DATA error racing the last active sample.
    stop_idle_tx.send_replace(true);
    while let Some(monitor) = idle_monitors.join_next().await {
        if let Err(error) = monitor
            .context("idle Pipe monitor failed to join")
            .and_then(|value| value)
            && result.is_ok()
        {
            result = Err(error);
        }
    }
    result
}

async fn measure_active<S>(
    pipes: Vec<S>,
    settings: Settings,
    workers: &mut JoinSet<anyhow::Result<(usize, Vec<f64>, usize)>>,
) -> anyhow::Result<Value>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let requested_samples = settings.requested_samples()?;
    let mut prepared = Vec::with_capacity(pipes.len());
    for (worker, mut pipe) in pipes.into_iter().enumerate() {
        let mut payload = vec![0x5a; settings.payload_bytes];
        let mut received = vec![0; settings.payload_bytes];
        stamp_payload(&mut payload, worker, 0, Phase::Warmup);
        for _ in 0..settings.warmup {
            exchange(&mut pipe, &payload, &mut received)
                .await
                .with_context(|| format!("latency warm-up failed for Pipe {worker}"))?;
        }
        prepared.push((pipe, payload, received));
    }

    let (start_tx, start_rx) = watch::channel(false);
    for (worker, (mut pipe, mut payload, mut received)) in prepared.into_iter().enumerate() {
        let mut start_rx = start_rx.clone();
        workers.spawn(async move {
            start_rx
                .changed()
                .await
                .context("latency measurement start cancelled")?;
            let mut times = Vec::with_capacity(settings.samples_per_pipe);
            let mut errors = 0;
            for sequence in 0..settings.samples_per_pipe {
                stamp_payload(&mut payload, worker, sequence, Phase::Measurement);
                let round_trip = Instant::now();
                if exchange(&mut pipe, &payload, &mut received).await.is_err() {
                    errors = 1;
                    break;
                }
                times.push(round_trip.elapsed().as_secs_f64());
            }
            Ok::<_, anyhow::Error>((worker, times, errors))
        });
    }
    // One wall-clock interval covers all workers; it is not the sum of their RTTs.
    let started = Instant::now();
    start_tx.send_replace(true);
    let mut times = Vec::with_capacity(requested_samples);
    let mut completed_samples_per_pipe = vec![0; settings.concurrency];
    let mut errors = 0;
    while let Some(result) = workers.join_next().await {
        let result = result
            .context("latency worker failed to join")
            .and_then(|value| value);
        match result {
            Ok((worker, mut samples, failed)) => {
                completed_samples_per_pipe[worker] = samples.len();
                times.append(&mut samples);
                errors += failed;
            }
            Err(error) => return Err(error),
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    times.sort_by(f64::total_cmp);
    let echo_payload_bytes = times.len() as u64 * settings.payload_bytes as u64 * 2;
    Ok(json!({
        "payload_bytes": settings.payload_bytes, "warmup": settings.warmup,
        "samples_per_pipe": settings.samples_per_pipe, "idle_pipes": settings.idle_pipes,
        "requested_samples": requested_samples,
        "completed_samples": times.len(), "errors": errors,
        "completed_samples_per_pipe": completed_samples_per_pipe,
        "rtt_seconds": {"p50": percentile(&times, 50), "p95": percentile(&times, 95), "p99": percentile(&times, 99), "max": times.last()},
        "measurement_seconds": elapsed,
        "echo_payload_bytes": echo_payload_bytes,
        "echo_goodput_bytes_per_second": echo_payload_bytes as f64 / elapsed,
    }))
}

fn stamp_payload(payload: &mut [u8], worker: usize, sequence: usize, phase: Phase) {
    // Six bits identify up to 64 Pipes; a separate bit distinguishes warm-up.
    // These identities remain distinct even when a one-byte payload wraps sequence bits.
    let warmup_bit = u64::from(matches!(phase, Phase::Warmup)) << 6;
    let identity = ((sequence as u64) << 7) | warmup_bit | worker as u64;
    for (byte, identity_byte) in payload.iter_mut().zip(identity.to_le_bytes()) {
        *byte = identity_byte;
    }
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    pipe: &mut S,
    payload: &[u8],
    received: &mut [u8],
) -> anyhow::Result<()> {
    timeout(ECHO_DEADLINE, async {
        pipe.write_all(payload).await?;
        pipe.read_exact(received).await?;
        ensure!(received == payload, "echo payload mismatch");
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("DATA RTT exchange timed out")?
}

fn percentile(sorted: &[f64], percent: usize) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    Some(sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)])
}

fn setting(name: &str, default: usize, min: usize, max: usize) -> anyhow::Result<usize> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an integer"))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    ensure!(
        (min..=max).contains(&value),
        "{name} must be in {min}..={max}"
    );
    Ok(value)
}

#[cfg(test)]
mod tests;
