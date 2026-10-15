use std::{collections::HashSet, future::Future, sync::Arc, task::Poll, time::Duration};

use tokio::sync::Barrier;

use super::*;

fn settings(concurrency: usize) -> Settings {
    Settings {
        warmup: 0,
        samples_per_pipe: 3,
        payload_bytes: 8,
        concurrency,
        idle_pipes: 0,
    }
}

#[test]
fn latency_percentiles_use_nearest_rank_and_preserve_empty_results() {
    let samples: Vec<_> = (1..=100).map(f64::from).collect();
    assert_eq!(percentile(&samples, 50), Some(50.0));
    assert_eq!(percentile(&samples, 95), Some(95.0));
    assert_eq!(percentile(&samples, 99), Some(99.0));
    assert_eq!(percentile(&[], 99), None);
    assert_eq!(percentile(&[0.25], 99), Some(0.25));
}

#[test]
fn sample_budget_bounds_total_timings_and_rejects_overflow() -> anyhow::Result<()> {
    let mut config = settings(64);
    config.samples_per_pipe = 1_562;
    assert_eq!(config.requested_samples()?, 99_968);
    config.samples_per_pipe = 1_563;
    assert!(config.requested_samples().is_err());
    config.samples_per_pipe = usize::MAX;
    assert!(config.requested_samples().is_err());
    config.concurrency = 1;
    config.samples_per_pipe = 100_000;
    assert_eq!(config.requested_samples()?, 100_000);
    Ok(())
}

#[test]
fn payload_identifies_each_pipe_even_with_one_byte() {
    let identities: HashSet<_> = (0..64)
        .map(|worker| {
            let mut payload = [0];
            stamp_payload(&mut payload, worker, 3, Phase::Measurement);
            payload[0]
        })
        .collect();
    assert_eq!(identities.len(), 64);
    let mut first = [0; 8];
    let mut next = [0; 8];
    stamp_payload(&mut first, 15, 0, Phase::Measurement);
    stamp_payload(&mut next, 15, 1, Phase::Measurement);
    assert_ne!(first, next);
}

#[tokio::test]
async fn active_pipes_progress_concurrently_with_exact_total_accounting() -> anyhow::Result<()> {
    let config = settings(4);
    let gate = Arc::new(Barrier::new(config.concurrency));
    let mut echoes = JoinSet::new();
    let mut pipes = Vec::new();
    for worker in 0..config.concurrency {
        let (pipe, mut peer) = tokio::io::duplex(64);
        pipes.push(pipe);
        let gate = Arc::clone(&gate);
        echoes.spawn(async move {
            for sequence in 0..config.samples_per_pipe {
                let mut payload = [0; 8];
                peer.read_exact(&mut payload).await?;
                let mut expected = [0; 8];
                stamp_payload(&mut expected, worker, sequence, Phase::Measurement);
                ensure!(payload == expected, "wrong Pipe or sample identity");
                // Every active Pipe must write before any one receives its echo.
                gate.wait().await;
                peer.write_all(&payload).await?;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    let result = timeout(Duration::from_secs(2), measure(pipes, Vec::new(), config)).await??;
    while let Some(echo) = echoes.join_next().await {
        echo??;
    }
    assert_eq!(result["requested_samples"], 12);
    assert_eq!(result["completed_samples"], 12);
    assert_eq!(result["completed_samples_per_pipe"], json!([3, 3, 3, 3]));
    assert_eq!(result["errors"], 0);
    assert_eq!(result["echo_payload_bytes"], 192);
    let elapsed = result["measurement_seconds"]
        .as_f64()
        .context("missing wall time")?;
    let rate = result["echo_goodput_bytes_per_second"]
        .as_f64()
        .context("missing goodput")?;
    assert!(elapsed > 0.0);
    assert_eq!(rate, 192.0 / elapsed);
    Ok(())
}

#[tokio::test]
async fn failed_pipe_keeps_partial_samples_while_sibling_finishes() -> anyhow::Result<()> {
    let config = settings(2);
    let mut echoes = JoinSet::new();
    let mut pipes = Vec::new();
    for worker in 0..config.concurrency {
        let (pipe, mut peer) = tokio::io::duplex(64);
        pipes.push(pipe);
        echoes.spawn(async move {
            for sequence in 0..config.samples_per_pipe {
                let mut payload = [0; 8];
                peer.read_exact(&mut payload).await?;
                if worker == 0 && sequence == 2 {
                    break;
                }
                peer.write_all(&payload).await?;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    let result = timeout(Duration::from_secs(2), measure(pipes, Vec::new(), config)).await??;
    while let Some(echo) = echoes.join_next().await {
        echo??;
    }
    assert_eq!(result["requested_samples"], 6);
    assert_eq!(result["completed_samples"], 5);
    assert_eq!(result["completed_samples_per_pipe"], json!([2, 3]));
    assert_eq!(result["errors"], 1);
    assert_eq!(result["echo_payload_bytes"], 80);
    Ok(())
}

#[tokio::test]
async fn warmup_is_excluded_and_failure_drops_all_prepared_pipes() -> anyhow::Result<()> {
    let config = Settings {
        warmup: 2,
        ..settings(1)
    };
    let (pipe, mut peer) = tokio::io::duplex(64);
    let echo = tokio::spawn(async move {
        let mut payload = [0; 8];
        for _ in 0..config.warmup + config.samples_per_pipe {
            peer.read_exact(&mut payload).await?;
            peer.write_all(&payload).await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let result = timeout(
        Duration::from_secs(2),
        measure(vec![pipe], Vec::new(), config),
    )
    .await??;
    echo.await??;
    assert_eq!(result["warmup"], 2);
    assert_eq!(result["completed_samples"], 3);
    assert_eq!(result["echo_payload_bytes"], 48);

    let config = Settings {
        warmup: 1,
        ..settings(3)
    };
    let (first, mut first_peer) = tokio::io::duplex(64);
    let (failed, failed_peer) = tokio::io::duplex(64);
    let (last, mut last_peer) = tokio::io::duplex(64);
    drop(failed_peer);
    let echo = tokio::spawn(async move {
        let mut payload = [0; 8];
        first_peer.read_exact(&mut payload).await?;
        first_peer.write_all(&payload).await?;
        ensure!(
            first_peer.read(&mut payload).await? == 0,
            "prepared Pipe did not close"
        );
        ensure!(
            last_peer.read(&mut payload).await? == 0,
            "unprepared Pipe did not close"
        );
        Ok::<_, anyhow::Error>(())
    });
    let result = timeout(
        Duration::from_secs(2),
        measure(vec![first, failed, last], Vec::new(), config),
    )
    .await?;
    assert!(result.is_err());
    timeout(Duration::from_secs(2), echo).await???;
    Ok(())
}

#[tokio::test]
async fn cross_pipe_echo_is_reported_as_failed_exchange() -> anyhow::Result<()> {
    let config = Settings {
        samples_per_pipe: 1,
        payload_bytes: 1,
        ..settings(1)
    };
    let (pipe, mut peer) = tokio::io::duplex(64);
    let echo = tokio::spawn(async move {
        let mut payload = [0];
        peer.read_exact(&mut payload).await?;
        stamp_payload(&mut payload, 1, 0, Phase::Measurement);
        peer.write_all(&payload).await?;
        Ok::<_, anyhow::Error>(())
    });
    let result = timeout(
        Duration::from_secs(2),
        measure(vec![pipe], Vec::new(), config),
    )
    .await??;
    echo.await??;
    assert_eq!(result["errors"], 1);
    assert_eq!(result["completed_samples_per_pipe"], json!([0]));
    assert_eq!(result["rtt_seconds"]["p50"], Value::Null);
    assert_eq!(result["echo_payload_bytes"], 0);
    Ok(())
}

#[tokio::test]
async fn idle_end_or_data_fails_warmup_and_measurement_and_closes_active_pipe() -> anyhow::Result<()>
{
    for warmup in [0, 1] {
        for send_data in [false, true] {
            let config = Settings {
                warmup,
                idle_pipes: 1,
                ..settings(1)
            };
            let (pipe, mut peer) = tokio::io::duplex(64);
            let (idle, mut idle_peer) = tokio::io::duplex(64);
            let echo = tokio::spawn(async move {
                let mut payload = [0; 8];
                peer.read_exact(&mut payload).await?;
                if send_data {
                    idle_peer.write_all(&[1]).await?;
                } else {
                    drop(idle_peer);
                }
                // The active exchange is held open until the idle monitor cancels it.
                ensure!(
                    peer.read(&mut payload).await? == 0,
                    "active Pipe survived idle failure"
                );
                Ok::<_, anyhow::Error>(())
            });
            let result = timeout(
                Duration::from_secs(2),
                measure(vec![pipe], vec![idle], config),
            )
            .await?;
            assert!(result.is_err());
            if let Err(error) = result {
                assert!(error.to_string().contains("idle Pipe 0"));
                assert!(error.to_string().contains(if send_data {
                    "unexpected DATA"
                } else {
                    "closed"
                }));
            }
            timeout(Duration::from_secs(2), echo).await???;
        }
    }
    Ok(())
}

#[tokio::test]
async fn successful_measurement_closes_idle_monitors() -> anyhow::Result<()> {
    let config = Settings {
        samples_per_pipe: 1,
        idle_pipes: 1,
        ..settings(1)
    };
    let (pipe, mut peer) = tokio::io::duplex(64);
    let (idle, mut idle_peer) = tokio::io::duplex(64);
    let echo = tokio::spawn(async move {
        let mut payload = [0; 8];
        peer.read_exact(&mut payload).await?;
        peer.write_all(&payload).await?;
        Ok::<_, anyhow::Error>(())
    });
    let result = timeout(
        Duration::from_secs(2),
        measure(vec![pipe], vec![idle], config),
    )
    .await??;
    echo.await??;
    assert_eq!(result["idle_pipes"], 1);
    assert_eq!(result["completed_samples"], 1);
    assert_eq!(
        timeout(Duration::from_secs(2), idle_peer.read(&mut [0; 1])).await??,
        0
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_warmup_echo_cannot_complete_a_measured_sample() -> anyhow::Result<()> {
    for payload_bytes in [1, 8] {
        let config = Settings {
            warmup: 1,
            samples_per_pipe: 1,
            payload_bytes,
            ..settings(1)
        };
        let (pipe, mut peer) = tokio::io::duplex(64);
        let echo = tokio::spawn(async move {
            let mut warmup = vec![0; payload_bytes];
            peer.read_exact(&mut warmup).await?;
            peer.write_all(&warmup).await?;
            peer.write_all(&warmup).await?;
            let mut measured = vec![0; payload_bytes];
            peer.read_exact(&mut measured).await?;
            // Deliberately never echo the measured request.
            Ok::<_, anyhow::Error>(())
        });
        let result = timeout(
            Duration::from_secs(2),
            measure(vec![pipe], Vec::new(), config),
        )
        .await??;
        echo.await??;
        assert_eq!(result["completed_samples"], 0);
        assert_eq!(result["errors"], 1);
    }
    Ok(())
}

#[tokio::test]
async fn idle_failure_ready_at_measurement_completion_is_retained() -> anyhow::Result<()> {
    let config = Settings {
        samples_per_pipe: 1,
        idle_pipes: 1,
        ..settings(1)
    };
    let (pipe, mut peer) = tokio::io::duplex(64);
    let (idle, idle_peer) = tokio::io::duplex(64);
    let measurement = measure(vec![pipe], vec![idle], config);
    tokio::pin!(measurement);
    // Start the workers, but do not poll the collector again until both events are ready.
    std::future::poll_fn(|cx| {
        assert!(measurement.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let mut payload = [0; 8];
    timeout(Duration::from_secs(2), peer.read_exact(&mut payload)).await??;
    peer.write_all(&payload).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), peer.read(&mut [0; 1])).await??,
        0
    );
    // On this current-thread runtime, the monitor cannot consume EOF before the
    // collector's next poll sees the already-completed active worker.
    drop(idle_peer);
    let result = timeout(Duration::from_secs(2), measurement).await?;
    assert!(result.is_err());
    if let Err(error) = result {
        assert!(error.to_string().contains("idle Pipe 0 closed"));
    }
    Ok(())
}
