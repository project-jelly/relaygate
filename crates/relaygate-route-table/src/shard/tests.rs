use super::*;
use crate::{AuthenticatedGatewayId, BindingId, GatewayId, GatewayLocator, RelaySessionId};
use uuid::Uuid;

fn shard() -> Result<RouteTableShard, RouteTableError> {
    RouteTableShard::new(
        ShardDirectory::from_json_bytes(
            br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-0","endpoint":"http://rt-0:8080"}]}"#,
        )?,
        ShardId::new("rt-0")?,
        RouteTableConfig::new(Duration::from_secs(30))?,
    )
}

fn registration(id: u128) -> Result<(RequestContext, RegistrationKey), RouteTableError> {
    let gateway = GatewayId::from_uuid(Uuid::from_u128(1));
    Ok((
        RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway)),
        RegistrationKey::new(
            gateway,
            RelaySessionId::from_uuid(Uuid::from_u128(id)),
            ShardId::new("rt-0")?,
        ),
    ))
}

fn binding(
    key: &RegistrationKey,
    id: u128,
    name: &str,
) -> Result<BindingProjection, RouteTableError> {
    Ok(BindingProjection::new(
        format!("test/{name}").parse()?,
        key.gateway_id(),
        key.relay_session_id(),
        BindingId::from_uuid(Uuid::from_u128(id)),
        GatewayLocator::new("gateway-a")?,
    ))
}

fn assert_stats(shard: &RouteTableShard, expected: [usize; 4]) {
    let scanned = RouteTableStats {
        registration_count: shard.registration_index.len(),
        binding_count: shard
            .registration_index
            .values()
            .map(|registration| registration.bindings.len())
            .sum(),
        destination_count: shard.destination_index.len(),
        expiry_record_count: shard.expiry_index.values().map(BTreeSet::len).sum(),
    };
    assert_eq!(shard.stats(), scanned);
    assert_eq!(
        scanned,
        RouteTableStats {
            registration_count: expected[0],
            binding_count: expected[1],
            destination_count: expected[2],
            expiry_record_count: expected[3],
        }
    );
}

#[test]
fn stats_match_full_scan_through_snapshot_replacement_and_deregistration()
-> Result<(), RouteTableError> {
    let mut shard = shard()?;
    let generation = shard.generation();
    let now = Instant::now();
    let (context, first) = registration(10)?;
    let (_, second) = registration(20)?;
    assert_stats(&shard, [0, 0, 0, 0]);

    let first_lease = shard
        .register(context, generation, first.clone(), now)?
        .lease_id();
    assert_stats(&shard, [1, 0, 0, 1]);
    let second_lease = shard
        .register(context, generation, second.clone(), now)?
        .lease_id();
    assert_stats(&shard, [2, 0, 0, 2]);
    let shared = binding(&first, 100, "shared")?;
    let alpha = binding(&first, 101, "alpha")?;
    let beta = binding(&first, 102, "beta")?;
    let initial = BindingSnapshot::new([shared.clone(), alpha.clone()])?;
    shard.update(
        context,
        generation,
        &first,
        first_lease,
        RegistrationRevision::FIRST,
        initial.clone(),
        now,
    )?;
    assert_stats(&shard, [2, 2, 2, 2]);
    shard.update(
        context,
        generation,
        &second,
        second_lease,
        RegistrationRevision::FIRST,
        BindingSnapshot::new([binding(&second, 200, "shared")?])?,
        now,
    )?;
    assert_stats(&shard, [2, 3, 2, 2]);

    let retry = shard.register(
        context,
        generation,
        first.clone(),
        now + Duration::from_secs(5),
    )?;
    assert_eq!(retry.lease_id(), first_lease);
    assert_eq!(retry.expires_in(), Duration::from_secs(25));
    assert_stats(&shard, [2, 3, 2, 2]);
    shard.update(
        context,
        generation,
        &first,
        first_lease,
        RegistrationRevision::FIRST,
        initial,
        now,
    )?;
    assert_stats(&shard, [2, 3, 2, 2]);

    shard.update(
        context,
        generation,
        &first,
        first_lease,
        RegistrationRevision::new(2)?,
        BindingSnapshot::new([shared.clone()])?,
        now,
    )?;
    assert_stats(&shard, [2, 2, 1, 2]);
    shard.update(
        context,
        generation,
        &first,
        first_lease,
        RegistrationRevision::new(3)?,
        BindingSnapshot::new([shared.clone(), alpha, beta])?,
        now,
    )?;
    assert_stats(&shard, [2, 4, 3, 2]);
    assert_eq!(
        shard
            .resolve(context, generation, shared.destination(), now)?
            .entries()
            .len(),
        2
    );
    assert_stats(&shard, [2, 4, 3, 2]);

    shard.deregister(context, generation, &first, first_lease, now)?;
    assert_stats(&shard, [1, 1, 1, 1]);
    shard.deregister(context, generation, &first, first_lease, now)?;
    assert_stats(&shard, [1, 1, 1, 1]);
    shard.deregister(context, generation, &second, second_lease, now)?;
    assert_stats(&shard, [0, 0, 0, 0]);
    Ok(())
}

#[test]
fn rejected_updates_leave_stats_equal_to_full_scan() -> Result<(), RouteTableError> {
    let mut shard = shard()?;
    let generation = shard.generation();
    let now = Instant::now();
    let (context, key) = registration(10)?;
    let lease = shard
        .register(context, generation, key.clone(), now)?
        .lease_id();
    let alpha = binding(&key, 100, "alpha")?;
    let beta = binding(&key, 101, "beta")?;
    let initial = BindingSnapshot::new([alpha.clone()])?;
    assert!(
        shard
            .update(
                context,
                generation,
                &key,
                lease,
                RegistrationRevision::new(2)?,
                initial.clone(),
                now
            )
            .is_err()
    );
    assert_stats(&shard, [1, 0, 0, 1]);
    shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        initial.clone(),
        now,
    )?;
    assert_stats(&shard, [1, 1, 1, 1]);

    let expanded = BindingSnapshot::new([alpha, beta])?;
    assert!(
        shard
            .update(
                context,
                generation,
                &key,
                lease,
                RegistrationRevision::FIRST,
                expanded.clone(),
                now
            )
            .is_err()
    );
    assert_stats(&shard, [1, 1, 1, 1]);
    shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::new(2)?,
        expanded.clone(),
        now,
    )?;
    assert_stats(&shard, [1, 2, 2, 1]);
    assert!(
        shard
            .update(
                context,
                generation,
                &key,
                lease,
                RegistrationRevision::FIRST,
                initial,
                now
            )
            .is_err()
    );
    assert_stats(&shard, [1, 2, 2, 1]);
    let relocated = BindingSnapshot::new([binding(&key, 100, "relocated")?])?;
    assert!(
        shard
            .update(
                context,
                generation,
                &key,
                lease,
                RegistrationRevision::new(3)?,
                relocated,
                now
            )
            .is_err()
    );
    assert_stats(&shard, [1, 2, 2, 1]);

    let wrong_lease = LeaseId::new();
    assert!(
        shard
            .update(
                context,
                generation,
                &key,
                wrong_lease,
                RegistrationRevision::new(3)?,
                expanded,
                now
            )
            .is_err()
    );
    assert_stats(&shard, [1, 2, 2, 1]);
    assert!(
        shard
            .keep_alive(context, generation, &key, wrong_lease, now)
            .is_err()
    );
    assert_stats(&shard, [1, 2, 2, 1]);
    assert!(
        shard
            .deregister(context, generation, &key, wrong_lease, now)
            .is_err()
    );
    assert_stats(&shard, [1, 2, 2, 1]);
    Ok(())
}

#[test]
fn stats_match_full_scan_through_renewal_expiry_and_new_lease() -> Result<(), RouteTableError> {
    let mut shard = shard()?;
    let generation = shard.generation();
    let start = Instant::now();
    let (context, first) = registration(10)?;
    let (_, second) = registration(20)?;
    let (_, third) = registration(30)?;
    let mut leases = Vec::new();
    for (index, key) in [&first, &second, &third].into_iter().enumerate() {
        let lease = shard
            .register(context, generation, key.clone(), start)?
            .lease_id();
        assert_stats(
            &shard,
            [index + 1, index, usize::from(index > 0), index + 1],
        );
        shard.update(
            context,
            generation,
            key,
            lease,
            RegistrationRevision::FIRST,
            BindingSnapshot::new([binding(key, 100, "shared")?])?,
            start,
        )?;
        assert_stats(&shard, [index + 1, index + 1, 1, index + 1]);
        leases.push(lease);
    }
    shard.keep_alive(context, generation, &first, leases[0], start)?;
    assert_stats(&shard, [3, 3, 1, 3]);
    shard.keep_alive(
        context,
        generation,
        &first,
        leases[0],
        start + Duration::from_secs(10),
    )?;
    assert_stats(&shard, [3, 3, 1, 3]);
    for _ in 0..100 {
        shard.keep_alive(
            context,
            generation,
            &second,
            leases[1],
            start + Duration::from_secs(20),
        )?;
        assert_stats(&shard, [3, 3, 1, 3]);
    }
    assert_eq!(shard.expire_due(start + Duration::from_secs(30)), 1);
    assert_stats(&shard, [2, 2, 1, 2]);
    assert_eq!(shard.expire_due(start + Duration::from_secs(30)), 0);
    assert_stats(&shard, [2, 2, 1, 2]);
    let destination = "test/shared".parse()?;
    assert_eq!(
        shard
            .resolve(
                context,
                generation,
                &destination,
                start + Duration::from_secs(40)
            )?
            .entries()
            .len(),
        1
    );
    assert_stats(&shard, [1, 1, 1, 1]);

    let expired_at = start + Duration::from_secs(50);
    assert!(
        shard
            .keep_alive(context, generation, &second, leases[1], expired_at)
            .is_err()
    );
    assert_stats(&shard, [0, 0, 0, 0]);
    let replacement = shard
        .register(context, generation, second.clone(), expired_at)?
        .lease_id();
    assert_ne!(replacement, leases[1]);
    assert_stats(&shard, [1, 0, 0, 1]);
    assert!(
        shard
            .deregister(context, generation, &second, leases[1], expired_at)
            .is_err()
    );
    assert_stats(&shard, [1, 0, 0, 1]);
    shard.update(
        context,
        generation,
        &second,
        replacement,
        RegistrationRevision::FIRST,
        BindingSnapshot::new([binding(&second, 200, "replacement")?])?,
        expired_at,
    )?;
    assert_stats(&shard, [1, 1, 1, 1]);
    assert_eq!(shard.expire_due(start + Duration::from_secs(80)), 1);
    assert_stats(&shard, [0, 0, 0, 0]);
    Ok(())
}

#[test]
fn expiry_records_count_bucket_members_and_ignore_duplicate_or_missing_removals()
-> Result<(), RouteTableError> {
    let mut shard = shard()?;
    let generation = shard.generation();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(30);
    let (context, first) = registration(10)?;
    let (_, second) = registration(20)?;
    let first_lease = shard
        .register(context, generation, first.clone(), start)?
        .lease_id();
    let second_lease = shard
        .register(context, generation, second.clone(), start)?
        .lease_id();
    assert_stats(&shard, [2, 0, 0, 2]);
    shard.insert_expiry(&first, first_lease, deadline);
    assert_stats(&shard, [2, 0, 0, 2]);
    let stale_lease = LeaseId::new();
    shard.remove_expiry(&first, stale_lease, deadline);
    assert_stats(&shard, [2, 0, 0, 2]);
    shard.remove_expiry(&first, first_lease, deadline + Duration::from_secs(1));
    assert_stats(&shard, [2, 0, 0, 2]);

    shard.insert_expiry(&first, stale_lease, start + Duration::from_secs(1));
    assert_stats(&shard, [2, 0, 0, 3]);
    assert_eq!(shard.expire_due(start + Duration::from_secs(1)), 0);
    assert_stats(&shard, [2, 0, 0, 2]);
    assert_eq!(shard.expire_due(deadline), 2);
    assert_stats(&shard, [0, 0, 0, 0]);
    shard.remove_expiry(&second, second_lease, deadline);
    assert_stats(&shard, [0, 0, 0, 0]);
    Ok(())
}
