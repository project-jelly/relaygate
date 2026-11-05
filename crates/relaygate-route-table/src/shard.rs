use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::{Duration, Instant},
};

use crate::{
    BindingIdentity, BindingProjection, BindingSet, BindingSnapshot, Destination, LeaseId,
    RegistrationAck, RegistrationKey, RegistrationRevision, RequestContext, RouteTableError,
    RouteTableStats, ShardDirectory, ShardDirectoryGeneration, ShardId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteTableConfig {
    lease_ttl: Duration,
}

impl RouteTableConfig {
    pub fn new(lease_ttl: Duration) -> Result<Self, RouteTableError> {
        if lease_ttl.is_zero() {
            return Err(RouteTableError::InvalidArgument(
                "lease TTL must be greater than zero".to_owned(),
            ));
        }
        Ok(Self { lease_ttl })
    }

    #[must_use]
    pub const fn lease_ttl(self) -> Duration {
        self.lease_ttl
    }
}

#[derive(Debug)]
struct RegistrationState {
    lease_id: LeaseId,
    revision: Option<RegistrationRevision>,
    deadline: Instant,
    bindings: BTreeMap<BindingIdentity, BindingProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpiryKey {
    registration_key: RegistrationKey,
    lease_id: LeaseId,
}

/// One READY, memory-only logical RouteTable shard.
#[derive(Debug)]
pub struct RouteTableShard {
    directory: ShardDirectory,
    shard_id: ShardId,
    config: RouteTableConfig,
    destination_index: HashMap<Destination, BTreeMap<BindingIdentity, BindingProjection>>,
    registration_index: HashMap<RegistrationKey, RegistrationState>,
    active_lease_ids: HashSet<LeaseId>,
    expiry_index: BTreeMap<Instant, BTreeSet<ExpiryKey>>,
    binding_count: usize,
    expiry_record_count: usize,
}

impl RouteTableShard {
    pub fn new(
        directory: ShardDirectory,
        shard_id: ShardId,
        config: RouteTableConfig,
    ) -> Result<Self, RouteTableError> {
        if directory.shard(&shard_id).is_none() {
            return Err(RouteTableError::InvalidArgument(
                "RouteTable shard is not present in ShardDirectory".to_owned(),
            ));
        }
        Ok(Self {
            directory,
            shard_id,
            config,
            destination_index: HashMap::new(),
            registration_index: HashMap::new(),
            active_lease_ids: HashSet::new(),
            expiry_index: BTreeMap::new(),
            binding_count: 0,
            expiry_record_count: 0,
        })
    }

    #[must_use]
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    #[must_use]
    pub const fn generation(&self) -> ShardDirectoryGeneration {
        self.directory.generation()
    }

    #[must_use]
    pub fn stats(&self) -> RouteTableStats {
        RouteTableStats {
            registration_count: self.registration_index.len(),
            binding_count: self.binding_count,
            destination_count: self.destination_index.len(),
            expiry_record_count: self.expiry_record_count,
        }
    }

    pub fn register(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: RegistrationKey,
        now: Instant,
    ) -> Result<RegistrationAck, RouteTableError> {
        self.validate_authenticated_owner(context, &key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(&key)?;
        self.expire_due(now);

        if let Some(registration) = self.registration_index.get(&key) {
            return Ok(Self::ack(registration, now));
        }

        let deadline = Self::deadline_from(self.config.lease_ttl, now)?;
        let lease_id = self.unique_lease_id();
        self.registration_index.insert(
            key.clone(),
            RegistrationState {
                lease_id,
                revision: None,
                deadline,
                bindings: BTreeMap::new(),
            },
        );
        self.active_lease_ids.insert(lease_id);
        self.insert_expiry(&key, lease_id, deadline);

        Ok(RegistrationAck::new(lease_id, None, self.config.lease_ttl))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        revision: RegistrationRevision,
        snapshot: BindingSnapshot,
        now: Instant,
    ) -> Result<RegistrationAck, RouteTableError> {
        self.validate_authenticated_owner(context, key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(key)?;
        self.validate_snapshot(key, &snapshot)?;
        self.expire_due(now);

        let registration = Self::current_registration(&mut self.registration_index, key, lease_id)?;
        match registration.revision {
            None if revision != RegistrationRevision::FIRST => {
                return Err(RouteTableError::FailedPrecondition(
                    "the first accepted RegistrationRevision must be 1".to_owned(),
                ));
            }
            Some(current) if revision == current => {
                if registration.bindings == *snapshot.as_map() {
                    return Ok(Self::ack(registration, now));
                }
                return Err(RouteTableError::FailedPrecondition(
                    "the same RegistrationRevision has a different snapshot".to_owned(),
                ));
            }
            Some(current) if revision < current => {
                return Err(RouteTableError::FailedPrecondition(
                    "RegistrationRevision is lower than the accepted revision".to_owned(),
                ));
            }
            None | Some(_) => {}
        }
        Self::validate_current_binding_identity_stability(registration, &snapshot)?;

        let old_bindings = std::mem::replace(&mut registration.bindings, snapshot.into_map());
        self.binding_count -= old_bindings.len();
        self.binding_count += registration.bindings.len();
        registration.revision = Some(revision);
        let deadline = registration.deadline;
        Self::remove_bindings(&mut self.destination_index, &old_bindings);
        Self::insert_bindings(&mut self.destination_index, &registration.bindings);

        Ok(RegistrationAck::new(
            lease_id,
            Some(revision),
            deadline.saturating_duration_since(now),
        ))
    }

    pub fn keep_alive(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        now: Instant,
    ) -> Result<RegistrationAck, RouteTableError> {
        self.validate_authenticated_owner(context, key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(key)?;
        self.expire_due(now);

        let registration = Self::current_registration(&mut self.registration_index, key, lease_id)?;
        let new_deadline = Self::deadline_from(self.config.lease_ttl, now)?;
        let old_deadline = std::mem::replace(&mut registration.deadline, new_deadline);
        let revision = registration.revision;
        self.remove_expiry(key, lease_id, old_deadline);
        self.insert_expiry(key, lease_id, new_deadline);

        Ok(RegistrationAck::new(
            lease_id,
            revision,
            self.config.lease_ttl,
        ))
    }

    pub fn deregister(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        now: Instant,
    ) -> Result<(), RouteTableError> {
        self.validate_authenticated_owner(context, key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(key)?;
        self.expire_due(now);

        let Some(registration) = self.registration_index.get(key) else {
            return Ok(());
        };
        if registration.lease_id != lease_id {
            return Err(RouteTableError::FailedPrecondition(
                "LeaseId is not the current active lease".to_owned(),
            ));
        }
        self.remove_registration(key);
        Ok(())
    }

    pub fn resolve(
        &mut self,
        _context: RequestContext,
        generation: ShardDirectoryGeneration,
        destination: &Destination,
        now: Instant,
    ) -> Result<BindingSet, RouteTableError> {
        self.validate_generation(generation)?;
        self.validate_destination_authority(destination)?;
        self.expire_due(now);

        let bindings = self
            .destination_index
            .get(destination)
            .ok_or(RouteTableError::NotFound)?;
        if bindings.is_empty() {
            return Err(RouteTableError::NotFound);
        }
        Ok(BindingSet::new(bindings.values().cloned().collect()))
    }

    /// Returns the earliest active lease deadline, if one exists.
    ///
    /// Runtime adapters use this monotonic deadline to drive expiry even when
    /// no RouteTable request arrives. The deadline is operational scheduling
    /// state, not a stable identity and must not be sent over the wire.
    #[must_use]
    pub fn next_expiry_deadline(&self) -> Option<Instant> {
        self.expiry_index
            .first_key_value()
            .map(|(deadline, _)| *deadline)
    }

    /// Removes all registrations whose current deadline is at or before `now`.
    pub fn expire_due(&mut self, now: Instant) -> usize {
        let mut expired = 0;
        while self
            .expiry_index
            .first_key_value()
            .is_some_and(|(deadline, _)| *deadline <= now)
        {
            let Some((_, candidates)) = self.expiry_index.pop_first() else {
                break;
            };
            self.expiry_record_count -= candidates.len();
            for candidate in candidates {
                let is_current = self
                    .registration_index
                    .get(&candidate.registration_key)
                    .is_some_and(|registration| {
                        registration.lease_id == candidate.lease_id && registration.deadline <= now
                    });
                if is_current {
                    self.remove_registration(&candidate.registration_key);
                    expired += 1;
                }
            }
        }
        expired
    }

    fn validate_generation(
        &self,
        generation: ShardDirectoryGeneration,
    ) -> Result<(), RouteTableError> {
        if generation != self.directory.generation() {
            return Err(RouteTableError::FailedPrecondition(
                "ShardDirectoryGeneration mismatch".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_authenticated_owner(
        &self,
        context: RequestContext,
        key: &RegistrationKey,
    ) -> Result<(), RouteTableError> {
        if context.authenticated_gateway_id().gateway_id() != key.gateway_id() {
            return Err(RouteTableError::PermissionDenied);
        }
        Ok(())
    }

    fn validate_registration_scope(&self, key: &RegistrationKey) -> Result<(), RouteTableError> {
        if key.shard_id() != &self.shard_id {
            return Err(RouteTableError::InvalidArgument(
                "RegistrationKey targets a different shard".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_snapshot(
        &self,
        key: &RegistrationKey,
        snapshot: &BindingSnapshot,
    ) -> Result<(), RouteTableError> {
        for binding in snapshot.entries() {
            let identity = binding.identity();
            if identity.gateway_id() != key.gateway_id()
                || identity.relay_session_id() != key.relay_session_id()
            {
                return Err(RouteTableError::InvalidArgument(
                    "snapshot binding is outside the RegistrationKey scope".to_owned(),
                ));
            }
            self.validate_destination_authority(binding.destination())?;
        }
        Ok(())
    }

    fn validate_current_binding_identity_stability(
        registration: &RegistrationState,
        snapshot: &BindingSnapshot,
    ) -> Result<(), RouteTableError> {
        for (identity, next) in snapshot.as_map() {
            if registration
                .bindings
                .get(identity)
                .is_some_and(|current| current != next)
            {
                return Err(RouteTableError::FailedPrecondition(
                    "an active BindingIdentity cannot change Destination or GatewayLocator"
                        .to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_destination_authority(
        &self,
        destination: &Destination,
    ) -> Result<(), RouteTableError> {
        if self.directory.authority(destination).id() != &self.shard_id {
            return Err(RouteTableError::InvalidArgument(
                "Destination belongs to a different authority shard".to_owned(),
            ));
        }
        Ok(())
    }

    /// Looks up the active registration for `key` and checks that `lease_id`
    /// is its current lease. Takes the index rather than `&mut self` so callers
    /// can keep the registration borrowed while they update the sibling
    /// `destination_index`/`config` fields.
    fn current_registration<'a>(
        registration_index: &'a mut HashMap<RegistrationKey, RegistrationState>,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Result<&'a mut RegistrationState, RouteTableError> {
        let registration = registration_index
            .get_mut(key)
            .ok_or_else(Self::lease_not_active)?;
        Self::check_lease(registration, lease_id)?;
        Ok(registration)
    }

    fn lease_not_active() -> RouteTableError {
        RouteTableError::FailedPrecondition("registration lease is not active".to_owned())
    }

    fn check_lease(
        registration: &RegistrationState,
        lease_id: LeaseId,
    ) -> Result<(), RouteTableError> {
        if registration.lease_id != lease_id {
            return Err(RouteTableError::FailedPrecondition(
                "LeaseId is not the current active lease".to_owned(),
            ));
        }
        Ok(())
    }

    fn deadline_from(lease_ttl: Duration, now: Instant) -> Result<Instant, RouteTableError> {
        now.checked_add(lease_ttl)
            .ok_or(RouteTableError::DeadlineOverflow)
    }

    fn unique_lease_id(&self) -> LeaseId {
        loop {
            let candidate = LeaseId::new();
            if !self.active_lease_ids.contains(&candidate) {
                return candidate;
            }
        }
    }

    fn ack(registration: &RegistrationState, now: Instant) -> RegistrationAck {
        RegistrationAck::new(
            registration.lease_id,
            registration.revision,
            registration.deadline.saturating_duration_since(now),
        )
    }

    fn insert_expiry(&mut self, key: &RegistrationKey, lease_id: LeaseId, deadline: Instant) {
        if self
            .expiry_index
            .entry(deadline)
            .or_default()
            .insert(ExpiryKey {
                registration_key: key.clone(),
                lease_id,
            })
        {
            self.expiry_record_count += 1;
        }
    }

    fn remove_expiry(&mut self, key: &RegistrationKey, lease_id: LeaseId, deadline: Instant) {
        let expiry_key = ExpiryKey {
            registration_key: key.clone(),
            lease_id,
        };
        let remove_bucket = if let Some(bucket) = self.expiry_index.get_mut(&deadline) {
            if bucket.remove(&expiry_key) {
                self.expiry_record_count -= 1;
            }
            bucket.is_empty()
        } else {
            false
        };
        if remove_bucket {
            self.expiry_index.remove(&deadline);
        }
    }

    fn remove_registration(&mut self, key: &RegistrationKey) {
        let Some(registration) = self.registration_index.remove(key) else {
            return;
        };
        self.binding_count -= registration.bindings.len();
        self.active_lease_ids.remove(&registration.lease_id);
        self.remove_expiry(key, registration.lease_id, registration.deadline);
        Self::remove_bindings(&mut self.destination_index, &registration.bindings);
    }

    fn insert_bindings(
        destination_index: &mut HashMap<Destination, BTreeMap<BindingIdentity, BindingProjection>>,
        bindings: &BTreeMap<BindingIdentity, BindingProjection>,
    ) {
        for (identity, binding) in bindings {
            destination_index
                .entry(binding.destination().clone())
                .or_default()
                .insert(*identity, binding.clone());
        }
    }

    fn remove_bindings(
        destination_index: &mut HashMap<Destination, BTreeMap<BindingIdentity, BindingProjection>>,
        bindings: &BTreeMap<BindingIdentity, BindingProjection>,
    ) {
        for (identity, binding) in bindings {
            let remove_destination =
                if let Some(bindings) = destination_index.get_mut(binding.destination()) {
                    bindings.remove(identity);
                    bindings.is_empty()
                } else {
                    false
                };
            if remove_destination {
                destination_index.remove(binding.destination());
            }
        }
    }
}

#[cfg(test)]
mod tests;
