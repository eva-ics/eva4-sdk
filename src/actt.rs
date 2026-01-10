use eva_common::prelude::*;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use uuid::Uuid;

/// Action termination helper for EVA ICS controller services
pub struct Actt {
    pending_by_oid: HashMap<OID, Mutex<HashMap<Uuid, PendingAction>>>,
    pending_by_uuid: Mutex<HashMap<Uuid, PendingAction>>,
}

impl Actt {
    /// Initialize a new helper object
    ///
    /// All OIDS, managed by the controller must be provided
    pub fn new(oids: &[&OID]) -> Self {
        let mut pending_by_oid = HashMap::new();
        for oid in oids {
            pending_by_oid.insert((*oid).clone(), <_>::default());
        }
        Self {
            pending_by_oid,
            pending_by_uuid: <_>::default(),
        }
    }
    /// Register additional OID
    #[inline]
    pub fn register(&mut self, oid: &OID) {
        self.pending_by_oid.insert(oid.clone(), <_>::default());
    }
    /// Unregister OID on-the-flow
    #[inline]
    pub fn unregister(&mut self, oid: &OID) {
        self.pending_by_oid.remove(oid);
    }
    /// Append new action to the helper
    ///
    /// # Panics
    ///
    /// Will panic if the internal mutexes are poisoned
    pub fn append(&self, oid: &OID, uuid: Uuid) -> EResult<()> {
        if let Some(actions) = self.pending_by_oid.get(oid) {
            let mut a = actions.lock();
            let mut a_uuid = self.pending_by_uuid.lock();
            if let Entry::Vacant(o) = a.entry(uuid) {
                o.insert(PendingAction::new());
                a_uuid.insert(uuid, PendingAction::new());
                Ok(())
            } else {
                Err(Error::core("duplicate action UUID"))
            }
        } else {
            Err(Error::core(format!(
                "{} is not in PENDING_ACTIONS map",
                oid
            )))
        }
    }
    /// Remove action from the helper
    ///
    /// If action was appended, it MUST be always removed either by RPC function (if failed to send
    /// the action to handler) or by the handler
    ///
    /// The handler checks the returned boolean and marks action canceled in case of false
    ///
    /// After removing, the action no longer can be terminated
    ///
    /// # Panics
    ///
    /// Will panic if the internal mutexes are poisoned
    pub fn remove(&self, oid: &OID, uuid: &Uuid) -> EResult<bool> {
        if let Some(actions) = self.pending_by_oid.get(oid) {
            let mut a = actions.lock();
            let mut a_uuid = self.pending_by_uuid.lock();
            if let Some(v) = a.remove(uuid) {
                if let Some(v_u) = a_uuid.remove(uuid) {
                    Ok(v.is_active() && v_u.is_active())
                } else {
                    Ok(v.is_active())
                }
            } else {
                Err(Error::not_found("action not found in PENDING_ACTIONS map"))
            }
        } else {
            Err(Error::core(format!(
                "{} is not in PENDING_ACTIONS map",
                oid
            )))
        }
    }
    /// The handler can check is the action active during its execution
    ///
    /// # Panics
    ///
    /// Will panic if the internal mutexes are poisoned
    pub fn is_active(&self, oid: &OID, uuid: &Uuid) -> EResult<bool> {
        if let Some(actions) = self.pending_by_oid.get(oid) {
            let a = actions.lock();
            let a_uuid = self.pending_by_uuid.lock();
            if let Some(v) = a.get(uuid) {
                if let Some(v_u) = a_uuid.get(uuid) {
                    Ok(v.is_active() && v_u.is_active())
                } else {
                    Ok(v.is_active())
                }
            } else {
                Err(Error::not_found("action not found in PENDING_ACTIONS map"))
            }
        } else {
            Err(Error::core(format!(
                "{} is not in PENDING_ACTIONS map",
                oid
            )))
        }
    }
    /// Mark the action terminated by uuid
    ///
    /// # Panics
    ///
    /// Will panic if the internal mutexes are poisoned
    pub fn mark_terminated(&self, uuid: &Uuid) -> EResult<()> {
        let mut a_uuid = self.pending_by_uuid.lock();
        if let Some(v_u) = a_uuid.get_mut(uuid) {
            v_u.cancel();
            Ok(())
        } else {
            Err(Error::not_found(format!("action {} not found", uuid)))
        }
    }
    /// Mark all actions pending for OID terminated
    ///
    /// # Panics
    ///
    /// Will panic if the internal mutexes are poisoned
    pub fn mark_killed(&self, oid: &OID) -> EResult<()> {
        if let Some(actions) = self.pending_by_oid.get(oid) {
            for a in actions.lock().values_mut() {
                a.cancel();
            }
            Ok(())
        } else {
            Err(Error::core(format!(
                "{} is not in PENDING_ACTIONS map",
                oid
            )))
        }
    }
}

struct PendingAction {
    active: bool,
}

impl PendingAction {
    #[inline]
    fn new() -> Self {
        Self { active: true }
    }
    #[inline]
    fn is_active(&self) -> bool {
        self.active
    }
    #[inline]
    fn cancel(&mut self) {
        self.active = false;
    }
}
