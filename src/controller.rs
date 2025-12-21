use crate::eapi_bus;
/// Controller methods and structures
use eva_common::actions::{self, ACTION_TOPIC};
use eva_common::events::{RAW_STATE_TOPIC, RawStateEventOwned};
use eva_common::op::Op;
use eva_common::payload::pack;
use eva_common::prelude::*;
use parking_lot::Mutex;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use ttl_cache::TtlCache;
use uuid::Uuid;

const RAW_STATE_CACHE_MAX_CAPACITY: usize = 1_000_000;

#[derive(Clone)]
pub struct RawStateEventPreparedOwned {
    state: RawStateEventOwned,
    delta: Option<f64>,
}

impl RawStateEventPreparedOwned {
    #[inline]
    pub fn from_rse_owned(state: RawStateEventOwned, delta: Option<f64>) -> Self {
        Self { state, delta }
    }
    #[inline]
    pub fn delta(&self) -> Option<f64> {
        self.delta
    }
    #[inline]
    pub fn state(&self) -> &RawStateEventOwned {
        &self.state
    }
    #[inline]
    pub fn state_mut(&mut self) -> &mut RawStateEventOwned {
        &mut self.state
    }
    pub fn is_modified(&self, prev: &RawStateEventOwned) -> bool {
        if self.state.force == eva_common::events::Force::None && self.state.status == prev.status {
            if self.state.value == prev.value {
                return false;
            }
            if let Some(delta_v) = self.delta {
                if let ValueOptionOwned::Value(ref prev_value) = prev.value {
                    if let ValueOptionOwned::Value(ref current_value) = self.state.value {
                        if let Ok(prev_value_f) = TryInto::<f64>::try_into(prev_value.clone()) {
                            if let Ok(current_value_f) =
                                TryInto::<f64>::try_into(current_value.clone())
                            {
                                if (current_value_f - prev_value_f).abs() < delta_v {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
        }
        true
    }
}

impl From<RawStateEventPreparedOwned> for RawStateEventOwned {
    fn from(s: RawStateEventPreparedOwned) -> Self {
        s.state
    }
}

pub struct RawStateCache {
    cache: Mutex<TtlCache<OID, RawStateEventPreparedOwned>>,
    ttl: Option<Duration>,
}

impl RawStateCache {
    pub fn new(ttl: Option<Duration>) -> Self {
        Self {
            cache: Mutex::new(TtlCache::new(RAW_STATE_CACHE_MAX_CAPACITY)),
            ttl,
        }
    }
    /// returns true if no ttl set or the state is modified, false if the event is not required to
    /// be sent
    ///
    /// # Panics
    ///
    /// Will panic if the mutex is poisoned
    pub fn push_check(
        &self,
        oid: &OID,
        raw_state: &RawStateEventOwned,
        delta: Option<f64>,
    ) -> bool {
        if let Some(ttl) = self.ttl {
            let mut cache = self.cache.lock();
            if let Some(v) = cache.get(oid) {
                if !v.is_modified(raw_state) {
                    return false;
                }
            }
            cache.insert(
                oid.clone(),
                RawStateEventPreparedOwned::from_rse_owned(raw_state.clone(), delta),
                ttl,
            );
            true
        } else {
            false
        }
    }
    /// Removes from the state hashmap, these who are not required to be sent, caches
    /// remaining
    ///
    /// # Panics
    ///
    /// Will panic if the mutex is poisoned
    pub fn retain_map_modified(&self, states: &mut HashMap<&OID, RawStateEventPreparedOwned>) {
        if let Some(ttl) = self.ttl {
            let mut cache = self.cache.lock();
            states.retain(|oid, raw| {
                if let Some(cached) = cache.get(oid) {
                    cached.is_modified(raw.state())
                } else {
                    true
                }
            });
            // cache kept ones
            for (oid, raw) in states {
                cache.insert((*oid).clone(), raw.clone(), ttl);
            }
        }
    }
}

#[path = "actt.rs"]
pub mod actt;
pub use eva_common::transform;

pub const ERR_NO_PARAMS: &str = "action params not specified";

#[inline]
pub fn format_action_topic(oid: &OID) -> String {
    format!("{}{}", ACTION_TOPIC, oid.as_path())
}

#[inline]
pub fn format_raw_state_topic(oid: &OID) -> String {
    format!("{}{}", RAW_STATE_TOPIC, oid.as_path())
}

#[derive(Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ItemProp {
    Status,
    Value,
}

/// Controller action object
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Action {
    uuid: Uuid,
    i: OID,
    #[serde(deserialize_with = "eva_common::tools::deserialize_duration_from_micros")]
    timeout: Duration,
    priority: u8,
    params: Option<actions::Params>,
    config: Option<Value>,
    #[serde(skip, default = "Instant::now")]
    received: Instant,
}

impl Action {
    pub async fn publish_event_pending(&self) -> EResult<()> {
        let event = self.event_pending();
        eapi_bus::publish(&format_action_topic(&self.i), pack(&event)?.into()).await?;
        Ok(())
    }
    pub async fn publish_event_running(&self) -> EResult<()> {
        let event = self.event_running();
        eapi_bus::publish(&format_action_topic(&self.i), pack(&event)?.into()).await?;
        Ok(())
    }
    pub async fn publish_event_completed(&self, out: Option<Value>) -> EResult<()> {
        let event = self.event_completed(out);
        eapi_bus::publish(&format_action_topic(&self.i), pack(&event)?.into()).await?;
        Ok(())
    }
    pub async fn publish_event_failed(
        &self,
        exitcode: i16,
        out: Option<Value>,
        err: Option<Value>,
    ) -> EResult<()> {
        let event = self.event_failed(exitcode, out, err);
        eapi_bus::publish(&format_action_topic(&self.i), pack(&event)?.into()).await?;
        Ok(())
    }
    pub async fn publish_event_canceled(&self) -> EResult<()> {
        let event = self.event_canceled();
        eapi_bus::publish(&format_action_topic(&self.i), pack(&event)?.into()).await?;
        Ok(())
    }
    pub async fn publish_event_terminated(&self) -> EResult<()> {
        let event = self.event_terminated();
        eapi_bus::publish(&format_action_topic(&self.i), pack(&event)?.into()).await?;
        Ok(())
    }
    pub fn event_pending(&self) -> actions::ActionEvent {
        actions::ActionEvent {
            uuid: self.uuid,
            status: actions::Status::Pending as u8,
            out: None,
            err: None,
            exitcode: None,
        }
    }
    pub fn event_running(&self) -> actions::ActionEvent {
        actions::ActionEvent {
            uuid: self.uuid,
            status: actions::Status::Running as u8,
            out: None,
            err: None,
            exitcode: None,
        }
    }
    pub fn event_completed(&self, out: Option<Value>) -> actions::ActionEvent {
        actions::ActionEvent {
            uuid: self.uuid,
            status: actions::Status::Completed as u8,
            out,
            err: None,
            exitcode: Some(0),
        }
    }
    pub fn event_failed(
        &self,
        exitcode: i16,
        out: Option<Value>,
        err: Option<Value>,
    ) -> actions::ActionEvent {
        actions::ActionEvent {
            uuid: self.uuid,
            status: actions::Status::Failed as u8,
            out,
            err,
            exitcode: Some(exitcode),
        }
    }
    pub fn event_canceled(&self) -> actions::ActionEvent {
        actions::ActionEvent {
            uuid: self.uuid,
            status: actions::Status::Canceled as u8,
            out: None,
            err: None,
            exitcode: None,
        }
    }
    pub fn event_terminated(&self) -> actions::ActionEvent {
        actions::ActionEvent {
            uuid: self.uuid,
            status: actions::Status::Terminated as u8,
            out: None,
            err: None,
            exitcode: Some(-15),
        }
    }
    #[inline]
    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }
    #[inline]
    pub fn oid(&self) -> &OID {
        &self.i
    }
    #[inline]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    #[inline]
    pub fn priority(&self) -> u8 {
        self.priority
    }
    #[inline]
    pub fn params(&self) -> Option<&actions::Params> {
        self.params.as_ref()
    }
    #[inline]
    pub fn take_params(&mut self) -> Option<actions::Params> {
        self.params.take()
    }
    pub fn take_unit_params(&mut self) -> EResult<actions::UnitParams> {
        if let Some(params) = self.params.take() {
            match params {
                eva_common::actions::Params::Unit(p) => Ok(p),
                eva_common::actions::Params::Lmacro(_) => Err(Error::not_implemented(
                    "can not exec lmacro action with unit",
                )),
            }
        } else {
            Err(Error::invalid_data(ERR_NO_PARAMS))
        }
    }
    pub fn take_lmacro_params(&mut self) -> EResult<actions::LmacroParams> {
        if let Some(params) = self.params.take() {
            match params {
                eva_common::actions::Params::Lmacro(p) => Ok(p),
                eva_common::actions::Params::Unit(_) => Err(Error::not_implemented(
                    "can not exec unit action with lmacro",
                )),
            }
        } else {
            Err(Error::invalid_data(ERR_NO_PARAMS))
        }
    }
    #[inline]
    pub fn config(&self) -> Option<&Value> {
        self.config.as_ref()
    }
    #[inline]
    pub fn take_config(&mut self) -> Option<Value> {
        self.config.take()
    }
    #[inline]
    pub fn op(&self) -> Op {
        Op::for_instant(self.received, self.timeout)
    }
}
