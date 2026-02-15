//! Helper module for EAPI micro-services
use crate::service::{self, EventKind};
use async_trait::async_trait;
use busrt::QoS;
use busrt::client::AsyncClient;
use busrt::rpc::{Rpc, RpcClient, RpcEvent, RpcHandlers};
use eva_common::acl::OIDMask;
use eva_common::common_payloads::ParamsId;
use eva_common::events::{RAW_STATE_TOPIC, RawStateEvent};
use eva_common::payload::{pack, unpack};
use eva_common::prelude::*;
use eva_common::services::Initial;
use eva_common::services::Registry;
use log::error;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeSeq};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

pub const AAA_REPORT_TOPIC: &str = "AAA/REPORT";

static RPC: OnceLock<Arc<RpcClient>> = OnceLock::new();
static RPC_SECONDARY: OnceLock<Arc<RpcClient>> = OnceLock::new();
static REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();
static CLIENT: OnceLock<Arc<Mutex<dyn AsyncClient>>> = OnceLock::new();
static TIMEOUT: OnceLock<Duration> = OnceLock::new();

pub enum LvarCommand<'a> {
    Set {
        status: ItemStatus,
        value: &'a Value,
    },
    Reset,
    Clear,
    Toggle,
    Increment,
    Decrement,
}

impl LvarCommand<'_> {
    pub fn as_str(&self) -> &'static str {
        match self {
            LvarCommand::Set { .. } => "lvar.set",
            LvarCommand::Reset => "lvar.reset",
            LvarCommand::Clear => "lvar.clear",
            LvarCommand::Toggle => "lvar.toggle",
            LvarCommand::Increment => "lvar.incr",
            LvarCommand::Decrement => "lvar.decr",
        }
    }
    /// returns new lvar value for increment/decrement, zero for others
    pub async fn execute(&self, oid: &OID) -> EResult<i64> {
        #[derive(Serialize)]
        struct Payload<'a> {
            i: &'a OID,
            #[serde(skip_serializing_if = "Option::is_none")]
            status: Option<ItemStatus>,
            #[serde(skip_serializing_if = "Option::is_none")]
            value: Option<&'a Value>,
        }
        let payload = Payload {
            i: oid,
            status: match self {
                LvarCommand::Set { status, .. } => Some(*status),
                LvarCommand::Reset
                | LvarCommand::Clear
                | LvarCommand::Toggle
                | LvarCommand::Increment
                | LvarCommand::Decrement => None,
            },
            value: match self {
                LvarCommand::Set { value, .. } => Some(value),
                LvarCommand::Reset
                | LvarCommand::Clear
                | LvarCommand::Toggle
                | LvarCommand::Increment
                | LvarCommand::Decrement => None,
            },
        };
        let res = call("eva.core", self.as_str(), pack(&payload)?.into()).await?;
        match self {
            LvarCommand::Set { .. }
            | LvarCommand::Reset
            | LvarCommand::Clear
            | LvarCommand::Toggle => Ok(0),
            LvarCommand::Increment | LvarCommand::Decrement => {
                let value: i64 = unpack(res.payload())?;
                Ok(value)
            }
        }
    }
}

pub async fn lvar_set(oid: &OID, status: ItemStatus, value: &Value) -> EResult<()> {
    LvarCommand::Set { status, value }.execute(oid).await?;
    Ok(())
}

pub async fn lvar_reset(oid: &OID) -> EResult<()> {
    LvarCommand::Reset.execute(oid).await?;
    Ok(())
}

pub async fn lvar_clear(oid: &OID) -> EResult<()> {
    LvarCommand::Clear.execute(oid).await?;
    Ok(())
}

pub async fn lvar_toggle(oid: &OID) -> EResult<()> {
    LvarCommand::Toggle.execute(oid).await?;
    Ok(())
}

pub async fn lvar_increment(oid: &OID) -> EResult<i64> {
    LvarCommand::Increment.execute(oid).await
}

pub async fn lvar_decrement(oid: &OID) -> EResult<i64> {
    LvarCommand::Decrement.execute(oid).await
}

#[async_trait]
pub trait ClientAccounting {
    async fn report<'a, T>(&self, event: T) -> EResult<()>
    where
        T: TryInto<busrt::borrow::Cow<'a>> + Send;
}

#[async_trait]
impl ClientAccounting for Arc<Mutex<dyn AsyncClient>> {
    /// # Panics
    ///
    /// Will panic if RPC not set
    async fn report<'a, T>(&self, event: T) -> EResult<()>
    where
        T: TryInto<busrt::borrow::Cow<'a>> + Send,
    {
        let payload: busrt::borrow::Cow = event
            .try_into()
            .map_err(|_| Error::invalid_data("Unable to serialize accounting event"))?;
        self.lock()
            .await
            .publish(AAA_REPORT_TOPIC, payload, QoS::Processed)
            .await?;
        Ok(())
    }
}

#[allow(clippy::ref_option)]
fn serialize_opt_uuid_as_seq<S>(uuid: &Option<Uuid>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(u) = uuid {
        let bytes = u.as_bytes();
        let mut seq = serializer.serialize_seq(Some(bytes.len()))?;
        for &byte in bytes {
            seq.serialize_element(&byte)?;
        }
        seq.end()
    } else {
        serializer.serialize_none()
    }
}

fn deserialize_opt_uuid<'de, D>(deserializer: D) -> Result<Option<Uuid>, D::Error>
where
    D: Deserializer<'de>,
{
    let val: Value = Deserialize::deserialize(deserializer)?;
    if val == Value::Unit {
        Ok(None)
    } else {
        Ok(Some(
            Uuid::deserialize(val).map_err(serde::de::Error::custom)?,
        ))
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct AccountingEvent<'a> {
    // the ID is usually assigned by the accounting service and should be None
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_opt_uuid_as_seq",
        deserialize_with = "deserialize_opt_uuid"
    )]
    pub id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub u: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub svc: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subj: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oid: Option<OID>,
    #[serde(default, skip_serializing_if = "Value::is_unit")]
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<&'a str>,
    #[serde(default)]
    pub code: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

impl<'a> TryFrom<AccountingEvent<'a>> for busrt::borrow::Cow<'_> {
    type Error = Error;
    #[inline]
    fn try_from(ev: AccountingEvent<'a>) -> EResult<Self> {
        Ok(busrt::borrow::Cow::Owned(pack(&ev)?))
    }
}

impl<'a> TryFrom<&AccountingEvent<'a>> for busrt::borrow::Cow<'_> {
    type Error = Error;
    #[inline]
    fn try_from(ev: &AccountingEvent<'a>) -> EResult<Self> {
        Ok(busrt::borrow::Cow::Owned(pack(&ev)?))
    }
}

impl<'a> AccountingEvent<'a> {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn user(mut self, user: &'a str) -> Self {
        self.u.replace(user);
        self
    }
    #[inline]
    pub fn src(mut self, src: &'a str) -> Self {
        self.src.replace(src);
        self
    }
    #[inline]
    pub fn svc(mut self, svc: &'a str) -> Self {
        self.svc.replace(svc);
        self
    }
    #[inline]
    pub fn subj(mut self, subj: &'a str) -> Self {
        self.subj.replace(subj);
        self
    }
    #[inline]
    pub fn data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }
    #[inline]
    pub fn note(mut self, note: &'a str) -> Self {
        self.note.replace(note);
        self
    }
    #[inline]
    pub fn code(mut self, code: i16) -> Self {
        self.code = code;
        self
    }
    #[inline]
    pub fn err(mut self, err: impl Into<String>) -> Self {
        self.err.replace(err.into());
        self
    }
    /// # Panics
    ///
    /// Will panic if module RPC not set
    pub async fn report(&self) -> EResult<()> {
        client().report(self).await
    }
}

/// Initializes the module
pub async fn init<H: RpcHandlers + Send + Sync + 'static>(
    initial: &Initial,
    handlers: H,
) -> EResult<Arc<RpcClient>> {
    let rpc = initial.init_rpc(handlers).await?;
    set(rpc.clone(), initial.timeout())?;
    let registry = initial.init_registry(&rpc);
    set_registry(registry)?;
    Ok(rpc)
}

/// Initializes the module in blocking mode with secondary client for calls
pub async fn init_blocking<H: RpcHandlers + Send + Sync + 'static>(
    initial: &Initial,
    handlers: H,
) -> EResult<(Arc<RpcClient>, Arc<RpcClient>)> {
    let (rpc, rpc_secondary) = initial.init_rpc_blocking_with_secondary(handlers).await?;
    set_blocking(rpc.clone(), rpc_secondary.clone(), initial.timeout())?;
    let registry = initial.init_registry(&rpc);
    set_registry(registry)?;
    Ok((rpc, rpc_secondary))
}

/// Manually initialize the module
pub fn set(rpc: Arc<RpcClient>, timeout: Duration) -> EResult<()> {
    CLIENT
        .set(rpc.client())
        .map_err(|_| Error::core("Unable to set CLIENT"))?;
    RPC.set(rpc).map_err(|_| Error::core("Unable to set RPC"))?;
    TIMEOUT
        .set(timeout)
        .map_err(|_| Error::core("Unable to set TIMEOUT"))?;
    Ok(())
}

/// Manually initialize the module
pub fn set_blocking(
    rpc: Arc<RpcClient>,
    rpc_secondary: Arc<RpcClient>,
    timeout: Duration,
) -> EResult<()> {
    set(rpc, timeout)?;
    RPC_SECONDARY
        .set(rpc_secondary)
        .map_err(|_| Error::core("Unable to set RPC_SECONDARY"))?;
    Ok(())
}

/// Manually initialize registry
pub fn set_registry(registry: Registry) -> EResult<()> {
    REGISTRY
        .set(Arc::new(registry))
        .map_err(|_| Error::core("Unable to set REGISTRY"))
}

/// # Panics
///
/// Will panic if RPC not set
pub async fn call0(target: &str, method: &str) -> EResult<RpcEvent> {
    tokio::time::timeout(
        timeout(),
        rpc_secondary().call(target, method, busrt::empty_payload!(), QoS::Processed),
    )
    .await?
    .map_err(Into::into)
}

/// # Panics
///
/// Will panic if RPC not set
pub async fn call(target: &str, method: &str, params: busrt::borrow::Cow<'_>) -> EResult<RpcEvent> {
    tokio::time::timeout(
        timeout(),
        rpc_secondary().call(target, method, params, QoS::Processed),
    )
    .await?
    .map_err(Into::into)
}

/// # Panics
///
/// Will panic if RPC not set
pub async fn call_with_timeout(
    target: &str,
    method: &str,
    params: busrt::borrow::Cow<'_>,
    timeout: Duration,
) -> EResult<RpcEvent> {
    tokio::time::timeout(
        timeout,
        rpc_secondary().call(target, method, params, QoS::Processed),
    )
    .await?
    .map_err(Into::into)
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub fn rpc() -> Arc<RpcClient> {
    RPC.get().cloned().unwrap()
}

/// Returns secondary client if initialized, otherwise fallbacks to the primary
///
/// Must be used for RPC calls when the primary client works in blocking mode
///
/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub fn rpc_secondary() -> Arc<RpcClient> {
    if let Some(rpc) = RPC_SECONDARY.get() {
        rpc.clone()
    } else {
        rpc()
    }
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub fn client() -> Arc<Mutex<dyn AsyncClient>> {
    CLIENT.get().cloned().unwrap()
}

/// Will return the default EVA ICS timeout (5 sec) if not set
#[inline]
pub fn timeout() -> Duration {
    TIMEOUT
        .get()
        .copied()
        .unwrap_or(eva_common::DEFAULT_TIMEOUT)
}

/// # Panics
///
/// Will panic if REGISTRY not set
#[inline]
pub fn registry() -> Arc<Registry> {
    REGISTRY.get().cloned().unwrap()
}

///
/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn subscribe(topic: &str) -> EResult<()> {
    tokio::time::timeout(timeout(), subscribe_impl(topic)).await??;
    Ok(())
}
#[inline]
async fn subscribe_impl(topic: &str) -> EResult<()> {
    let Some(op) = client()
        .lock()
        .await
        .subscribe(topic, QoS::Processed)
        .await?
    else {
        return Ok(());
    };
    op.await??;
    Ok(())
}

///
/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn subscribe_bulk(topics: &[&str]) -> EResult<()> {
    tokio::time::timeout(timeout(), subscribe_bulk_impl(topics)).await??;
    Ok(())
}
#[inline]
async fn subscribe_bulk_impl(topics: &[&str]) -> EResult<()> {
    let Some(op) = client()
        .lock()
        .await
        .subscribe_bulk(topics, QoS::Processed)
        .await?
    else {
        return Ok(());
    };
    op.await??;
    Ok(())
}

#[inline]
pub async fn publish_item_state(
    oid: &OID,
    status: ItemStatus,
    value: Option<&Value>,
) -> EResult<()> {
    let ev = value.map_or_else(
        || RawStateEvent::new0(status),
        |v| RawStateEvent::new(status, v),
    );
    publish_item_state_event(oid, ev).await
}

#[inline]
pub async fn publish_item_state_event(oid: &OID, ev: RawStateEvent<'_>) -> EResult<()> {
    let topic = format!("{}{}", RAW_STATE_TOPIC, oid.as_path());
    publish(&topic, pack(&ev)?.into()).await?;
    Ok(())
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn publish(topic: &str, payload: busrt::borrow::Cow<'_>) -> EResult<()> {
    tokio::time::timeout(timeout(), publish_impl(topic, payload)).await??;
    Ok(())
}
#[inline]
async fn publish_impl(topic: &str, payload: busrt::borrow::Cow<'_>) -> EResult<()> {
    let Some(op) = client()
        .lock()
        .await
        .publish(topic, payload, QoS::No)
        .await?
    else {
        return Ok(());
    };
    op.await??;
    Ok(())
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn publish_confirmed(topic: &str, payload: busrt::borrow::Cow<'_>) -> EResult<()> {
    tokio::time::timeout(timeout(), publish_confirmed_impl(topic, payload)).await??;
    Ok(())
}
#[inline]
async fn publish_confirmed_impl(topic: &str, payload: busrt::borrow::Cow<'_>) -> EResult<()> {
    let Some(op) = client()
        .lock()
        .await
        .publish(topic, payload, QoS::Processed)
        .await?
    else {
        return Ok(());
    };
    op.await??;
    Ok(())
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn subscribe_oids<'a, M>(masks: M, kind: EventKind) -> EResult<()>
where
    M: IntoIterator<Item = &'a OIDMask>,
{
    tokio::time::timeout(timeout(), subscribe_oids_impl(masks, kind)).await??;
    Ok(())
}
#[inline]
async fn subscribe_oids_impl<'a, M>(masks: M, kind: EventKind) -> EResult<()>
where
    M: IntoIterator<Item = &'a OIDMask>,
{
    service::subscribe_oids(rpc().as_ref(), masks, kind).await
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn unsubscribe_oids<'a, M>(masks: M, kind: EventKind) -> EResult<()>
where
    M: IntoIterator<Item = &'a OIDMask>,
{
    tokio::time::timeout(timeout(), unsubscribe_oids_impl(masks, kind)).await??;
    Ok(())
}
#[inline]
async fn unsubscribe_oids_impl<'a, M>(masks: M, kind: EventKind) -> EResult<()>
where
    M: IntoIterator<Item = &'a OIDMask>,
{
    service::unsubscribe_oids(rpc().as_ref(), masks, kind).await
}

/// Request announce for the specific items, the core will send states to item state topics,
/// exclusively for the current service. As the method locks the core inventory for updates, it
/// should be used with caution
///
/// The method will return immediately if the core is inactive as all the states will be
/// automaticaly announced when the node goes to ready state
#[inline]
pub async fn request_announce<'a, M>(masks: M, kind: EventKind) -> EResult<()>
where
    M: IntoIterator<Item = &'a OIDMask>,
{
    #[derive(Serialize)]
    struct Payload<'a> {
        i: Vec<&'a OIDMask>,
        src: Option<&'a str>,
        broadcast: bool,
    }
    if !service::svc_is_core_active(rpc().as_ref(), timeout()).await {
        // the core is inactive, no need to announce
        return Ok(());
    }
    let payload = Payload {
        i: masks.into_iter().collect(),
        src: match kind {
            EventKind::Actual | EventKind::Any => None,
            EventKind::Local => Some(".local"),
            EventKind::Remote => Some(".remote-any"),
        },
        broadcast: false,
    };
    call("eva.core", "item.announce", pack(&payload)?.into()).await?;
    Ok(())
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn exclude_oids<'a, M>(masks: M, kind: EventKind) -> EResult<()>
where
    M: IntoIterator<Item = &'a OIDMask>,
{
    tokio::time::timeout(timeout(), exclude_oids_impl(masks, kind)).await??;
    Ok(())
}
#[inline]
async fn exclude_oids_impl<'a, M>(masks: M, kind: EventKind) -> EResult<()>
where
    M: IntoIterator<Item = &'a OIDMask>,
{
    service::exclude_oids(rpc().as_ref(), masks, kind).await
}

/// Returns true if the core was inactive (not ready) and the service has been waiting for it,
/// false if the core was already active
///
/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn wait_core(wait_forever: bool) -> EResult<bool> {
    service::svc_wait_core(rpc().as_ref(), timeout(), wait_forever).await
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub fn init_logs(initial: &Initial) -> EResult<()> {
    service::svc_init_logs(initial, client())
}

/// calls mark_ready, block and mark_terminating
///
/// # Panics
///
/// Will panic if RPC not set
pub async fn run() -> EResult<()> {
    mark_ready().await?;
    block().await;
    mark_terminating().await?;
    Ok(())
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn mark_ready() -> EResult<()> {
    service::svc_mark_ready(&client()).await
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn mark_terminating() -> EResult<()> {
    service::svc_mark_terminating(&client()).await
}

/// Must be called once
pub fn set_bus_error_suicide_timeout(bes_timeout: Duration) -> EResult<()> {
    service::set_bus_error_suicide_timeout(bes_timeout)
}

/// Blocks the service while active
///
/// In case if the local bus connection is dropped, the service is terminated immediately, as well
/// as all its subprocesses
///
/// This behaviour can be changed by calling set_bus_error_suicide_timeout method and specifying a
/// proper required shutdown timeout until the service is killed
///
/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn block() {
    if let Some(secondary) = RPC_SECONDARY.get() {
        service::svc_block2(rpc().as_ref(), secondary).await;
    } else {
        service::svc_block(rpc().as_ref()).await;
    }
}

/// Creates items, ignores errors if an item already exists
///
/// Must be called after the node core is ready
///
/// # Panics
///
/// Will panic if RPC not set
pub async fn create_items<O: AsRef<OID>>(oids: &[O]) -> EResult<()> {
    for oid in oids {
        let payload = ParamsId {
            i: oid.as_ref().as_str(),
        };
        if let Err(e) = call("eva.core", "item.create", pack(&payload)?.into()).await
            && e.kind() != ErrorKind::ResourceAlreadyExists
        {
            return Err(e);
        }
    }
    Ok(())
}
///
/// Deploys items
///
/// Must be called after the node core is ready
///
/// The parameter must contain a list of item deployment payloads is equal to item.deploy
/// eva.core EAPI call
/// See also https://info.bma.ai/en/actual/eva4/iac.html#items
///
/// The parameter MUST be a collection: either inside the payload, or a list of payloads in
/// a vector/slice etc.
///
/// Example:
///
/// ```rust,ignore
/// let me = initial.id().to_owned();
/// tokio::spawn(async move {
///   let _ = eapi_bus::wait_core(true).await;
///     let x: OID = "lmacro:aaa".parse().unwrap();
///     let payload = serde_json::json! {[
///        {
///          "oid": x,
///           "action": {"svc": me }
///        }
///        ]};
///        let result = eapi_bus::deploy_items(&payload).await;
/// });
/// ```
///
/// # Panics
///
/// Will panic if RPC not set
pub async fn deploy_items<T: Serialize>(items: &T) -> EResult<()> {
    #[derive(Serialize)]
    struct Payload<'a, T: Serialize> {
        items: &'a T,
    }
    call("eva.core", "item.deploy", pack(&Payload { items })?.into()).await?;
    Ok(())
}

pub async fn undeploy_items<T: Serialize>(items: &T) -> EResult<()> {
    #[derive(Serialize)]
    struct Payload<'a, T: Serialize> {
        items: &'a T,
    }
    call(
        "eva.core",
        "item.undeploy",
        pack(&Payload { items })?.into(),
    )
    .await?;
    Ok(())
}

#[derive(Serialize, Debug, Clone)]
pub struct ParamsRunLmacro {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    args: Vec<Value>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    kwargs: BTreeMap<String, Value>,
    #[serde(skip_serializing)]
    wait: Duration,
    #[serde(skip_serializing)]
    timeout_if_not_finished: bool,
}

impl Default for ParamsRunLmacro {
    fn default() -> Self {
        Self::new()
    }
}

impl ParamsRunLmacro {
    pub fn new() -> Self {
        Self {
            args: <_>::default(),
            kwargs: BTreeMap::new(),
            wait: eva_common::DEFAULT_TIMEOUT,
            timeout_if_not_finished: true,
        }
    }
    pub fn arg<V: Serialize>(mut self, arg: V) -> EResult<Self> {
        self.args.push(to_value(arg)?);
        Ok(self)
    }
    pub fn args<V, I>(mut self, args: I) -> EResult<Self>
    where
        V: Serialize,
        I: IntoIterator<Item = V>,
    {
        for arg in args {
            self.args.push(to_value(arg)?);
        }
        Ok(self)
    }
    pub fn kwarg<V: Serialize>(mut self, key: impl Into<String>, value: V) -> EResult<Self> {
        self.kwargs.insert(key.into(), to_value(value)?);
        Ok(self)
    }
    pub fn kwargs<V, I>(mut self, kwargs: I) -> EResult<Self>
    where
        V: Serialize,
        I: IntoIterator<Item = (String, V)>,
    {
        for (key, value) in kwargs {
            self.kwargs.insert(key, to_value(value)?);
        }
        Ok(self)
    }
    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
    /// Does not return an error if the lmacro action is not finished
    pub fn allow_unfinished(mut self) -> Self {
        self.timeout_if_not_finished = false;
        self
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct ParamsUnitAction {
    value: Value,
    #[serde(skip_serializing)]
    wait: Duration,
    #[serde(skip_serializing)]
    timeout_if_not_finished: bool,
}

impl ParamsUnitAction {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            wait: eva_common::DEFAULT_TIMEOUT,
            timeout_if_not_finished: true,
        }
    }
    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
    /// Does not return an error if the unit action is not finished
    pub fn allow_unfinished(mut self) -> Self {
        self.timeout_if_not_finished = false;
        self
    }
}

#[derive(Deserialize, Debug, Clone)]
struct ActionState {
    exitcode: Option<i16>,
    #[serde(default)]
    finished: bool,
    #[serde(default)]
    out: Value,
    #[serde(default)]
    err: Value,
}

pub async fn unit_action(i: &OID, params: &ParamsUnitAction) -> EResult<Value> {
    #[derive(Serialize)]
    struct Params<'a> {
        i: &'a OID,
        #[allow(clippy::struct_field_names)]
        params: &'a ParamsUnitAction,
        #[serde(serialize_with = "eva_common::tools::serialize_duration_as_f64")]
        wait: Duration,
    }
    let p = Params {
        i,
        params,
        wait: params.wait,
    };
    let payload = pack(&p)?;
    let recommended_timeout = params.wait + Duration::from_millis(500);
    let timeout = timeout().max(recommended_timeout);
    let res: ActionState = unpack(
        call_with_timeout("eva.core", "action", payload.into(), timeout)
            .await?
            .payload(),
    )?;
    if (!res.finished || res.exitcode.is_none()) && params.timeout_if_not_finished {
        return Err(Error::timeout());
    }
    if res.exitcode.is_some_and(|code| code != 0) {
        return Err(Error::failed(res.err.to_string()));
    }
    Ok(res.out)
}

pub async fn run_lmacro(i: &OID, params: &ParamsRunLmacro) -> EResult<Value> {
    #[derive(Serialize)]
    struct Params<'a> {
        i: &'a OID,
        #[allow(clippy::struct_field_names)]
        params: &'a ParamsRunLmacro,
        #[serde(serialize_with = "eva_common::tools::serialize_duration_as_f64")]
        wait: Duration,
    }
    let p = Params {
        i,
        params,
        wait: params.wait,
    };
    let payload = pack(&p)?;
    let recommended_timeout = params.wait + Duration::from_millis(500);
    let timeout = timeout().max(recommended_timeout);
    let res: ActionState = unpack(
        call_with_timeout("eva.core", "run", payload.into(), timeout)
            .await?
            .payload(),
    )?;
    if (!res.finished || res.exitcode.is_none()) && params.timeout_if_not_finished {
        return Err(Error::timeout());
    }
    if let Some(code) = res.exitcode
        && code != 0
    {
        return Err(Error::failed(res.err.to_string()));
    }
    Ok(res.out)
}

/// Spawns a future which is executed after the node core is ready
pub fn spawn_when_ready<F>(future: F)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = wait_core(true).await {
            error!("Failed to wait for core: {}", e);
            return;
        }
        future.await;
    });
}
