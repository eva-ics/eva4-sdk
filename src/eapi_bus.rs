//! Helper module for EAPI micro-services
use crate::service::{self, EventKind};
use async_trait::async_trait;
use busrt::client::AsyncClient;
use busrt::rpc::{Rpc, RpcClient, RpcEvent, RpcHandlers};
use busrt::QoS;
use eva_common::acl::OIDMask;
use eva_common::common_payloads::ParamsId;
use eva_common::payload::pack;
use eva_common::prelude::*;
use eva_common::services::Initial;
use eva_common::services::Registry;
use once_cell::sync::OnceCell;
use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

pub const AAA_REPORT_TOPIC: &str = "AAA/REPORT";

static RPC: OnceCell<Arc<RpcClient>> = OnceCell::new();
static RPC_SECONDARY: OnceCell<Arc<RpcClient>> = OnceCell::new();
static REGISTRY: OnceCell<Arc<Registry>> = OnceCell::new();
static CLIENT: OnceCell<Arc<Mutex<dyn AsyncClient>>> = OnceCell::new();
static TIMEOUT: OnceCell<Duration> = OnceCell::new();

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
        self.u.replace(src);
        self
    }
    #[inline]
    pub fn svc(mut self, svc: &'a str) -> Self {
        self.u.replace(svc);
        self
    }
    #[inline]
    pub fn subj(mut self, subj: &'a str) -> Self {
        self.u.replace(subj);
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
    client()
        .lock()
        .await
        .subscribe(topic, QoS::Processed)
        .await?;
    Ok(())
}

///
/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn subscribe_bulk(topics: &[&str]) -> EResult<()> {
    client()
        .lock()
        .await
        .subscribe_bulk(topics, QoS::Processed)
        .await?;
    Ok(())
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn publish(topic: &str, payload: busrt::borrow::Cow<'_>) -> EResult<()> {
    client()
        .lock()
        .await
        .publish(topic, payload, QoS::No)
        .await?;
    Ok(())
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn publish_confirmed(topic: &str, payload: busrt::borrow::Cow<'_>) -> EResult<()> {
    client()
        .lock()
        .await
        .publish(topic, payload, QoS::Processed)
        .await?;
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
    service::subscribe_oids(rpc().as_ref(), masks, kind).await
}

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn wait_core(wait_forever: bool) -> EResult<()> {
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
        if let Err(e) = call("eva.core", "item.create", pack(&payload)?.into()).await {
            if e.kind() != ErrorKind::ResourceAlreadyExists {
                return Err(e);
            }
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
