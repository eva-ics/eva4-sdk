//! Helper module for EAPI micro-services
use crate::service::{self, EventKind};
use async_trait::async_trait;
use busrt::client::AsyncClient;
use busrt::rpc::{Rpc, RpcClient, RpcEvent};
use busrt::QoS;
use eva_common::acl::OIDMaskList;
use eva_common::payload::pack;
use eva_common::prelude::*;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

pub const AAA_REPORT_TOPIC: &str = "AAA/REPORT";

static RPC: OnceCell<Arc<RpcClient>> = OnceCell::new();
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

#[derive(Serialize, Deserialize, Default)]
pub struct AccountingEvent<'a> {
    // the ID is usually assigned by the accounting service and should be None
    #[serde(skip_serializing_if = "Option::is_none")]
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

/// Must be called before using the module
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

/// # Panics
///
/// Will panic if RPC not set
pub async fn call0(target: &str, method: &str) -> EResult<RpcEvent> {
    tokio::time::timeout(
        timeout(),
        rpc().call(target, method, busrt::empty_payload!(), QoS::No),
    )
    .await?
    .map_err(Into::into)
}

/// # Panics
///
/// Will panic if RPC not set
pub async fn call(target: &str, method: &str, params: busrt::borrow::Cow<'_>) -> EResult<RpcEvent> {
    tokio::time::timeout(timeout(), rpc().call(target, method, params, QoS::No))
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
pub async fn subscribe_oids(masks: &OIDMaskList, kind: EventKind) -> EResult<()> {
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
pub fn init_logs(initial: &eva_common::services::Initial) -> EResult<()> {
    service::svc_init_logs(initial, client())
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

/// # Panics
///
/// Will panic if RPC not set
#[inline]
pub async fn block() {
    service::svc_block(rpc().as_ref()).await
}
