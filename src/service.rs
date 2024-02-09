/// Service basics
use busrt::client::AsyncClient;
use busrt::rpc::{Rpc, RpcClient, RpcError, RpcEvent, RpcResult};
use busrt::QoS;
use eva_common::acl::OIDMask;
use eva_common::events::{
    ANY_STATE_TOPIC, LOCAL_STATE_TOPIC, REMOTE_STATE_TOPIC, SERVICE_STATUS_TOPIC,
};
use eva_common::op::Op;
use eva_common::payload::{pack, unpack};
use eva_common::prelude::*;
use eva_common::services;
use eva_common::services::SERVICE_PAYLOAD_INITIAL;
use eva_common::services::SERVICE_PAYLOAD_PING;
use eva_common::SLEEP_STEP;
pub use eva_sdk_derive::svc_main;
use lazy_static::lazy_static;
use log::error;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde::{Deserialize, Deserializer};
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::sync::atomic;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::futures::TaskLocalFuture;
use tokio::time::sleep;
use uuid::Uuid;

static BUS_ERROR_SUICIDE_TIMEOUT: OnceCell<Duration> = OnceCell::new();

const ERR_CRITICAL_BUS: &str = "CRITICAL: bus disconnected";

/// Must be called once
pub fn set_bus_error_suicide_timeout(bes_timeout: Duration) -> EResult<()> {
    BUS_ERROR_SUICIDE_TIMEOUT
        .set(bes_timeout)
        .map_err(|_| Error::failed("Unable to set BUS_ERROR_SUICIDE_TIMEOUT"))
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

#[derive(Deserialize)]
pub struct ExtendedParams {
    #[serde(deserialize_with = "deserialize_opt_uuid")]
    call_trace_id: Option<Uuid>,
}

pub fn process_extended_payload(full_payload: &[u8]) -> EResult<(&[u8], Option<ExtendedParams>)> {
    if full_payload.len() > 4 && full_payload[0] == 0xc1 && full_payload[1] == 0xc1 {
        let pos = usize::try_from(u16::from_le_bytes([full_payload[2], full_payload[3]]))? + 4;
        if full_payload.len() < pos {
            return Err(Error::invalid_data("invalid extended payload"));
        }
        let xp = &full_payload[4..pos];
        return Ok((&full_payload[pos..], Some(unpack(xp)?)));
    }
    Ok((full_payload, None))
}

#[inline]
pub fn svc_call_scope<F>(xp: Option<ExtendedParams>, f: F) -> TaskLocalFuture<Option<Uuid>, F>
where
    F: Future,
{
    eva_common::logger::CALL_TRACE_ID.scope(
        if let Some(x) = xp {
            x.call_trace_id
        } else {
            None
        },
        f,
    )
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Local,
    Remote,
    Any,
    Actual,
}

impl EventKind {
    #[inline]
    pub fn topic(&self) -> &str {
        match self {
            EventKind::Local => LOCAL_STATE_TOPIC,
            EventKind::Remote => REMOTE_STATE_TOPIC,
            EventKind::Any => ANY_STATE_TOPIC,
            EventKind::Actual => unimplemented!(),
        }
    }
}

lazy_static! {
    static ref NEED_PANIC: Mutex<Option<Duration>> = <_>::default();
}

/// Will be deprecated soon. Use eva_sdk::eapi instead
pub async fn subscribe_oids<'a, R, M>(rpc: &R, masks: M, kind: EventKind) -> EResult<()>
where
    R: Rpc,
    M: IntoIterator<Item = &'a OIDMask>,
{
    let topics: Vec<String> = if kind == EventKind::Actual {
        let mut t = Vec::new();
        for mask in masks {
            t.push(format!("{}{}", LOCAL_STATE_TOPIC, mask.as_path()));
            t.push(format!("{}{}", REMOTE_STATE_TOPIC, mask.as_path()));
        }
        t
    } else {
        masks
            .into_iter()
            .map(|mask| format!("{}{}", kind.topic(), mask.as_path()))
            .collect()
    };
    rpc.client()
        .lock()
        .await
        .subscribe_bulk(
            &topics.iter().map(String::as_str).collect::<Vec<&str>>(),
            QoS::No,
        )
        .await?;
    Ok(())
}

pub fn set_poc(panic_in: Option<Duration>) {
    *NEED_PANIC.lock() = panic_in;
}

pub fn poc() {
    if let Some(delay) = *NEED_PANIC.lock() {
        svc_terminate();
        bmart::process::suicide(delay + Duration::from_secs(1), false);
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            std::process::exit(1);
        });
    }
}

pub fn svc_handle_default_rpc(method: &str, info: &services::ServiceInfo) -> RpcResult {
    match method {
        "test" => Ok(None),
        "info" => Ok(Some(pack(info)?)),
        "stop" => {
            svc_terminate();
            Ok(None)
        }
        _ => Err(RpcError::method(None)),
    }
}

/// Will be deprecated soon. Use eva_sdk::eapi instead
#[inline]
pub async fn safe_rpc_call(
    rpc: &RpcClient,
    target: &str,
    method: &str,
    params: busrt::borrow::Cow<'_>,
    qos: QoS,
    timeout: Duration,
) -> EResult<RpcEvent> {
    tokio::time::timeout(timeout, rpc.call(target, method, params, qos))
        .await?
        .map_err(Into::into)
}

/// Will be deprecated soon. Use eva_sdk::eapi instead
pub async fn svc_wait_core(rpc: &RpcClient, timeout: Duration, wait_forever: bool) -> EResult<()> {
    #[derive(Deserialize)]
    struct TR {
        active: bool,
    }
    let wait_until = Instant::now() + timeout;
    loop {
        if svc_is_terminating() {
            return Err(Error::failed(
                "core load wait aborted, the service is not active",
            ));
        }
        if let Ok(ev) = safe_rpc_call(
            rpc,
            "eva.core",
            "test",
            busrt::empty_payload!(),
            QoS::Processed,
            timeout,
        )
        .await
        {
            if let Ok(result) = unpack::<TR>(ev.payload()) {
                if result.active {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(SLEEP_STEP).await;
        if !wait_forever && wait_until <= Instant::now() {
            return Err(Error::timeout());
        }
    }
}

static ACTIVE: atomic::AtomicBool = atomic::AtomicBool::new(false);
static TERMINATING: atomic::AtomicBool = atomic::AtomicBool::new(false);

#[inline]
pub fn svc_is_active() -> bool {
    ACTIVE.load(atomic::Ordering::SeqCst)
}

#[inline]
pub fn svc_is_terminating() -> bool {
    TERMINATING.load(atomic::Ordering::SeqCst)
}

#[macro_export]
macro_rules! svc_need_ready {
    () => {
        if !svc_is_active() {
            return;
        }
    };
}

#[macro_export]
macro_rules! svc_rpc_need_ready {
    () => {
        if !svc_is_active() {
            return Err(Error::not_ready("service is not ready").into());
        }
    };
}

/// Terminate the service (canceling block)
#[inline]
pub fn svc_terminate() {
    ACTIVE.store(false, atomic::Ordering::SeqCst);
    TERMINATING.store(true, atomic::Ordering::SeqCst);
}

/// Block the service until terminate is called
///
/// Will be deprecated soon. Use eva_sdk::eapi instead
#[inline]
pub async fn svc_block(rpc: &RpcClient) {
    while svc_is_active() {
        if !rpc.is_connected() {
            error!("{}", ERR_CRITICAL_BUS);
            bmart::process::suicide(
                BUS_ERROR_SUICIDE_TIMEOUT
                    .get()
                    .map_or_else(|| Duration::from_secs(0), |v| *v),
                false,
            );
        }
        sleep(SLEEP_STEP).await;
    }
}

/// Block the service until terminate is called, checking both primary and secondary RPC
///
/// Will be deprecated soon. Use eva_sdk::eapi instead
#[inline]
pub async fn svc_block2(rpc: &RpcClient, secondary: &RpcClient) {
    while svc_is_active() {
        if !rpc.is_connected() || !secondary.is_connected() {
            error!("{}", ERR_CRITICAL_BUS);
            bmart::process::suicide(
                BUS_ERROR_SUICIDE_TIMEOUT
                    .get()
                    .map_or_else(|| Duration::from_secs(0), |v| *v),
                false,
            );
        }
        sleep(SLEEP_STEP).await;
    }
}

/// Initializing service logs
///
/// Will be deprecated soon. Use eva_sdk::eapi instead
///
/// After calling, log macros can be used, all records are transferred to bus LOG/IN/ topics
///
/// # Panics
///
/// Will panic if the mutex is poisoned
#[inline]
pub fn svc_init_logs<C>(
    initial: &services::Initial,
    client: Arc<tokio::sync::Mutex<C>>,
) -> EResult<()>
where
    C: ?Sized + AsyncClient + 'static,
{
    eva_common::logger::init_bus(
        client,
        initial.bus_queue_size(),
        initial.eva_log_level_filter(),
        initial.call_tracing(),
    )
}

/// Sends a broadcast event to mark the service ready at launcher and announce neighbors
///
/// Will be deprecated soon. Use eva_sdk::eapi instead
///
/// # Panics
///
/// Will panic only if payload::pack is broken
pub async fn svc_mark_ready<C>(client: &tokio::sync::Mutex<C>) -> EResult<()>
where
    C: ?Sized + AsyncClient + 'static,
{
    client
        .lock()
        .await
        .publish(
            SERVICE_STATUS_TOPIC,
            pack(&services::ServiceStatusBroadcastEvent::ready())
                .unwrap()
                .into(),
            QoS::Processed,
        )
        .await?
        .unwrap()
        .await??;
    ACTIVE.store(true, atomic::Ordering::SeqCst);
    Ok(())
}

/// Sends a broadcast event to mark the service terminating at launcher and announce neighbors
///
/// Will be deprecated soon. Use eva_sdk::eapi instead
///
/// # Panics
///
/// Will panic only if payload::pack is broken
pub async fn svc_mark_terminating<C>(client: &tokio::sync::Mutex<C>) -> EResult<()>
where
    C: ?Sized + AsyncClient + 'static,
{
    ACTIVE.store(false, atomic::Ordering::SeqCst);
    TERMINATING.store(true, atomic::Ordering::SeqCst);
    client
        .lock()
        .await
        .publish(
            SERVICE_STATUS_TOPIC,
            pack(&services::ServiceStatusBroadcastEvent::terminating())
                .unwrap()
                .into(),
            QoS::Processed,
        )
        .await?
        .unwrap()
        .await??;
    Ok(())
}

/// Start service signal handlers (SIGTERM and SIGINT)
///
/// Calls the terminate method when received
pub fn svc_start_signal_handlers() {
    macro_rules! handle_signal {
        ($signal: expr) => {{
            tokio::spawn(async move {
                signal($signal).unwrap().recv().await;
                svc_terminate();
            });
        }};
    }
    macro_rules! ignore_signal {
        ($signal: expr) => {{
            tokio::spawn(async move {
                loop {
                    signal($signal).unwrap().recv().await;
                }
            });
        }};
    }
    handle_signal!(SignalKind::terminate());
    handle_signal!(SignalKind::hangup());
    ignore_signal!(SignalKind::interrupt());
}

fn process_initial(buf: &[u8]) -> EResult<services::Initial> {
    let initial: services::Initial = unpack(buf)?;
    if initial.config_version() != services::SERVICE_CONFIG_VERSION {
        return Err(Error::not_implemented(format!(
            "config version not supported: {}",
            initial.config_version()
        )));
    }
    if initial.eapi_version() != crate::EAPI_VERSION {
        return Err(Error::not_implemented(format!(
            "EAPI version not supported: {}",
            initial.config_version(),
        )));
    }
    Ok(initial)
}

pub async fn read_initial() -> EResult<services::Initial> {
    let op = Op::new(eva_common::DEFAULT_TIMEOUT);
    let mut stdin = tokio::io::stdin();
    let mut buf = [0_u8; 1];
    tokio::time::timeout(op.timeout()?, stdin.read_exact(&mut buf)).await??;
    if buf[0] != SERVICE_PAYLOAD_INITIAL {
        return Err(Error::invalid_data("invalid payload"));
    }
    let mut buf = [0_u8; 4];
    tokio::time::timeout(op.timeout()?, stdin.read_exact(&mut buf)).await??;
    let len: usize = u32::from_le_bytes(buf).try_into().map_err(Error::failed)?;
    let mut buf = vec![0_u8; len];
    tokio::time::timeout(op.timeout()?, stdin.read_exact(&mut buf)).await??;
    process_initial(&buf)
}

pub fn read_initial_sync() -> EResult<services::Initial> {
    let mut buf = [0_u8; 1];
    std::io::stdin().read_exact(&mut buf)?;
    if buf[0] != SERVICE_PAYLOAD_INITIAL {
        return Err(Error::invalid_data("invalid payload"));
    }
    let mut buf = [0_u8; 4];
    std::io::stdin().read_exact(&mut buf)?;
    let len: usize = u32::from_le_bytes(buf).try_into().map_err(Error::failed)?;
    let mut buf = vec![0_u8; len];
    std::io::stdin().read_exact(&mut buf)?;
    process_initial(&buf)
}

pub fn svc_launch<L, LFut>(launcher: L) -> EResult<()>
where
    L: FnMut(services::Initial) -> LFut,
    LFut: std::future::Future<Output = EResult<()>>,
{
    let initial = read_initial_sync()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(initial.workers() as usize)
        .enable_all()
        .build()?;
    rt.block_on(launch(launcher, initial))?;
    Ok(())
}

async fn launch<L, LFut>(mut launcher: L, mut initial: services::Initial) -> EResult<()>
where
    L: FnMut(services::Initial) -> LFut,
    LFut: std::future::Future<Output = EResult<()>>,
{
    eva_common::self_test();
    let op = Op::new(initial.startup_timeout());
    let eva_dir = initial.eva_dir().to_owned();
    initial.init()?;
    if initial.is_mode_normal() {
        let shutdown_timeout = initial.shutdown_timeout();
        let mut stdin = tokio_fd::AsyncFd::try_from(libc::STDIN_FILENO)?;
        tokio::spawn(async move {
            let mut buf = [0_u8; 1];
            let pid = std::process::id();
            macro_rules! kill {
                () => {
                    tokio::spawn(async move {
                        bmart::process::kill_pstree(pid, Some(shutdown_timeout), true).await;
                    });
                    svc_terminate();
                    break;
                };
            }
            loop {
                if stdin.read_exact(&mut buf).await.is_ok() {
                    if buf[0] != SERVICE_PAYLOAD_PING {
                        kill!();
                    }
                } else {
                    kill!();
                }
                tokio::time::sleep(SLEEP_STEP).await;
            }
        });
        if let Some(prepare_command) = initial.prepare_command() {
            let cmd = format!("cd \"{}\" && {}", eva_dir, prepare_command);
            let t_o = op.timeout()?.as_secs_f64().to_string();
            let opts = bmart::process::Options::new()
                .env("EVA_SYSTEM_NAME", initial.system_name())
                .env("EVA_DIR", initial.eva_dir())
                .env("EVA_SVC_ID", initial.id())
                .env("EVA_SVC_DATA_PATH", initial.data_path().unwrap_or_default())
                .env("EVA_TIMEOUT", t_o.as_str());
            let res = bmart::process::command("sh", ["-c", &cmd], op.timeout()?, opts).await?;
            if !res.ok() {
                return Err(Error::failed(format!(
                    "prepare command failed: {}",
                    res.err.join("\n")
                )));
            }
            for r in res.out {
                println!("{}", r);
            }
            for r in res.err {
                eprintln!("{}", r);
            }
        }
    } else if !initial.can_rtf() {
        return Err(Error::failed(
            "the service is started in react-to-fail mode, but rtf disabled for the service",
        ));
    }
    initial
        .extend_config(op.timeout()?, Path::new(&eva_dir))
        .await?;
    launcher(initial).await
}
