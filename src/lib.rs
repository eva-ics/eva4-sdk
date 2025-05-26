pub mod bitman;
#[cfg(feature = "controller")]
pub mod controller;
pub mod eapi_bus;
#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "hmi")]
pub mod hmi;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "logicline")]
pub mod ll;
#[cfg(feature = "pubsub")]
pub mod pubsub;
pub mod service;
pub mod state;
pub mod types;

pub const EAPI_VERSION: u16 = 1;

pub mod prelude {
    pub use crate::eapi_bus::{self, AccountingEvent, ClientAccounting};
    #[cfg(feature = "logicline")]
    pub use crate::ll;
    pub use crate::service::process_extended_payload;
    pub use crate::service::safe_rpc_call;
    pub use crate::service::svc_block;
    pub use crate::service::svc_call_scope;
    pub use crate::service::svc_handle_default_rpc;
    pub use crate::service::svc_init_logs;
    pub use crate::service::svc_is_active;
    pub use crate::service::svc_is_terminating;
    #[cfg(not(target_os = "windows"))]
    pub use crate::service::svc_launch;
    pub use crate::service::svc_main;
    pub use crate::service::svc_mark_ready;
    pub use crate::service::svc_mark_terminating;
    #[cfg(not(target_os = "windows"))]
    pub use crate::service::svc_start_signal_handlers;
    pub use crate::service::svc_terminate;
    pub use crate::service::svc_wait_core;
    pub use crate::service::BusRtEapiEvent as _;
    pub use crate::svc_need_ready;
    pub use crate::svc_rpc_need_ready;
    pub use busrt::client::AsyncClient;
    pub use busrt::rpc::{Rpc, RpcClient, RpcError, RpcEvent, RpcHandlers, RpcResult};
    pub use busrt::{Frame, QoS};
    pub use eva_common::err_logger;
    pub use eva_common::payload::{pack, unpack};
    pub use eva_common::services::Registry;
    pub use eva_common::services::{Initial, ServiceInfo, ServiceMethod};
    pub use eva_common::{EResult, Error};
    pub use log::{debug, error, info, trace, warn};
}
