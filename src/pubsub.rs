use eva_common::events::NodeInfo;
use serde::{Deserialize, Serialize};

pub const PS_ITEM_STATE_TOPIC: &str = "ST/";
pub const PS_ITEM_BULK_STATE_TOPIC: &str = "STBULK/";
pub const PS_NODE_STATE_TOPIC: &str = "NODE/ST/";

#[derive(Deserialize, Serialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum NodeStatus {
    Running = 1,
    Terminating = 0xef,
}

#[derive(Deserialize, Serialize)]
pub struct PsNodeStatus {
    status: NodeStatus,
    info: Option<NodeInfo>,
    #[serde(default = "eva_common::tools::default_true")]
    api_enabled: bool,
}

impl PsNodeStatus {
    #[inline]
    pub fn new_running() -> Self {
        Self {
            status: NodeStatus::Running,
            info: None,
            api_enabled: true,
        }
    }
    #[inline]
    pub fn new_terminating() -> Self {
        Self {
            status: NodeStatus::Terminating,
            info: None,
            api_enabled: true,
        }
    }
    #[inline]
    pub fn with_info(mut self, info: NodeInfo) -> Self {
        self.info = Some(info);
        self
    }
    #[inline]
    pub fn with_api_disabled(mut self) -> Self {
        self.api_enabled = false;
        self
    }
    #[inline]
    pub fn status(&self) -> NodeStatus {
        self.status
    }
    #[inline]
    pub fn info(&self) -> Option<&NodeInfo> {
        self.info.as_ref()
    }
    #[inline]
    pub fn take_info(&mut self) -> Option<NodeInfo> {
        self.info.take()
    }
    #[inline]
    pub fn is_api_enabled(&self) -> bool {
        self.api_enabled
    }
}
