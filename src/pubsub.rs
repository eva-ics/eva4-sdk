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
}

impl PsNodeStatus {
    #[inline]
    pub fn new_running() -> Self {
        Self {
            status: NodeStatus::Running,
        }
    }
    #[inline]
    pub fn new_terminating() -> Self {
        Self {
            status: NodeStatus::Terminating,
        }
    }
    #[inline]
    pub fn status(&self) -> NodeStatus {
        self.status
    }
}
