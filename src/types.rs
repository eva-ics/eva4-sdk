use eva_common::events::{
    LocalStateEvent, RawStateEventOwned, RemoteStateEvent, ReplicationStateEvent,
};
use eva_common::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// basic state object, usually collected from topics, ignores extra fields
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct State {
    #[serde(alias = "s")]
    pub status: ItemStatus,
    #[serde(alias = "v")]
    pub value: Option<Value>,
    #[serde(rename = "t")]
    pub set_time: f64,
}

impl From<LocalStateEvent> for State {
    #[inline]
    fn from(ev: LocalStateEvent) -> Self {
        Self {
            status: ev.status,
            value: Some(ev.value),
            set_time: ev.t,
        }
    }
}

impl From<RemoteStateEvent> for State {
    #[inline]
    fn from(ev: RemoteStateEvent) -> Self {
        Self {
            status: ev.status,
            value: Some(ev.value),
            set_time: ev.t,
        }
    }
}

/// basic state with OID specified inside the object, collected from RPC, useful to keep/transfer
/// via internal channels
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ItemState {
    #[serde(alias = "i")]
    pub oid: OID,
    #[serde(alias = "s")]
    pub status: ItemStatus,
    #[serde(alias = "v")]
    pub value: Option<Value>,
    #[serde(rename = "t")]
    pub set_time: f64,
}

impl ItemState {
    #[inline]
    pub fn from_state(state: State, oid: OID) -> Self {
        Self {
            oid,
            status: state.status,
            value: state.value,
            set_time: state.set_time,
        }
    }
    #[inline]
    pub fn into_state(self) -> (OID, State) {
        (
            self.oid,
            State {
                status: self.status,
                value: self.value,
                set_time: self.set_time,
            },
        )
    }
}

/// short item state, without set-time, collected from RPC, useful to keep/transfer via internal
/// channels
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ShortItemState {
    #[serde(alias = "i")]
    pub oid: OID,
    #[serde(alias = "s")]
    pub status: ItemStatus,
    #[serde(alias = "v")]
    pub value: Option<Value>,
}

impl ShortItemState {
    #[inline]
    pub fn into_raw_state_event_owned(self) -> (OID, RawStateEventOwned) {
        (
            self.oid,
            if let Some(value) = self.value {
                RawStateEventOwned::new(self.status, value)
            } else {
                RawStateEventOwned::new0(self.status)
            },
        )
    }
}

/// short item state, without set-time but with connected field, useful to check are collected
/// items connected or keep the most recent known state
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ShortItemStateConnected {
    #[serde(alias = "i")]
    pub oid: OID,
    #[serde(alias = "s")]
    pub status: ItemStatus,
    #[serde(alias = "v")]
    pub value: Option<Value>,
    pub connected: bool,
}

/// helper object for database services - status can be null/absent
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HistoricalState {
    #[serde(alias = "s", skip_serializing_if = "Option::is_none")]
    pub status: Option<Value>,
    #[serde(alias = "v", skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(rename = "t")]
    pub set_time: f64,
}

impl HistoricalState {
    pub fn na(timestamp: f64, need_status: bool, need_value: bool) -> Self {
        Self {
            status: if need_status { Some(Value::Unit) } else { None },
            value: if need_value { Some(Value::Unit) } else { None },
            set_time: timestamp,
        }
    }
}

/// result object for database services
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CompactStateHistory {
    #[serde(alias = "s", skip_serializing_if = "Option::is_none")]
    pub status: Option<Vec<Value>>,
    #[serde(alias = "v", skip_serializing_if = "Option::is_none")]
    pub value: Option<Vec<Value>>,
    #[serde(rename = "t")]
    pub set_time: Vec<f64>,
}

impl From<CompactStateHistory> for Vec<HistoricalState> {
    fn from(mut data: CompactStateHistory) -> Self {
        let mut result = Vec::new();
        for set_time in data.set_time.into_iter().rev() {
            let status = if let Some(ref mut s) = data.status {
                s.pop()
            } else {
                None
            };
            let value = if let Some(ref mut v) = data.value {
                v.pop()
            } else {
                None
            };
            result.push(HistoricalState {
                status,
                value,
                set_time,
            });
        }
        result.reverse();
        result
    }
}

/// a special state object, returned by state_history
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum StateHistoryData {
    Regular(Vec<HistoricalState>),
    Compact(CompactStateHistory),
}

impl StateHistoryData {
    #[inline]
    pub fn new_regular(data: Vec<HistoricalState>) -> Self {
        Self::Regular(data)
    }
    pub fn new_compact(data: Vec<HistoricalState>, need_status: bool, need_value: bool) -> Self {
        let mut status = Vec::new();
        let mut value = Vec::new();
        let mut set_time = Vec::new();
        for d in data {
            if need_status {
                status.push(d.status.unwrap_or_default());
            }
            if need_value {
                value.push(d.value.unwrap_or_default());
            }
            set_time.push(d.set_time);
        }
        Self::Compact(CompactStateHistory {
            status: if need_status { Some(status) } else { None },
            value: if need_value { Some(value) } else { None },
            set_time,
        })
    }
}

/// Standard format for time-series data frames filling
#[derive(Debug, Copy, Clone)]
pub enum Fill {
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Weeks(u32),
}

impl Fill {
    pub fn as_secs_f64(&self) -> f64 {
        match self {
            Fill::Seconds(v) => f64::from(*v),
            Fill::Minutes(v) => f64::from(*v) * 60.0,
            Fill::Hours(v) => f64::from(*v) * 3_600.0,
            Fill::Days(v) => f64::from(*v) * 86_400.0,
            Fill::Weeks(v) => f64::from(*v) * 604_800.0,
        }
    }
    pub fn as_secs(&self) -> u64 {
        match self {
            Fill::Seconds(v) => u64::from(*v),
            Fill::Minutes(v) => u64::from(*v) * 60,
            Fill::Hours(v) => u64::from(*v) * 3_600,
            Fill::Days(v) => u64::from(*v) * 86_400,
            Fill::Weeks(v) => u64::from(*v) * 604_800,
        }
    }
    pub fn time_series(&self, t_start: f64, t_end: f64, limit: Option<usize>) -> Vec<f64> {
        let mut data = Vec::new();
        let period = self.as_secs_f64();
        if let Some(l) = limit {
            let x = t_end % period;
            let mut ts = if x == 0.0 { t_end } else { t_end - x };
            let y = t_start % period;
            let start = if y == 0.0 { t_start } else { t_start - x };
            while ts >= start && data.len() < l {
                data.push(ts);
                ts -= period;
            }
            data.reverse();
        } else {
            let x = t_start % period;
            let mut ts = if x == 0.0 { t_start } else { t_start - x };
            while ts <= t_end {
                data.push(ts);
                ts += period;
            }
        }
        data
    }
    pub fn fill_na(
        &self,
        t_start: f64,
        t_end: f64,
        limit: Option<usize>,
        need_status: bool,
        need_value: bool,
    ) -> Vec<HistoricalState> {
        let mut data = Vec::new();
        let period = self.as_secs_f64();
        if let Some(l) = limit {
            let x = t_end % period;
            let mut ts = if x == 0.0 { t_end } else { t_end - x };
            let y = t_start % period;
            let start = if y == 0.0 { t_start } else { t_start - x };
            while ts >= start && data.len() < l {
                data.push(HistoricalState::na(ts, need_status, need_value));
                ts -= period;
            }
            data.reverse();
        } else {
            let x = t_start % period;
            let mut ts = if x == 0.0 { t_start } else { t_start - x };
            while ts <= t_end {
                data.push(HistoricalState::na(ts, need_status, need_value));
                ts += period;
            }
        }
        data
    }
}

impl fmt::Display for Fill {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fill::Seconds(v) => write!(f, "{}S", v),
            Fill::Minutes(v) => write!(f, "{}T", v),
            Fill::Hours(v) => write!(f, "{}H", v),
            Fill::Days(v) => write!(f, "{}D", v),
            Fill::Weeks(v) => write!(f, "{}W", v),
        }
    }
}

impl<'de> Deserialize<'de> for Fill {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Fill, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl Serialize for Fill {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl FromStr for Fill {
    type Err = Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            Err(Error::invalid_params("invalid filling"))
        } else {
            macro_rules! ep {
                ($res: expr) => {{
                    let res = $res.map_err(|e| {
                        Error::invalid_params(format!("unable to parse filling: {}", e))
                    })?;
                    if res == 0 {
                        return Err(Error::invalid_params("fill numbers can not be zero"));
                    }
                    res
                }};
            }
            Ok(match &s[s.len() - 1..] {
                "S" => Fill::Seconds(ep!(s[..s.len() - 1].parse())),
                "T" => Fill::Minutes(ep!(s[..s.len() - 1].parse())),
                "H" => Fill::Hours(ep!(s[..s.len() - 1].parse())),
                "D" => Fill::Days(ep!(s[..s.len() - 1].parse())),
                "W" => Fill::Weeks(ep!(s[..s.len() - 1].parse())),
                v => {
                    return Err(Error::invalid_params(format!(
                        "invalid filling type: {}",
                        v
                    )))
                }
            })
        }
    }
}

/// State property chooser
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum StateProp {
    Status,
    Value,
}

impl StateProp {
    pub fn as_str(&self) -> &str {
        match self {
            StateProp::Status => "status",
            StateProp::Value => "value",
        }
    }
}

impl fmt::Display for StateProp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'de> Deserialize<'de> for StateProp {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<StateProp, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl Serialize for StateProp {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl FromStr for StateProp {
    type Err = Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "status" | "S" => Ok(StateProp::Status),
            "value" | "V" => Ok(StateProp::Value),
            v => Err(Error::invalid_params(format!("invalid prop: {}", v))),
        }
    }
}

/// State property extended chooser
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum StatePropExt {
    Status,
    Value,
    Act,
}

impl StatePropExt {
    pub fn as_str(&self) -> &str {
        match self {
            StatePropExt::Status => "status",
            StatePropExt::Value => "value",
            StatePropExt::Act => "act",
        }
    }
}

impl<'de> Deserialize<'de> for StatePropExt {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<StatePropExt, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl Serialize for StatePropExt {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl FromStr for StatePropExt {
    type Err = Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "status" | "S" => Ok(StatePropExt::Status),
            "value" | "V" => Ok(StatePropExt::Value),
            "act" | "A" => Ok(StatePropExt::Act),
            v => Err(Error::invalid_params(format!("invalid prop: {}", v))),
        }
    }
}

/// Full item state, used by replication services for bulk topics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullItemState {
    #[serde(alias = "i")]
    pub oid: OID,
    #[serde(alias = "s")]
    pub status: ItemStatus,
    #[serde(alias = "v")]
    pub value: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub act: Option<usize>,
    pub ieid: IEID,
    pub t: f64,
}

impl FullItemState {
    pub fn from_local_state_event(state: LocalStateEvent, oid: OID) -> Self {
        Self {
            oid,
            status: state.status,
            value: state.value,
            act: state.act,
            ieid: state.ieid,
            t: state.t,
        }
    }
    pub fn from_remote_state_event(state: RemoteStateEvent, oid: OID) -> Self {
        Self {
            oid,
            status: state.status,
            value: state.value,
            act: state.act,
            ieid: state.ieid,
            t: state.t,
        }
    }
    pub fn into_replication_state_event(self, node_name: &str) -> ReplicationStateEvent {
        ReplicationStateEvent {
            status: self.status,
            value: self.value,
            act: self.act,
            ieid: self.ieid,
            t: self.t,
            node: node_name.to_owned(),
            force_accept: false,
        }
    }
}

impl From<FullItemState> for LocalStateEvent {
    fn from(state: FullItemState) -> LocalStateEvent {
        Self {
            status: state.status,
            value: state.value,
            act: state.act,
            ieid: state.ieid,
            t: state.t,
        }
    }
}

/// Full item state with connected field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullItemStateConnected {
    #[serde(alias = "i")]
    pub oid: OID,
    #[serde(alias = "s")]
    pub status: ItemStatus,
    #[serde(alias = "v")]
    pub value: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub act: Option<usize>,
    pub ieid: IEID,
    pub t: f64,
    pub connected: bool,
}

impl FullItemStateConnected {
    pub fn from_local_state_event(state: LocalStateEvent, oid: OID) -> Self {
        Self {
            oid,
            status: state.status,
            value: state.value,
            act: state.act,
            ieid: state.ieid,
            t: state.t,
            connected: true,
        }
    }
    pub fn from_remote_state_event(state: RemoteStateEvent, oid: OID) -> Self {
        Self {
            oid,
            status: state.status,
            value: state.value,
            act: state.act,
            ieid: state.ieid,
            t: state.t,
            connected: state.connected,
        }
    }
}

/// Full item state for remote items, used by HMI services
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullRemoteItemState {
    #[serde(alias = "i")]
    pub oid: OID,
    #[serde(alias = "s")]
    pub status: ItemStatus,
    #[serde(alias = "v")]
    pub value: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub act: Option<usize>,
    pub ieid: IEID,
    pub t: f64,
    pub node: String,
    pub connected: bool,
}

impl FullRemoteItemState {
    pub fn from_local_state_event(state: LocalStateEvent, oid: OID, system_name: &str) -> Self {
        Self {
            oid,
            status: state.status,
            value: state.value,
            act: state.act,
            ieid: state.ieid,
            t: state.t,
            node: system_name.to_owned(),
            connected: true,
        }
    }
    pub fn from_remote_state_event(state: RemoteStateEvent, oid: OID) -> Self {
        Self {
            oid,
            status: state.status,
            value: state.value,
            act: state.act,
            ieid: state.ieid,
            t: state.t,
            node: state.node,
            connected: state.connected,
        }
    }
}
