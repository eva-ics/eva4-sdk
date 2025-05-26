use busrt::rpc::{RpcError, RpcEvent, RpcResult};
use eva_common::{
    acl::Acl,
    payload::{pack, unpack},
    prelude::*,
    services,
};
pub use logicline::global::{ingress, processor, set_recording};
pub use logicline::{action, Processor, Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use once_cell::sync::OnceCell;

use crate::hmi::XParamsOwned;

static SNAPSHOT_ACL_MAPPING: OnceCell<SnapshotAclMapping> = OnceCell::new();

pub fn set_snapshot_acl_mapping(mapping: SnapshotAclMapping) -> EResult<()> {
    SNAPSHOT_ACL_MAPPING
        .set(mapping)
        .map_err(|_| Error::core("Snapshot ACL mapping already set"))
}

pub(crate) fn api_ll_info() -> EResult<Vec<u8>> {
    #[derive(Serialize)]
    struct Response {
        recording: bool,
    }
    let current_recording_state = logicline::global::is_recording();
    pack(&Response {
        recording: current_recording_state,
    })
}

pub(crate) fn api_ll_snapshot(params: Option<Value>, acl: Option<&Acl>) -> EResult<Vec<u8>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Params {
        #[serde(default)]
        line_filter: String,
    }
    let mut line_filter = String::new();
    if let Some(p) = params {
        if !p.is_unit() {
            line_filter = Params::deserialize(p)?.line_filter;
        }
    };
    if line_filter == "#" || line_filter == "*" {
        line_filter.clear();
    }
    let mut snapshot = logicline::global::snapshot_filtered(|l| l.name().starts_with(&line_filter));
    if let Some(acl) = acl {
        if let Some(mapping) = SNAPSHOT_ACL_MAPPING.get() {
            snapshot = mapping.format_snapshot(snapshot, acl);
        }
    }
    pack(&snapshot)
}

pub(crate) fn api_ll_set_recording(params: Value) -> EResult<Vec<u8>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Params {
        recording: bool,
    }
    let params: Params = Params::deserialize(params)?;
    logicline::global::set_recording(params.recording);
    Ok(vec![])
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct SnapshotAclMapping {
    inner: BTreeMap<String, OID>,
}

impl SnapshotAclMapping {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add OID mapping for ACL checks in snapshots. source: `line/step_name`
    pub fn add(&mut self, source: impl AsRef<str>, oid: OID) {
        self.inner.insert(source.as_ref().to_owned(), oid);
    }

    pub fn format_snapshot(
        &self,
        mut snapshot: logicline::Snapshot,
        acl: &Acl,
    ) -> logicline::Snapshot {
        for line in snapshot.lines_mut().values_mut() {
            let line_name = line.name().to_owned();
            for step in line.steps_mut() {
                for i in step.info_mut() {
                    let source = format!("{}/{}", line_name, i.name());
                    let Some(oid) = self.inner.get(&source) else {
                        continue;
                    };
                    if acl.check_item_read(oid) {
                        continue;
                    }
                    *i = i.to_modified(
                        None,
                        Some(serde_json::Value::String("<hidden>".to_owned())),
                        None,
                        None,
                    );
                }
            }
        }
        snapshot
    }
}

pub fn handle_default_rpc(event: RpcEvent, info: &services::ServiceInfo) -> RpcResult {
    let method = event.parse_method()?;
    let payload = event.payload();
    match method {
        "ll.info" => Ok(Some(api_ll_info()?)),
        "ll.snapshot" => Ok(Some(api_ll_snapshot(
            if payload.is_empty() {
                None
            } else {
                unpack(payload)?
            },
            None,
        )?)),
        "ll.set_recording" => {
            if payload.is_empty() {
                return Err(RpcError::params(None));
            }
            Ok(Some(api_ll_set_recording(unpack(payload)?)?))
        }
        "x" => {
            if payload.is_empty() {
                Err(RpcError::params(None))
            } else {
                let xp: XParamsOwned = unpack(payload)?;
                match xp.method() {
                    "ll.info" => Ok(Some(api_ll_info()?)),
                    "ll.snapshot" => Ok(Some(api_ll_snapshot(Some(xp.params), Some(&xp.acl))?)),
                    "ll.set_recording" => {
                        xp.acl.require_admin()?;
                        Ok(Some(api_ll_set_recording(xp.params)?))
                    }
                    _ => Err(RpcError::method(None)),
                }
            }
        }
        _ => crate::service::svc_handle_default_rpc(method, info),
    }
}
