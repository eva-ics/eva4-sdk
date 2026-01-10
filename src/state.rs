use std::sync::LazyLock;
use std::{borrow::Cow, collections::BTreeMap, str::Split, sync::Arc};

use eva_common::events::{
    LOCAL_STATE_TOPIC, LocalStateEvent, REMOTE_ARCHIVE_STATE_TOPIC, REMOTE_STATE_TOPIC,
    RemoteStateEvent,
};

use crate::eapi_bus;
use crate::prelude::{Frame, pack, unpack};
use crate::types::FullItemStateConnected;
use eva_common::value::{Value, ValueOptionOwned};
use eva_common::{
    EResult, Error, ItemKind, OID, OID_MASK_PREFIX_REGEX,
    acl::{OIDMask, OIDMaskList},
};
use eva_common::{IEID, ItemStatus};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

static DB: LazyLock<Db> = LazyLock::new(<_>::default);

pub async fn process_bus_frame(frame: &Frame) -> EResult<()> {
    DB.process_bus_frame(frame).await
}

/// Get a state from the process-global database
pub async fn get(oid: &OID) -> EResult<Option<State>> {
    DB.get(oid).await
}

/// Query the process-global database for states matching the given mask and filter
pub async fn query(query: Query<'_>) -> EResult<Vec<State>> {
    DB.query(query).await
}

/// Remove a local state from the process-global database
pub async fn remove_local(oid: &OID) -> Option<State> {
    DB.remove_local(oid).await
}

#[derive(Deserialize, Debug)]
struct FullItemStateConnectedWithMeta {
    #[serde(flatten)]
    st: FullItemStateConnected,
    #[serde(default)]
    meta: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct State(Arc<StateInner>);

impl State {
    pub fn oid(&self) -> &OID {
        &self.0.st.oid
    }
    pub fn status(&self) -> ItemStatus {
        self.0.st.status
    }
    pub fn value(&self) -> &Value {
        &self.0.st.value
    }
    pub fn act(&self) -> usize {
        self.0.st.act.unwrap_or_default()
    }
    pub fn ieid(&self) -> IEID {
        self.0.st.ieid
    }
    pub fn set_time(&self) -> f64 {
        self.0.st.t
    }
    pub fn connected(&self) -> bool {
        self.0.st.connected
    }
    pub fn meta(&self) -> Option<&Value> {
        self.0.meta.as_ref()
    }
}

impl Clone for State {
    fn clone(&self) -> Self {
        State(self.0.clone())
    }
}

impl From<FullItemStateConnected> for State {
    fn from(st: FullItemStateConnected) -> Self {
        State(Arc::new(StateInner {
            meta: ValueOptionOwned::No,
            st,
        }))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StateInner {
    #[serde(skip_serializing_if = "ValueOptionOwned::is_none")]
    meta: ValueOptionOwned,
    #[serde(flatten)]
    st: FullItemStateConnected,
}

pub struct Query<'a> {
    mask: &'a OIDMask,
    filter: Option<Filter<'a>>,
    local: bool,
}

impl<'a> Query<'a> {
    pub fn new(mask: &'a OIDMask) -> Self {
        Self {
            mask,
            filter: None,
            local: false,
        }
    }
    pub fn filter(mut self, filter: Filter<'a>) -> Self {
        self.filter = Some(filter);
        self
    }
    /// Set the query as local-only (fetch only local states, do not query the node inventory),
    /// works faster than a non-local query but may miss certain items or their metadata. It is
    /// recommended first (or periodically) to run a non-local query then use local ones
    pub fn local(mut self) -> Self {
        self.local = true;
        self
    }
}

#[derive(Default)]
pub struct Db {
    state_db: Arc<RwLock<StateDb>>,
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Db {
            state_db: self.state_db.clone(),
        }
    }
}

impl Db {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn process_bus_frame(&self, frame: &Frame) -> EResult<()> {
        let mut db = self.state_db.write().await;
        db.process_bus_frame(frame)
    }
    pub async fn get(&self, oid: &OID) -> EResult<Option<State>> {
        #[derive(Serialize)]
        struct Params<'a> {
            i: &'a OID,
            full: bool,
        }
        let db = self.state_db.read().await;
        let res = db.get_local(oid);
        if res.is_some() {
            return Ok(res);
        }
        let r = eapi_bus::call(
            "eva.core",
            "item.state",
            pack(&Params { i: oid, full: true })?.into(),
        )
        .await?;
        let s_st: Vec<FullItemStateConnectedWithMeta> = unpack(r.payload())?;
        if s_st.len() > 1 {
            return Err(Error::invalid_data("expected a single state"));
        }
        let Some(s) = s_st.into_iter().next() else {
            return Ok(None);
        };
        drop(db);
        Ok(Some(
            self.state_db
                .write()
                .await
                .record_state_connected_with_meta(s),
        ))
    }
    pub async fn query(&self, query: Query<'_>) -> EResult<Vec<State>> {
        #[derive(Serialize)]
        struct Params<'a> {
            i: &'a OIDMask,
            full: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            include: Option<&'a OIDMaskList>,
            #[serde(skip_serializing_if = "Option::is_none")]
            exclude: Option<&'a OIDMaskList>,
        }
        if query.local {
            return self.state_db.read().await.query_local(&query);
        }
        let payload = Params {
            i: query.mask,
            full: true,
            include: query.filter.as_ref().and_then(|f| f.include),
            exclude: query.filter.as_ref().and_then(|f| f.exclude),
        };
        let r = eapi_bus::call("eva.core", "item.state", pack(&payload)?.into()).await?;
        let s_st: Vec<FullItemStateConnectedWithMeta> = unpack(r.payload())?;
        if s_st.is_empty() {
            return Ok(Vec::new());
        }
        let mut r_vec = Vec::with_capacity(s_st.len());
        let mut db = self.state_db.write().await;
        for s in s_st {
            r_vec.push(db.record_state_connected_with_meta(s));
        }
        Ok(r_vec)
    }
    pub async fn remove_local(&self, oid: &OID) -> Option<State> {
        let mut db = self.state_db.write().await;
        db.remove(oid)
    }
}

// a low-level lock-free db component
#[derive(Default)]
struct StateDb {
    db: StateMap,
}

impl StateDb {
    fn process_bus_frame(&mut self, frame: &Frame) -> EResult<()> {
        let Some(topic) = frame.topic() else {
            return Ok(());
        };
        if let Some(oid_str) = topic.strip_prefix(LOCAL_STATE_TOPIC) {
            let oid: OID = OID::from_path(oid_str).map_err(Error::invalid_data)?;
            let ev: LocalStateEvent = unpack(frame.payload())?;
            let st: FullItemStateConnected =
                FullItemStateConnected::from_local_state_event(ev, oid);
            self.record(State::from(st)).ok();
            return Ok(());
        }
        let Some(oid_str) = topic
            .strip_prefix(REMOTE_STATE_TOPIC)
            .or_else(|| topic.strip_prefix(REMOTE_ARCHIVE_STATE_TOPIC))
        else {
            return Ok(());
        };
        let oid: OID = OID::from_path(oid_str).map_err(Error::invalid_data)?;
        let ev: RemoteStateEvent = unpack(frame.payload())?;
        let st: FullItemStateConnected = FullItemStateConnected::from_remote_state_event(ev, oid);
        self.record(State::from(st)).ok();
        Ok(())
    }
    fn record_state_connected_with_meta(&mut self, mut s: FullItemStateConnectedWithMeta) -> State {
        if s.meta.is_none() {
            s.meta = Some(Value::Unit);
        }
        let mut st = State(
            StateInner {
                st: s.st,
                meta: s.meta.into(),
            }
            .into(),
        );
        if let Err(e) = self.record(st.clone()) {
            st = e; // got newer state, use it instead
        }
        st
    }
    fn record(&mut self, state: State) -> Result<(), State> {
        self.db.append(state, false)
    }
    //fn record_force(&mut self, state: State) -> EResult<()> {
    //self.db.append(state, true)
    //}
    fn get_local(&self, oid: &OID) -> Option<State> {
        self.db.get(oid)
    }
    fn remove(&mut self, oid: &OID) -> Option<State> {
        self.db.remove(oid)
    }
    fn query_local(&self, query: &Query<'_>) -> EResult<Vec<State>> {
        let filter: Cow<Filter> = query
            .filter
            .as_ref()
            .map_or_else(|| Cow::Owned(Filter::new()), Cow::Borrowed);
        self.db.get_by_mask(query.mask, &filter)
    }
}

#[derive(Default, Debug)]
struct StateTree {
    childs: BTreeMap<String, StateTree>,
    members: BTreeMap<Arc<OID>, State>,
    members_wildcard: BTreeMap<Arc<OID>, State>,
}

impl StateTree {
    fn is_empty(&self) -> bool {
        self.childs.is_empty() && self.members.is_empty()
    }
}

#[derive(Debug, Default)]
struct StateMap {
    unit: StateTree,
    sensor: StateTree,
    lvar: StateTree,
    lmacro: StateTree,
}

#[derive(Default, Clone)]
pub struct Filter<'a> {
    include: Option<&'a OIDMaskList>,
    exclude: Option<&'a OIDMaskList>,
}

impl<'a> Filter<'a> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn include(mut self, mask_list: &'a OIDMaskList) -> Self {
        self.include = Some(mask_list);
        self
    }
    pub fn exclude(mut self, mask_list: &'a OIDMaskList) -> Self {
        self.exclude = Some(mask_list);
        self
    }
    fn matches(&self, state: &State) -> bool {
        if let Some(f) = self.include
            && !f.matches(state.oid())
        {
            return false;
        }
        if let Some(f) = self.exclude {
            !f.matches(state.oid())
        } else {
            true
        }
    }
}

impl StateMap {
    #[inline]
    fn get_tree(&self, tp: ItemKind) -> &StateTree {
        match tp {
            ItemKind::Unit => &self.unit,
            ItemKind::Sensor => &self.sensor,
            ItemKind::Lvar => &self.lvar,
            ItemKind::Lmacro => &self.lmacro,
            //_ => Err(Error::not_implemented()),
        }
    }
    #[inline]
    fn get_tree_mut(&mut self, tp: ItemKind) -> &mut StateTree {
        match tp {
            ItemKind::Unit => &mut self.unit,
            ItemKind::Sensor => &mut self.sensor,
            ItemKind::Lvar => &mut self.lvar,
            ItemKind::Lmacro => &mut self.lmacro,
            //_ => Err(Error::not_implemented()),
        }
    }
    #[inline]
    /// in case of if the state is already present as is newer or equal to the existing one,
    /// returns the eixsting state as an error
    fn append(&mut self, state: State, force: bool) -> Result<(), State> {
        let tree = self.get_tree_mut(state.oid().kind());
        append_state_rec(tree, state.oid().full_id().split('/'), &state, force)
    }
    #[inline]
    fn get(&self, oid: &OID) -> Option<State> {
        let tree = self.get_tree(oid.kind());
        get_state_rec(tree, oid.full_id().split('/'))
    }
    #[inline]
    fn remove(&mut self, oid: &OID) -> Option<State> {
        let tree = self.get_tree_mut(oid.kind());
        remove_state_rec(tree, oid.full_id().split('/'), oid)
    }
    fn get_by_mask(&self, mask: &OIDMask, filter: &Filter) -> EResult<Vec<State>> {
        if let Some(tp) = mask.kind() {
            if tp == ItemKind::Lmacro {
                return Ok(Vec::new());
            }
            let tree = self.get_tree(tp);
            if let Some(chunks) = mask.chunks() {
                let mut result = Vec::new();
                get_state_by_mask_rec(tree, chunks.iter(), &mut result, filter)?;
                Ok(result)
            } else {
                Ok(tree
                    .members_wildcard
                    .values()
                    .filter(|x| filter.matches(x))
                    .cloned()
                    .collect())
            }
        } else {
            let mut result = Vec::new();
            if let Some(chunks) = mask.chunks() {
                get_state_by_mask_rec(&self.unit, chunks.iter(), &mut result, filter)?;
                get_state_by_mask_rec(&self.sensor, chunks.iter(), &mut result, filter)?;
                get_state_by_mask_rec(&self.lvar, chunks.iter(), &mut result, filter)?;
            } else {
                result.extend(
                    self.unit
                        .members_wildcard
                        .values()
                        .filter(|x| filter.matches(x))
                        .cloned()
                        .collect::<Vec<State>>(),
                );
                result.extend(
                    self.sensor
                        .members_wildcard
                        .values()
                        .filter(|x| filter.matches(x))
                        .cloned()
                        .collect::<Vec<State>>(),
                );
                result.extend(
                    self.lvar
                        .members_wildcard
                        .values()
                        .filter(|x| filter.matches(x))
                        .cloned()
                        .collect::<Vec<State>>(),
                );
            }
            Ok(result)
        }
    }
}

fn get_state_rec(tree: &StateTree, mut sp: Split<char>) -> Option<State> {
    if let Some(chunk) = sp.next() {
        if let Some(child) = tree.childs.get(chunk) {
            get_state_rec(child, sp)
        } else {
            None
        }
    } else if tree.members.is_empty() {
        None
    } else {
        Some(tree.members.values().next().unwrap().clone())
    }
}
fn remove_state_rec(tree: &mut StateTree, mut sp: Split<char>, oid: &OID) -> Option<State> {
    if let Some(chunk) = sp.next() {
        tree.members_wildcard.remove(oid)?;
        let state = if let Some(c) = tree.childs.get_mut(chunk) {
            let state = remove_state_rec(c, sp.clone(), oid)?;
            if c.is_empty() {
                tree.childs.remove(chunk);
            }
            state
        } else {
            return None;
        };
        Some(state)
    } else {
        tree.members.remove(oid)
    }
}

fn get_state_by_mask_rec(
    tree: &StateTree,
    mut iter: std::slice::Iter<&str>,
    result: &mut Vec<State>,
    filter: &Filter,
) -> EResult<()> {
    if let Some(chunk) = iter.next() {
        if *chunk == "#" {
            result.extend(
                tree.members_wildcard
                    .values()
                    .filter(|x| filter.matches(x))
                    .cloned()
                    .collect::<Vec<State>>(),
            );
        } else if *chunk == "+" {
            for child in tree.childs.values() {
                get_state_by_mask_rec(child, iter.clone(), result, filter)?;
            }
        } else if let Some(regex) = chunk.strip_prefix(OID_MASK_PREFIX_REGEX) {
            let re = regex::Regex::new(regex).map_err(Error::invalid_params)?;
            for (name, child) in &tree.childs {
                if re.is_match(name) {
                    get_state_by_mask_rec(child, iter.clone(), result, filter)?;
                }
            }
        } else if let Some(child) = tree.childs.get(*chunk) {
            get_state_by_mask_rec(child, iter, result, filter)?;
        }
    } else {
        result.extend(
            tree.members
                .values()
                .filter(|x| filter.matches(x))
                .cloned()
                .collect::<Vec<State>>(),
        );
    }
    Ok(())
}

fn append_state_rec(
    tree: &mut StateTree,
    mut sp: Split<char>,
    state: &State,
    force: bool,
) -> Result<(), State> {
    macro_rules! process_entry {
        ($entry: expr) => {
            match $entry {
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    let existing = e.get();
                    if existing.ieid() > state.ieid() && !force {
                        return Err(existing.clone());
                    }
                    if existing.meta().is_none() || state.meta().is_some() {
                        e.insert(state.clone());
                    } else {
                        // existing meta is some but new state has no meta
                        let state_with_meta = State(Arc::new(StateInner {
                            meta: existing.meta().cloned().into(),
                            st: state.0.st.clone(),
                        }));
                        e.insert(state_with_meta);
                    }
                }
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert(state.clone());
                }
            }
        };
    }
    if let Some(chunk) = sp.next() {
        process_entry!(tree.members_wildcard.entry(state.oid().clone().into()));
        if let Some(c) = tree.childs.get_mut(chunk) {
            append_state_rec(c, sp.clone(), state, force)?;
        } else {
            let mut child = StateTree::default();
            append_state_rec(&mut child, sp.clone(), state, force)?;
            tree.childs.insert(chunk.to_owned(), child);
        }
        return Ok(());
    }
    process_entry!(tree.members.entry(state.oid().clone().into()));
    Ok(())
}

#[cfg(test)]
mod test {

    use crate::types::FullItemStateConnected;
    use eva_common::{IEID, OID, acl::OIDMask, events::LocalStateEvent, value::Value};

    use super::{FullItemStateConnectedWithMeta, Query};

    use super::Db;

    #[tokio::test]
    async fn test_get_query() {
        let db = Db::new();
        let oid: OID = "sensor:tests/t1".parse().unwrap();
        let ev = LocalStateEvent {
            status: 1,
            value: 123u8.into(),
            act: None,
            ieid: IEID::new(1, 0),
            t: 123.0,
        };
        let st = FullItemStateConnected::from_local_state_event(ev, oid);
        db.state_db.write().await.record(st.into()).unwrap();
        let oid: OID = "sensor:t5".parse().unwrap();
        let ev = LocalStateEvent {
            status: 1,
            value: 456u16.into(),
            act: None,
            ieid: IEID::new(1, 0),
            t: 123.0,
        };
        let st = FullItemStateConnected::from_local_state_event(ev, oid);
        db.state_db.write().await.record(st.into()).unwrap();
        let st = db.get(&"sensor:tests/t1".parse().unwrap()).await.unwrap();
        let st = st.expect("state not found");
        assert!(st.oid() == &"sensor:tests/t1".parse().unwrap());
        assert!(st.status() == 1);
        assert!(st.value() == &123u8.into());
        let st = db.get(&"sensor:t5".parse().unwrap()).await.unwrap();
        let st = st.expect("state not found");
        assert!(st.oid() == &"sensor:t5".parse().unwrap());
        assert!(st.status() == 1);
        assert!(st.value() == &456u16.into());
        let mask = "sensor:#".parse::<OIDMask>().unwrap();
        let q = Query::new(&mask).local();
        let states = db.query(q).await.unwrap();
        assert_eq!(states.len(), 2);
        assert!(
            states
                .iter()
                .any(|s| s.oid() == &"sensor:tests/t1".parse().unwrap())
        );
        assert!(
            states
                .iter()
                .any(|s| s.oid() == &"sensor:t5".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn test_push_state_without_meta_update_meta() {
        let db = Db::new();
        let oid: OID = "sensor:tests/t1".parse().unwrap();
        let mut ev = LocalStateEvent {
            status: 1,
            value: 123u8.into(),
            act: None,
            ieid: IEID::new(1, 0),
            t: 123.0,
        };
        let meta = Value::String("Hello world".into());
        let st = FullItemStateConnectedWithMeta {
            st: FullItemStateConnected::from_local_state_event(ev.clone(), oid.clone()),
            meta: Some(meta.clone()),
        };
        db.state_db
            .write()
            .await
            .record_state_connected_with_meta(st);
        ev.value = 456u16.into();
        ev.ieid = IEID::new(1, 1);
        let st = FullItemStateConnected::from_local_state_event(ev.clone(), oid.clone());
        db.state_db.write().await.record(st.into()).unwrap();
        let st = db.get(&oid).await.unwrap();
        let st = st.expect("state not found");
        assert!(st.oid() == &oid);
        assert!(st.status() == 1);
        assert!(st.value() == &456u16.into());
        assert_eq!(st.meta().unwrap(), &meta);
        let meta = Value::String("New meta".into());
        ev.ieid = IEID::new(1, 2);
        let st = FullItemStateConnectedWithMeta {
            st: FullItemStateConnected::from_local_state_event(ev, oid.clone()),
            meta: Some(meta.clone()),
        };
        db.state_db
            .write()
            .await
            .record_state_connected_with_meta(st);
        let st = db.get(&oid).await.unwrap();
        let st = st.expect("state not found");
        assert!(st.oid() == &oid);
        assert!(st.status() == 1);
        assert!(st.value() == &456u16.into());
        assert_eq!(st.meta().unwrap(), &meta);
    }

    #[tokio::test]
    async fn test_push_older_state() {
        let db = Db::new();
        let oid: OID = "sensor:tests/t1".parse().unwrap();
        let mut ev = LocalStateEvent {
            status: 1,
            value: 123u8.into(),
            act: None,
            ieid: IEID::new(1, 1),
            t: 123.0,
        };
        let st: FullItemStateConnected =
            FullItemStateConnected::from_local_state_event(ev.clone(), oid.clone());
        db.state_db.write().await.record(st.into()).unwrap();
        ev.value = 456u16.into();
        ev.ieid = IEID::new(1, 0); // older ieid
        let st: FullItemStateConnected =
            FullItemStateConnected::from_local_state_event(ev.clone(), oid.clone());
        let r = db.state_db.write().await.record(st.into());
        assert!(r.is_err());
        let st = db.get(&oid).await.unwrap();
        let st = st.expect("state not found");
        assert!(st.oid() == &oid);
        assert!(st.status() == 1);
        assert!(st.value() == &123u8.into());
    }
}
