use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet},
    ops::{Bound, Deref},
    sync::Arc,
};

use compact_str::CompactString;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct Object {
    pub namespace: CompactString,
    pub id: CompactString,
}

#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
pub struct ObjectRef<'a> {
    pub namespace: &'a str,
    pub id: &'a str,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum ObjectOrSet {
    Object(Object),
    Set((Object, Relation)),
}
#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct Relation(pub CompactString);

type S = ObjectOrSet;
type R = Relation;
type D = Object;

pub struct RelationSet {
    src_to_dst: BTreeMap<Arc<S>, HashMap<Arc<R>, HashSet<Arc<D>>>>,
    dst_to_src: BTreeMap<Arc<D>, HashMap<Arc<R>, HashSet<Arc<S>>>>,
}

impl RelationSet {
    pub fn new() -> Self {
        Self {
            src_to_dst: BTreeMap::new(),
            dst_to_src: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, src: impl Into<S>, rel: impl Into<R>, dst: impl Into<D>) {
        let src = Arc::new(src.into());
        let rel = Arc::new(rel.into());
        let dst = Arc::new(dst.into());

        if let Some(rels_dsts) = self.src_to_dst.get_mut(&src) {
            if let Some(dsts) = rels_dsts.get_mut(&rel) {
                dsts.insert(dst.clone());
            } else {
                let mut dsts = HashSet::new();
                dsts.insert(dst.clone());
                rels_dsts.insert(rel.clone(), dsts);
            }
        } else {
            let mut rels_dsts = HashMap::new();
            let mut dsts = HashSet::new();
            dsts.insert(dst.clone());
            rels_dsts.insert(rel.clone(), dsts);
            self.src_to_dst.insert(src.clone(), rels_dsts);
        }

        if let Some(rels_srcs) = self.dst_to_src.get_mut(&dst) {
            if let Some(srcs) = rels_srcs.get_mut(&rel) {
                srcs.insert(src.clone());
            } else {
                let mut srcs = HashSet::new();
                srcs.insert(src.clone());
                rels_srcs.insert(rel.clone(), srcs);
            }
        } else {
            let mut rels_srcs = HashMap::new();
            let mut srcs = HashSet::new();
            srcs.insert(src.clone());
            rels_srcs.insert(rel.clone(), srcs);
            self.dst_to_src.insert(dst.clone(), rels_srcs);
        }
    }

    pub fn remove(&mut self, src: impl Into<S>, rel: impl Into<R>, dst: impl Into<D>) {
        let src = src.into();
        let rel = rel.into();
        let dst = dst.into();

        if let Some(dsts) = self
            .src_to_dst
            .get_mut(&src)
            .and_then(|rels_dsts| rels_dsts.get_mut(&rel))
        {
            dsts.remove(&dst);
        }

        if let Some(srcs) = self
            .dst_to_src
            .get_mut(&dst)
            .and_then(|rels_srcs| rels_srcs.get_mut(&rel))
        {
            srcs.remove(&src);
        }
    }

    pub fn remove_by_src(&mut self, src: &S) {
        for (rel, dsts) in self.src_to_dst.remove(src).iter().flat_map(|x| x.iter()) {
            for dst in dsts {
                if let Some(srcs) = self
                    .dst_to_src
                    .get_mut(dst)
                    .and_then(|rels_srcs| rels_srcs.get_mut(rel))
                {
                    srcs.remove(src);
                }
            }
        }
    }

    pub fn remove_by_dst(&mut self, dst: &D) {
        for (rel, srcs) in self.dst_to_src.remove(dst).iter().flat_map(|x| x.iter()) {
            for src in srcs {
                if let Some(dsts) = self
                    .src_to_dst
                    .get_mut(src)
                    .and_then(|rels_dsts| rels_dsts.get_mut(rel))
                {
                    dsts.remove(dst);
                }
            }
        }
    }

    pub fn has(&self, src: impl Into<S>, rel: impl Into<R>, dst: impl Into<D>) -> bool {
        let src = src.into();
        let rel = rel.into();
        let dst = dst.into();

        self.src_to_dst
            .get(&src)
            .and_then(|rels_dsts| rels_dsts.get(&rel))
            .and_then(|dsts| dsts.get(&dst))
            .is_some()
    }

    pub fn has_object<'a>(&self, obj: impl Into<&'a Object>) -> bool {
        let obj = obj.into();
        let has_dst_obj = self.dst_to_src.contains_key(obj);

        let cursor = self
            .src_to_dst
            .lower_bound(Bound::Included(&ObjectOrSet::Object(obj.clone())));

        let has_src_obj = if let Some(key) = cursor.key() {
            obj.namespace == key.object().namespace && obj.id == key.object().id
        } else {
            false
        };

        has_dst_obj || has_src_obj
    }

    pub fn has_recursive(
        &self,
        src: impl Into<S>,
        rel: impl Into<R>,
        dst: impl Into<D>,
        limit: u32,
    ) -> bool {
        let src = src.into();
        let rel = rel.into();
        let dst = dst.into();

        let mut dist: HashMap<(Arc<Object>, Arc<Relation>), u32> = HashMap::new();
        let mut q: BinaryHeap<Distanced<(Arc<Object>, Arc<Relation>)>> = BinaryHeap::new();

        for (nrel, ndst) in self
            .src_to_dst
            .get(&src)
            .iter()
            .flat_map(|x| x.iter())
            .flat_map(|(r, d)| d.iter().map(|d| (r.clone(), d.clone())))
        {
            if *nrel == rel && *ndst == dst {
                return true;
            }
            dist.insert((ndst.clone(), nrel.clone()), 1);
            q.push(Distanced::one((ndst, nrel)));
        }

        while let Some(distanced) = q.pop() {
            let node_dist = distanced.distance() + 1;
            let node = ObjectOrSet::Set(((*distanced.0).clone(), (*distanced.1).clone()));
            for (nrel, ndst) in self
                .src_to_dst
                .get(&node)
                .iter()
                .flat_map(|x| x.iter())
                .flat_map(|(r, d)| d.iter().map(|d| (r.clone(), d.clone())))
            {
                if *nrel == rel && *ndst == dst {
                    return true;
                }
                if let Some(existing_node_dist) = dist.get(&*distanced) {
                    if *existing_node_dist <= node_dist || node_dist >= limit {
                        continue;
                    }
                }
                dist.insert((ndst.clone(), nrel.clone()), node_dist);
                q.push(Distanced::one((ndst, nrel)));
            }
        }
        false
    }

    pub async fn to_file(&self, file: &mut File) {
        for (dst, rels_srcs) in self.dst_to_src.iter() {
            file.write_all(format!("[{}:{}]\n", &dst.namespace, &dst.id).as_bytes())
                .await
                .unwrap();
            for (rel, srcs) in rels_srcs.iter() {
                let srcs = srcs
                    .iter()
                    .map(|src| {
                        let src_obj = src.object();
                        let src_str = if src_obj.namespace == dst.namespace && src_obj.id == dst.id
                        {
                            "self".to_string()
                        } else {
                            format!("{}:{}", src_obj.namespace, src_obj.id)
                        };
                        match &**src {
                            ObjectOrSet::Object(_) => src_str,
                            ObjectOrSet::Set(set) => {
                                format!("{}#{}", src_str, set.1 .0)
                            }
                        }
                    })
                    .reduce(|acc, x| acc + ", " + &x)
                    .unwrap_or_default();

                file.write_all(format!("{} = [{}]\n", &rel.0, &srcs).as_bytes())
                    .await
                    .unwrap();
            }
            file.write_all("\n".as_bytes()).await.unwrap();
        }
    }
    pub async fn from_file(file: &mut File) -> Self {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut graph = Self::new();
        let mut node: Option<(String, String)> = None;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.starts_with('[') && line.ends_with(']') {
                let line = &mut line[1..line.len() - 1].split(':');
                let namespace = line.next().unwrap();
                let id = line.next().unwrap();
                node = Some((namespace.to_string(), id.to_string()));
            } else if line.contains('=') && line.contains('[') && line.contains(']') {
                if let Some(dst) = &node {
                    let equals_pos = line.find('=').unwrap();
                    let arr_start = line.find('[').unwrap();
                    let arr_stop = line.find(']').unwrap();

                    let rel = line[..equals_pos].trim();
                    let arr = line[arr_start + 1..arr_stop].split(", ");

                    for obj in arr {
                        let src: ObjectOrSet = if obj.contains('#') {
                            let sep_1 = obj.find(':');
                            let sep_2 = obj.find('#').unwrap();

                            let (namespace, id) = if let Some(sep_1) = sep_1 {
                                (&obj[..sep_1], &obj[sep_1 + 1..sep_2])
                            } else {
                                (dst.0.as_str(), dst.1.as_str())
                            };

                            let rel = &obj[sep_2 + 1..];

                            (namespace, id, rel).into()
                        } else {
                            let sep_1 = obj.find(':');

                            let (namespace, id) = if let Some(sep_1) = sep_1 {
                                (&obj[..sep_1], &obj[sep_1 + 1..])
                            } else {
                                (dst.0.as_str(), dst.1.as_str())
                            };
                            (namespace, id).into()
                        };

                        graph.insert(src, rel, dst.clone());
                    }
                }
            }
        }
        graph
    }
}

#[derive(PartialEq, Eq)]
struct Distanced<T> {
    distance: u32,
    data: T,
}

impl<T> Distanced<T> {
    pub fn one(data: T) -> Self {
        Self { distance: 1, data }
    }
    pub fn distance(&self) -> u32 {
        self.distance
    }
}

impl<T> Deref for Distanced<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: PartialEq> PartialOrd for Distanced<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&other.distance)
    }
}
impl<T: Eq> Ord for Distanced<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.cmp(&other.distance)
    }
}

impl PartialOrd for Relation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}
impl Ord for Relation {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for ObjectOrSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (
            self.object().partial_cmp(other.object()),
            self.relation(),
            other.relation(),
        ) {
            (Some(Ordering::Equal), self_rel, other_rel) => self_rel.partial_cmp(&other_rel),
            (ord, _, _) => ord,
        }
    }
}
impl Ord for ObjectOrSet {
    fn cmp(&self, other: &Self) -> Ordering {
        self.object()
            .cmp(other.object())
            .then(self.relation().cmp(&other.relation()))
    }
}

impl PartialOrd for Object {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.namespace.partial_cmp(&other.namespace) {
            Some(core::cmp::Ordering::Equal) => self.id.partial_cmp(&other.id),
            ord => ord,
        }
    }
}
impl Ord for Object {
    fn cmp(&self, other: &Self) -> Ordering {
        self.namespace
            .cmp(&other.namespace)
            .then(self.id.cmp(&other.id))
    }
}

impl From<(&str, &str)> for ObjectOrSet {
    fn from((namespace, id): (&str, &str)) -> Self {
        ObjectOrSet::Object(Object {
            namespace: namespace.into(),
            id: id.into(),
        })
    }
}
impl From<(&str, &str, &str)> for ObjectOrSet {
    fn from((namespace, id, rel): (&str, &str, &str)) -> Self {
        ObjectOrSet::Set(((namespace, id).into(), Relation(rel.into())))
    }
}

impl From<(&str, &str)> for Object {
    fn from((namespace, id): (&str, &str)) -> Self {
        Self {
            namespace: namespace.into(),
            id: id.into(),
        }
    }
}
impl From<(String, String)> for Object {
    fn from((namespace, id): (String, String)) -> Self {
        Self {
            namespace: namespace.into(),
            id: id.into(),
        }
    }
}

impl From<&str> for Relation {
    fn from(value: &str) -> Self {
        Relation(value.into())
    }
}
impl From<String> for Relation {
    fn from(value: String) -> Self {
        Relation(value.into())
    }
}

impl ObjectOrSet {
    pub fn object(&self) -> &Object {
        match self {
            ObjectOrSet::Object(obj) => obj,
            ObjectOrSet::Set((obj, _)) => obj,
        }
    }
    pub fn relation(&self) -> Option<&Relation> {
        match self {
            ObjectOrSet::Object(_) => None,
            ObjectOrSet::Set((_, rel)) => Some(rel),
        }
    }
}
impl Relation {
    pub fn new(relation: &str) -> Self {
        Self(relation.into())
    }
}
