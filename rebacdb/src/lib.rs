#![doc = include_str!("../README.md")]
use std::{
    borrow::{Borrow, Cow},
    cmp::Ordering,
    collections::{BTreeSet, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt},
    sync::RwLock,
};

#[cfg(test)]
mod tests;

const WILDCARD_ID: &str = "*";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
struct VertexId {
    namespace: String,
    id: String,
    relation: Option<String>,
}
/// shared version of [`VertexId`]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
struct VertexIdRef<'a> {
    namespace: &'a str,
    id: &'a str,
    relation: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectOrSet<'a> {
    Object(Cow<'a, Object>),
    Set(Cow<'a, Set>),
}

/// representation of a an object and a relation (e.g. (`file`, `foo.pdf`, `read`))
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Set(VertexId);

/// representation of an object (e.g. (`user`, `alice`))
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object(VertexId);

struct Vertex {
    id: VertexId,
    edges_in: RwLock<HashSet<Arc<Vertex>>>,
    edges_out: RwLock<HashSet<Arc<Vertex>>>,
}

/// graph-based database implementation
#[derive(Default)]
pub struct RelationGraph {
    /// all verticies of the graph
    verticies: RwLock<BTreeSet<Arc<Vertex>>>,
}

trait VertexIdentifier {
    fn namespace(&self) -> &str;
    fn id(&self) -> &str;
    fn relation(&self) -> Option<&str>;
    fn vertex_id(&self) -> &VertexId;
}

impl Object {
    pub fn new(namespace: String, id: String) -> Self {
        Self(VertexId {
            namespace,
            id,
            relation: None,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.0.namespace
    }

    pub fn id(&self) -> &str {
        &self.0.id
    }

    fn vertex_id(&self) -> &VertexId {
        &self.0
    }
}

impl Set {
    pub fn new(namespace: String, id: String, relation: String) -> Self {
        Self(VertexId {
            namespace,
            id,
            relation: Some(relation),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.0.namespace
    }

    pub fn id(&self) -> &str {
        &self.0.id
    }

    pub fn relation(&self) -> &str {
        self.0.relation.as_deref().unwrap_or("")
    }

    fn vertex_id(&self) -> &VertexId {
        &self.0
    }
}

impl<'a> ObjectOrSet<'a> {
    pub fn namespace(&self) -> &str {
        match self {
            Self::Object(obj) => obj.namespace(),
            Self::Set(set) => set.namespace(),
        }
    }
    pub fn id(&self) -> &str {
        match self {
            Self::Object(obj) => obj.id(),
            Self::Set(set) => set.id(),
        }
    }

    pub fn relation(&self) -> Option<&str> {
        match self {
            Self::Object(_) => None,
            Self::Set(set) => set.0.relation.as_deref(),
        }
    }

    fn vertex_id(&self) -> &VertexId {
        match self {
            Self::Object(obj) => obj.vertex_id(),
            Self::Set(set) => set.vertex_id(),
        }
    }
}

impl RelationGraph {
    /// create a new relation between from a [`Object`] or [`Set`] to a [`Set`]
    pub async fn insert(&self, src: impl Into<ObjectOrSet<'_>>, dst: &Set) {
        let src: ObjectOrSet<'_> = src.into();
        let mut verticies = self.verticies.write().await;

        let mut get_or_create = |vertex: &VertexId| match verticies.get(vertex) {
            Some(vertex) => vertex.clone(),
            None => {
                let vertex = Arc::new(Vertex {
                    id: vertex.clone(),
                    edges_out: RwLock::new(HashSet::new()),
                    edges_in: RwLock::new(HashSet::new()),
                });
                verticies.insert(vertex.clone());
                vertex
            }
        };

        let src_without_relation = src.relation().is_none();

        let src_wildcard: ObjectOrSet = (src.namespace(), WILDCARD_ID, src.relation()).into();
        let src_wildcard = get_or_create(src_wildcard.vertex_id());
        let src_vertex = get_or_create(src.vertex_id());

        let dst_wildcard: Set = (dst.namespace(), WILDCARD_ID, dst.relation()).into();
        let dst_wildcard = get_or_create(dst_wildcard.vertex_id());
        let dst_vertex = get_or_create(dst.vertex_id());

        if src_without_relation && src_vertex.id.id != WILDCARD_ID {
            add_edge(src_vertex.clone(), src_wildcard).await;
        } else if !src_without_relation {
            add_edge(src_wildcard, src_vertex.clone()).await;
        }

        add_edge(dst_wildcard, dst_vertex.clone()).await;
        add_edge(src_vertex, dst_vertex).await;
    }

    /// remove a relation
    pub async fn remove(&self, src: impl Into<ObjectOrSet<'_>>, dst: &Set) {
        let src: ObjectOrSet<'_> = src.into();
        let mut verticies = self.verticies.write().await;

        let src = verticies.get(src.vertex_id()).cloned();
        let dst = verticies.get(dst.vertex_id()).cloned();

        if let (Some(src), Some(dst)) = (src, dst) {
            src.edges_out.write().await.retain(|x| x != &dst);
            dst.edges_in.write().await.retain(|x| x != &src);

            if src.edges_in.read().await.is_empty() && src.edges_out.read().await.is_empty() {
                verticies.remove(&src.id);
            }
            if dst.edges_in.read().await.is_empty() && dst.edges_out.read().await.is_empty() {
                verticies.remove(&dst.id);
            }
        }
    }

    /// checks if there is a *direct* relation between `src` and `dst`
    pub async fn has(&self, src: impl Into<ObjectOrSet<'_>>, dst: &Set) -> bool {
        let src: ObjectOrSet<'_> = src.into();
        let (src, dst) = {
            let verticies = self.verticies.read().await;
            (
                verticies.get(src.vertex_id()).cloned(),
                verticies.get(dst.vertex_id()).cloned(),
            )
        };

        if let (Some(src), Some(dst)) = (src, dst) {
            src.edges_out.read().await.contains(&dst)
        } else {
            false
        }
    }

    /// checks if there is a *path* between src and dst using [BFS](https://en.wikipedia.org/wiki/Breadth-first_search)
    ///
    /// # Arguments
    /// * `src` - start of the path
    /// * `dst` - end of the path
    /// * `limit` - optional maximum search depth of the search before returing false
    pub async fn check<'a>(
        &self,
        src: impl Into<ObjectOrSet<'_>>,
        dst: &Set,
        limit: Option<u32>,
    ) -> bool {
        let src: ObjectOrSet<'_> = src.into();
        let mut distance = 1;

        let mut neighbors: Vec<Arc<Vertex>> = if let Some(src) =
            self.verticies.read().await.get(src.vertex_id())
        {
            src.edges_out.read().await.iter().cloned().collect()
        } else {
            let wildcard_src: Object = (src.namespace(), WILDCARD_ID).into();
            if let Some(wildcard_src) = self.verticies.read().await.get(wildcard_src.vertex_id()) {
                wildcard_src
                    .edges_out
                    .read()
                    .await
                    .iter()
                    .cloned()
                    .collect()
            } else {
                return false;
            }
        };

        let mut visited: HashSet<Arc<Vertex>> = HashSet::new();

        while !neighbors.is_empty() {
            if let Some(limit) = limit {
                if distance > limit {
                    return false;
                }
            }

            let mut next_neighbors = vec![];
            for neighbor in neighbors {
                if distance > 1 && visited.contains(&neighbor) {
                    continue;
                }

                //check if the current vertex is the dst vertex or the wildcard vertex for the dst
                //namespace. Without checking the wildcard vertex, not initialized dsts that should
                //be affected by the wildcard wouldn't be found.
                if &neighbor.id == dst
                    || (neighbor.id.namespace == dst.namespace()
                        && neighbor.id.id == WILDCARD_ID
                        && neighbor.id.relation.as_deref() == Some(dst.relation()))
                {
                    return true;
                }

                let mut vertex_neighbors =
                    neighbor.edges_out.read().await.iter().cloned().collect();
                next_neighbors.append(&mut vertex_neighbors);

                visited.insert(neighbor);
            }
            neighbors = next_neighbors;
            distance += 1;
        }
        false
    }

    /// get all objects that are related to dst with the relation path
    pub async fn expand(&self, dst: &Set) -> Vec<(Object, Vec<Set>)> {
        let start_vertex = {
            let verticies = self.verticies.read().await;
            match verticies.get(dst.vertex_id()) {
                Some(v) => v.clone(),
                None => {
                    let wildcard_dst: Set = (dst.namespace(), WILDCARD_ID, dst.relation()).into();

                    match verticies.get(wildcard_dst.vertex_id()) {
                        Some(v) => v.clone(),
                        None => return vec![],
                    }
                }
            }
        };

        let mut visited: HashSet<Arc<Vertex>> = HashSet::new();

        let mut neighbors: Vec<(Arc<Vertex>, Vec<Arc<Vertex>>)> = start_vertex
            .edges_in
            .read()
            .await
            .iter()
            .map(|v| (v.clone(), vec![start_vertex.clone()]))
            .collect();

        visited.insert(start_vertex);

        let mut expanded_verticies: Vec<(Arc<Vertex>, Vec<Arc<Vertex>>)> = vec![];

        while !neighbors.is_empty() {
            let mut next_neighbors = vec![];
            for (neighbor, mut neighbor_path) in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }

                if neighbor.id.relation.is_none() {
                    expanded_verticies.push((neighbor, neighbor_path));
                    continue;
                }

                neighbor_path.push(neighbor.clone());

                next_neighbors.append(
                    &mut neighbor
                        .edges_in
                        .read()
                        .await
                        .iter()
                        .map(|v| (v.clone(), neighbor_path.clone()))
                        .collect(),
                );

                visited.insert(neighbor);
            }
            neighbors = next_neighbors;
        }

        expanded_verticies
            .into_iter()
            .map(|(v, path)| {
                (
                    Object(v.id.clone()),
                    path.into_iter().map(|w| Set(w.id.clone())).collect(),
                )
            })
            .collect()
    }

    /// write graph to file
    pub async fn write_savefile(&self, writeable: &mut (impl AsyncWriteExt + Unpin)) {
        let mut current: (String, String) = (String::new(), String::new());
        for vertex in self.verticies.read().await.iter() {
            if current != (vertex.id.namespace.clone(), vertex.id.id.clone()) {
                current = (vertex.id.namespace.clone(), vertex.id.id.clone());
                writeable.write_all("\n".as_bytes()).await.unwrap();
                writeable
                    .write_all(format!("[{}:{}]\n", &current.0, &current.1).as_bytes())
                    .await
                    .unwrap();
            }

            let srcs = vertex
                .edges_in
                .read()
                .await
                .iter()
                .filter(|x| x.id.id != WILDCARD_ID)
                .map(|src| {
                    let obj = if src.id.namespace == current.0 && src.id.id == current.1 {
                        "self".to_string()
                    } else {
                        format!("{}:{}", &src.id.namespace, &src.id.id)
                    };

                    if let Some(rel) = &src.id.relation {
                        format!("{}#{}", &obj, &rel)
                    } else {
                        obj
                    }
                })
                .reduce(|acc, x| acc + ", " + &x)
                .unwrap_or_default();

            if let Some(rel) = &vertex.id.relation {
                writeable
                    .write_all(format!("{} = [ {} ]\n", &rel, &srcs).as_bytes())
                    .await
                    .unwrap();
            }
        }
    }
    /// read graph from file
    pub async fn read_savefile(readable: &mut (impl AsyncBufReadExt + Unpin)) -> Self {
        let mut lines = readable.lines();
        let graph = Self::default();
        let mut vertex: Option<(String, String)> = None;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.starts_with('[') && line.ends_with(']') {
                let line = &mut line[1..line.len() - 1].split(':');
                let namespace = line.next().unwrap();
                let id = line.next().unwrap();
                vertex = Some((namespace.to_string(), id.to_string()));
            } else if line.contains('=') && line.contains('[') && line.contains(']') {
                if let Some(dst) = &vertex {
                    let equals_pos = line.find('=').unwrap();
                    let arr_start = line.find('[').unwrap();
                    let arr_stop = line.find(']').unwrap();

                    let rel = line[..equals_pos].trim();
                    let arr = line[arr_start + 1..arr_stop].trim().split(", ");

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

                            ObjectOrSet::Set(Cow::Owned((namespace, id, rel).into()))
                        } else {
                            let sep_1 = obj.find(':');

                            let (namespace, id) = if let Some(sep_1) = sep_1 {
                                (&obj[..sep_1], &obj[sep_1 + 1..])
                            } else {
                                (dst.0.as_str(), dst.1.as_str())
                            };
                            ObjectOrSet::Object(Cow::Owned((namespace, id).into()))
                        };

                        graph
                            .insert(src, &(dst.0.as_str(), dst.1.as_str(), rel).into())
                            .await;
                    }
                }
            }
        }
        graph
    }
}

impl Debug for Vertex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("vertex").field("id", &self.id).finish()
    }
}

async fn add_edge(from: Arc<Vertex>, to: Arc<Vertex>) {
    if !from.edges_out.read().await.contains(&to) {
        from.edges_out.write().await.insert(to.clone());
    }
    if !to.edges_in.read().await.contains(&from) {
        to.edges_in.write().await.insert(from);
    }
}

impl Borrow<VertexId> for Arc<Vertex> {
    fn borrow(&self) -> &VertexId {
        &self.id
    }
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Vertex {}

impl PartialOrd for Vertex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Vertex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for Vertex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl From<(&str, &str)> for Object {
    fn from(value: (&str, &str)) -> Self {
        Self(VertexId {
            namespace: value.0.to_string(),
            id: value.1.to_string(),
            relation: None,
        })
    }
}

impl From<(String, String)> for Object {
    fn from(value: (String, String)) -> Self {
        Self(VertexId {
            namespace: value.0,
            id: value.1,
            relation: None,
        })
    }
}

impl From<(&str, &str, &str)> for Set {
    fn from(value: (&str, &str, &str)) -> Self {
        Self(VertexId {
            namespace: value.0.to_string(),
            id: value.1.to_string(),
            relation: Some(value.2.to_string()),
        })
    }
}

impl From<(String, String, String)> for Set {
    fn from(value: (String, String, String)) -> Self {
        Self(VertexId {
            namespace: value.0,
            id: value.1,
            relation: Some(value.2),
        })
    }
}

impl From<(&str, &str, Option<&str>)> for ObjectOrSet<'_> {
    fn from(value: (&str, &str, Option<&str>)) -> Self {
        match value.2 {
            Some(r) => Self::Set(Cow::Owned((value.0, value.1, r).into())),
            None => Self::Object(Cow::Owned((value.0, value.1).into())),
        }
    }
}

impl From<(String, String, Option<String>)> for ObjectOrSet<'_> {
    fn from(value: (String, String, Option<String>)) -> Self {
        match value.2 {
            Some(r) => Self::Set(Cow::Owned((value.0, value.1, r).into())),
            None => Self::Object(Cow::Owned((value.0, value.1).into())),
        }
    }
}

impl Borrow<VertexId> for Set {
    fn borrow(&self) -> &VertexId {
        &self.0
    }
}

impl PartialEq<VertexId> for Set {
    fn eq(&self, other: &VertexId) -> bool {
        self.0.eq(other)
    }
}
impl PartialEq<Set> for VertexId {
    fn eq(&self, other: &Set) -> bool {
        self.eq(&other.0)
    }
}

impl Borrow<VertexId> for Object {
    fn borrow(&self) -> &VertexId {
        &self.0
    }
}

impl From<Set> for ObjectOrSet<'_> {
    fn from(value: Set) -> Self {
        Self::Set(Cow::Owned(value))
    }
}
impl From<Object> for ObjectOrSet<'_> {
    fn from(value: Object) -> Self {
        Self::Object(Cow::Owned(value))
    }
}

impl<'a> From<&'a Set> for ObjectOrSet<'a> {
    fn from(value: &'a Set) -> Self {
        Self::Set(Cow::Borrowed(value))
    }
}
impl<'a> From<&'a Object> for ObjectOrSet<'a> {
    fn from(value: &'a Object) -> Self {
        Self::Object(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a ObjectOrSet<'a>> for ObjectOrSet<'a> {
    fn from(value: &'a ObjectOrSet<'a>) -> Self {
        match value {
            Self::Object(obj) => Self::Object(Cow::Borrowed(obj.borrow())),
            Self::Set(set) => Self::Set(Cow::Borrowed(set.borrow())),
        }
    }
}
