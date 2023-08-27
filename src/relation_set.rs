use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, HashSet},
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::Arc,
};

use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::RwLock,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId {
    pub namespace: String,
    pub id: String,
    pub relation: Option<String>,
}

pub struct Node {
    pub id: NodeId,
    pub edges_in: RwLock<Vec<Arc<Node>>>,
    pub edges_out: RwLock<Vec<Arc<Node>>>,
}

#[derive(Debug, PartialEq, Eq)]
struct Distanced<T> {
    distance: u32,
    data: T,
}

#[derive(Default)]
pub struct RelationSet {
    nodes: RwLock<BTreeSet<Arc<Node>>>,
}

impl RelationSet {
    pub async fn insert(&self, src: impl Into<NodeId>, dst: impl Into<NodeId>) {
        let src = src.into();
        let dst = dst.into();

        let mut nodes = self.nodes.write().await;

        let src_node = match nodes.get(&src) {
            Some(node) => node.clone(),
            None => {
                let node = Arc::new(Node {
                    id: src,
                    edges_out: RwLock::new(vec![]),
                    edges_in: RwLock::new(vec![]),
                });
                nodes.insert(node.clone());
                node
            }
        };
        let dst_node = match nodes.get(&dst).cloned() {
            Some(node) => node.clone(),
            None => {
                let node = Arc::new(Node {
                    id: dst,
                    edges_out: RwLock::new(vec![]),
                    edges_in: RwLock::new(vec![]),
                });
                nodes.insert(node.clone());
                node
            }
        };
        add_edge(src_node, dst_node).await;
    }

    pub async fn remove(&self, src: impl Into<NodeId>, dst: impl Into<NodeId>) {
        let src = src.into();
        let dst = dst.into();

        let mut nodes = self.nodes.write().await;

        let src = nodes.get(&src).cloned();
        let dst = nodes.get(&dst).cloned();

        if let (Some(src), Some(dst)) = (src, dst) {
            src.edges_out.write().await.retain(|x| x != &dst);
            dst.edges_in.write().await.retain(|x| x != &src);

            if src.edges_in.read().await.is_empty() && src.edges_out.read().await.is_empty() {
                nodes.remove(&src.id);
            }
            if dst.edges_in.read().await.is_empty() && dst.edges_out.read().await.is_empty() {
                nodes.remove(&dst.id);
            }
        }
    }

    pub async fn has(&self, src: impl Into<NodeId>, dst: impl Into<NodeId>) -> bool {
        let src = src.into();
        let dst = dst.into();

        let (src, dst) = {
            let nodes = self.nodes.read().await;
            (nodes.get(&src).cloned(), nodes.get(&dst).cloned())
        };

        if let (Some(src), Some(dst)) = (src, dst) {
            src.edges_out.read().await.contains(&dst)
        } else {
            false
        }
    }

    pub async fn has_recursive<'a>(
        &self,
        src: impl Into<NodeId>,
        dst: impl Into<NodeId>,
        limit: Option<u32>,
    ) -> bool {
        let src = src.into();
        let dst = dst.into();

        let src = self.nodes.read().await.get(&src).unwrap().clone();

        let src_neighbors = src
            .edges_out
            .read()
            .await
            .iter()
            .map(|x| Distanced::one(x.clone()))
            .collect::<Vec<_>>();

        let mut q: BinaryHeap<Distanced<Arc<Node>>> = BinaryHeap::from(src_neighbors);
        let mut visited: HashSet<Arc<Node>> = HashSet::new();

        while let Some(distanced) = q.pop() {
            if distanced.id == dst {
                return true;
            }
            if let Some(limit) = limit {
                if distanced.distance() > limit {
                    return false;
                }
            }

            for neighbor in distanced.edges_out.read().await.iter() {
                if !visited.contains(neighbor) {
                    q.push(Distanced::new(neighbor.clone(), distanced.distance() + 1))
                }
            }

            visited.insert(distanced.clone());
        }
        false
    }

    pub async fn to_file(&self, file: &mut File) {
        let mut current: (String, String) = (String::new(), String::new());
        for node in self.nodes.read().await.iter() {
            if current != (node.id.namespace.clone(), node.id.id.clone()) {
                current = (node.id.namespace.clone(), node.id.id.clone());
                file.write_all("\n".as_bytes()).await.unwrap();
                file.write_all(format!("[{}:{}]\n", &current.0, &current.1).as_bytes())
                    .await
                    .unwrap();
            }

            let srcs = node
                .edges_in
                .read()
                .await
                .iter()
                .map(|src| {
                    if src.id.namespace == current.0 && src.id.id == current.1 {
                        "self".to_string()
                    } else if let Some(rel) = &src.id.relation {
                        format!("{}:{}#{}", &src.id.namespace, &src.id.id, &rel)
                    } else {
                        format!("{}:{}", &src.id.namespace, &src.id.id)
                    }
                })
                .reduce(|acc, x| acc + ", " + &x)
                .unwrap_or_default();

            if let Some(rel) = &node.id.relation {
                file.write_all(format!("{} = [ {} ]\n", &rel, &srcs).as_bytes())
                    .await
                    .unwrap();
            }
        }
    }
    pub async fn from_file(file: &mut File) -> Self {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let graph = Self::default();
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
                    let arr = line[arr_start + 1..arr_stop].trim().split(", ");

                    for obj in arr {
                        let src: NodeId = if obj.contains('#') {
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

                        graph
                            .insert(src, (dst.0.as_str(), dst.1.as_str(), rel))
                            .await;
                    }
                }
            }
        }
        graph
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node").field("id", &self.id).finish()
    }
}

async fn add_edge(from: Arc<Node>, to: Arc<Node>) {
    from.edges_out.write().await.push(to.clone());
    to.edges_in.write().await.push(from);
}

impl Borrow<NodeId> for Arc<Node> {
    fn borrow(&self) -> &NodeId {
        &self.id
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}
impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<T> Distanced<T> {
    pub fn new(data: T, distance: u32) -> Self {
        Self { distance, data }
    }
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

impl From<(&str, &str)> for NodeId {
    fn from(value: (&str, &str)) -> Self {
        Self {
            namespace: value.0.to_string(),
            id: value.1.to_string(),
            relation: None,
        }
    }
}

impl From<(&str, &str, &str)> for NodeId {
    fn from(value: (&str, &str, &str)) -> Self {
        Self {
            namespace: value.0.to_string(),
            id: value.1.to_string(),
            relation: Some(value.2.to_string()),
        }
    }
}

impl From<(&str, &str, Option<&str>)> for NodeId {
    fn from(value: (&str, &str, Option<&str>)) -> Self {
        Self {
            namespace: value.0.to_string(),
            id: value.1.to_string(),
            relation: value.2.map(|x| x.to_string()),
        }
    }
}

impl From<(String, String)> for NodeId {
    fn from(value: (String, String)) -> Self {
        Self {
            namespace: value.0,
            id: value.1,
            relation: None,
        }
    }
}

impl From<(String, String, String)> for NodeId {
    fn from(value: (String, String, String)) -> Self {
        Self {
            namespace: value.0,
            id: value.1,
            relation: Some(value.2),
        }
    }
}

impl From<(String, String, Option<String>)> for NodeId {
    fn from(value: (String, String, Option<String>)) -> Self {
        Self {
            namespace: value.0,
            id: value.1,
            relation: value.2,
        }
    }
}
