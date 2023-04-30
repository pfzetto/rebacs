use std::{
    collections::{
        hash_map::{Iter, IterMut},
        BinaryHeap, HashMap, HashSet,
    },
    hash::Hash,
    ops::Deref,
    sync::Arc,
};

use log::info;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

#[derive(Default)]
pub struct Graph {
    nodes: BidMap<Object, ObjectRef>,
    edges: BidThreeMap<ObjectOrSet, Relation, ObjectRef>,
    counter: u32,
}

#[derive(Hash, PartialEq, Eq, Clone, Serialize, Deserialize, Debug)]
pub struct Object {
    pub namespace: String,
    pub id: String,
}

#[derive(Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ObjectRef(pub u32);

#[derive(PartialEq, Eq, Hash, Clone, Debug, Deserialize, Serialize)]
pub enum ObjectOrSet {
    Object(ObjectRef),
    Set((ObjectRef, Relation)),
}

#[derive(Hash, PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Relation(String);

#[derive(PartialEq, Eq, Clone, Hash, Serialize, Deserialize, Debug)]
pub struct ObjectRelation(pub ObjectRef, pub Relation);

impl Object {
    pub fn new(namespace: &str, id: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            id: id.to_string(),
        }
    }
}

impl ObjectOrSet {
    pub fn object_ref(&self) -> &ObjectRef {
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

impl From<ObjectRef> for ObjectOrSet {
    fn from(value: ObjectRef) -> Self {
        Self::Object(value)
    }
}

impl From<(ObjectRef, &str)> for ObjectOrSet {
    fn from(value: (ObjectRef, &str)) -> Self {
        Self::Set((value.0, Relation::new(value.1)))
    }
}

impl Relation {
    pub fn new(relation: &str) -> Self {
        Self(relation.to_string())
    }
}
impl Deref for Relation {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<(ObjectRef, Relation)> for ObjectRelation {
    fn from(value: (ObjectRef, Relation)) -> Self {
        Self(value.0, value.1)
    }
}
impl From<(ObjectRef, &str)> for ObjectRelation {
    fn from(value: (ObjectRef, &str)) -> Self {
        Self(value.0, Relation::new(value.1))
    }
}

impl Graph {
    pub fn get_node(&self, namespace: &str, id: &str) -> Option<ObjectRef> {
        self.nodes.get_by_a(&Object::new(namespace, id)).cloned()
    }
    pub fn object_from_ref(&self, obj: &ObjectRef) -> Object {
        self.nodes.get_by_b(obj).unwrap().clone()
    }
    pub fn add_node(&mut self, node: Object) -> ObjectRef {
        let obj_ref = ObjectRef(self.counter);
        self.nodes.insert(node, obj_ref);
        self.counter += 1;
        obj_ref
    }
    pub fn remove_node(&mut self, node: Object) {
        let index = self.nodes.remove_by_a(&node);
        if let Some(index) = index {
            self.edges.remove_by_c(&index);
            self.edges.get_by_a(&ObjectOrSet::Object(*index));
            //TODO: remove edges with ObjectOrSet::Set
        }
    }

    pub fn has_relation(&self, src: ObjectOrSet, dst: ObjectRelation) -> bool {
        self.edges.has(&src, &dst.1, &dst.0)
    }
    pub fn add_relation(&mut self, src: ObjectOrSet, dst: ObjectRelation) {
        self.edges.insert(src, dst.1, dst.0);
    }
    pub fn remove_relation(&mut self, src: ObjectOrSet, dst: ObjectRelation) {
        self.edges.remove(&src, &dst.1, &dst.0);
    }

    pub fn is_related_to(
        &self,
        src: impl Into<ObjectOrSet>,
        dst: impl Into<ObjectRelation>,
    ) -> bool {
        let src = src.into();
        let dst = dst.into();
        let mut dist: HashMap<ObjectRelation, u32> = HashMap::new();
        let mut q: BinaryHeap<ObjectRelationDist> = BinaryHeap::new();

        for neighbor in self
            .edges
            .get_by_a(&src)
            .iter()
            .flat_map(|(r, m)| m.iter().map(|x| ObjectRelation(**x, (**r).clone())))
        {
            if neighbor == dst {
                return true;
            }
            dist.insert(neighbor.clone(), 1);
            q.push(ObjectRelationDist(1, neighbor.clone()));
        }

        while let Some(ObjectRelationDist(node_dist, node)) = q.pop() {
            let node_dist = node_dist + 1;
            let node = ObjectOrSet::Set((node.0, node.1));
            for neighbor in self
                .edges
                .get_by_a(&node)
                .iter()
                .flat_map(|(r, m)| m.iter().map(|x| ObjectRelation(**x, (**r).clone())))
            {
                if neighbor == dst {
                    return true;
                }
                if let Some(existing_node_dist) = dist.get(&neighbor) {
                    if *existing_node_dist < node_dist {
                        continue;
                    }
                }
                dist.insert(neighbor.clone(), node_dist);
                q.push(ObjectRelationDist(node_dist, neighbor.clone()));
            }
        }

        false
    }
    pub fn related_to(&self, dst: ObjectRef, relation: Relation) -> HashSet<ObjectRef> {
        let mut relation_sets = vec![];
        let mut relations: HashSet<ObjectRef> = HashSet::new();
        for obj in self.edges.get_by_cb(&dst, &relation) {
            match obj {
                ObjectOrSet::Object(obj) => {
                    relations.insert(*obj);
                }
                ObjectOrSet::Set(set) => relation_sets.push(set),
            }
        }
        while let Some(set) = relation_sets.pop() {
            for obj in self.edges.get_by_cb(&set.0, &set.1) {
                match obj {
                    ObjectOrSet::Object(obj) => {
                        relations.insert(*obj);
                    }
                    ObjectOrSet::Set(set) => relation_sets.push(set),
                }
            }
        }
        relations
    }
    pub fn relations(&self, src: impl Into<ObjectRelation>) -> HashSet<ObjectRef> {
        let src: ObjectRelation = src.into();

        let mut visited = HashSet::new();
        let mut relation_sets = vec![];
        let mut relations = HashSet::new();

        for (rel, neighbors) in self.edges.get_by_a(&ObjectOrSet::Object(src.0)) {
            for neighbor in neighbors {
                if *rel == src.1 {
                    relations.insert(*neighbor);
                }
                relation_sets.push((rel, neighbor));
            }
        }

        while let Some((rel, obj_ref)) = relation_sets.pop() {
            if !visited.contains(&(rel, obj_ref)) {
                for (rel, neighbors) in self
                    .edges
                    .get_by_a(&ObjectOrSet::Set((*obj_ref, (*rel).clone())))
                {
                    for neighbor in neighbors {
                        if *rel == src.1 {
                            relations.insert(*neighbor);
                        }
                        relation_sets.push((rel, neighbor));
                    }
                }
                visited.insert((rel, obj_ref));
            }
        }

        relations
    }

    pub async fn to_file(&self, file: &mut File) {
        info!("writing graph to file");
        for (obj, obj_ref) in self.nodes.iter() {
            file.write_all(format!("[{}:{}]\n", &obj.namespace, &obj.id).as_bytes())
                .await
                .unwrap();
            for (rel, arr) in self.edges.get_by_c(obj_ref.as_ref()) {
                let arr = arr
                    .iter()
                    .filter_map(|x| {
                        let rel_obj_ref = x.object_ref();
                        self.nodes.get_by_b(rel_obj_ref).map(|rel_obj| {
                            let (namespace, id) = (&rel_obj.namespace, &rel_obj.id);

                            if *namespace == obj.namespace && *id == obj.id {
                                match x.relation() {
                                    None => "self".to_string(),
                                    Some(rel) => format!("self#{}", &rel.0),
                                }
                            } else {
                                match x.relation() {
                                    None => format!("{}:{}", &namespace, &id),
                                    Some(rel) => format!("{}:{}#{}", &namespace, &id, &rel.0),
                                }
                            }
                        })
                    })
                    .reduce(|acc, e| acc + ", " + &e)
                    .unwrap_or_default();
                file.write_all(format!("{} = [{}]\n", &rel.0, &arr).as_bytes())
                    .await
                    .unwrap();
            }
            file.write_all("\n".as_bytes()).await.unwrap();
        }
    }

    pub async fn from_file(file: &mut File) -> Self {
        info!("reading graph from file");
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut graph = Graph::default();

        let mut node: Option<(ObjectRef, String, String)> = None;
        let mut relations = vec![];
        while let Ok(Some(line)) = lines.next_line().await {
            if line.starts_with('[') && line.ends_with(']') {
                let line = &mut line[1..line.len() - 1].split(':');
                let namespace = line.next().unwrap();
                let id = line.next().unwrap();
                let obj_ref = graph.add_node(Object::new(namespace, id));
                node = Some((obj_ref, namespace.to_string(), id.to_string()));
            } else if line.contains('=') && line.contains('[') && line.contains(']') {
                if let Some(dst) = &node {
                    let equals_pos = line.find('=').unwrap();
                    let arr_start = line.find('[').unwrap();
                    let arr_stop = line.find(']').unwrap();

                    let rel = line[..equals_pos].trim();
                    let arr = line[arr_start + 1..arr_stop].split(", ");

                    for obj in arr {
                        let (src_namespace, src_id, src_rel) = if obj.contains('#') {
                            let sep_1 = obj.find(':');
                            let sep_2 = obj.find('#').unwrap();

                            let (namespace, id) = if let Some(sep_1) = sep_1 {
                                (&obj[..sep_1], &obj[sep_1 + 1..sep_2])
                            } else {
                                (dst.1.as_str(), dst.2.as_str())
                            };

                            let rel = &obj[sep_2 + 1..];

                            (namespace, id, Some(rel))
                        } else {
                            let sep_1 = obj.find(':');

                            let (namespace, id) = if let Some(sep_1) = sep_1 {
                                (&obj[..sep_1], &obj[sep_1 + 1..])
                            } else {
                                (dst.1.as_str(), dst.2.as_str())
                            };
                            (namespace, id, None)
                        };

                        relations.push((
                            src_namespace.to_string(),
                            src_id.to_string(),
                            src_rel.map(String::from),
                            dst.0,
                            rel.to_string(),
                        ));
                    }
                }
            }
        }

        for relation in relations {
            let src = match relation.2 {
                Some(rel) => {
                    let obj = graph.get_node(&relation.0, &relation.1).unwrap();
                    ObjectOrSet::Set((obj, Relation::new(&rel)))
                }
                None => {
                    let obj = graph.get_node(&relation.0, &relation.1).unwrap();
                    ObjectOrSet::Object(obj)
                }
            };
            graph.add_relation(src, ObjectRelation(relation.3, Relation(relation.4)));
        }

        graph
    }
}

/// Helper Struct used for Dijkstra
#[derive(PartialEq, Eq)]
struct ObjectRelationDist(u32, ObjectRelation);

impl Ord for ObjectRelationDist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}
impl PartialOrd for ObjectRelationDist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.0.cmp(&self.0))
    }
}

pub struct BidMap<A, B> {
    left_to_right: HashMap<Arc<A>, Arc<B>>,
    right_to_left: HashMap<Arc<B>, Arc<A>>,
}

impl<A, B> Default for BidMap<A, B> {
    fn default() -> Self {
        Self {
            left_to_right: Default::default(),
            right_to_left: Default::default(),
        }
    }
}

impl<A, B> BidMap<A, B>
where
    A: Eq + Hash,
    B: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            left_to_right: HashMap::new(),
            right_to_left: HashMap::new(),
        }
    }

    pub fn insert(&mut self, a: A, b: B) {
        let a = Arc::new(a);
        let b = Arc::new(b);
        self.left_to_right.insert(a.clone(), b.clone());
        self.right_to_left.insert(b, a);
    }

    pub fn remove_by_a(&mut self, a: &A) -> Option<Arc<B>> {
        if let Some(b) = self.left_to_right.remove(a) {
            self.right_to_left.remove(&b);
            Some(b)
        } else {
            None
        }
    }

    pub fn remove_by_b(&mut self, b: &B) -> Option<Arc<A>> {
        if let Some(a) = self.right_to_left.remove(b) {
            self.left_to_right.remove(&a);
            Some(a)
        } else {
            None
        }
    }

    pub fn get_by_a(&self, a: &A) -> Option<&B> {
        self.left_to_right.get(a).map(Deref::deref)
    }

    pub fn get_by_b(&self, b: &B) -> Option<&A> {
        self.right_to_left.get(b).map(Deref::deref)
    }

    pub fn iter(&self) -> Iter<Arc<A>, Arc<B>> {
        self.left_to_right.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<Arc<A>, Arc<B>> {
        self.left_to_right.iter_mut()
    }
}

pub struct BidThreeMap<A, B, C> {
    left_to_right: HashMap<Arc<A>, HashMap<Arc<B>, HashSet<Arc<C>>>>,
    right_to_left: HashMap<Arc<C>, HashMap<Arc<B>, HashSet<Arc<A>>>>,
}

impl<A, B, C> BidThreeMap<A, B, C>
where
    A: Eq + Hash,
    B: Eq + Hash,
    C: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            left_to_right: HashMap::new(),
            right_to_left: HashMap::new(),
        }
    }

    pub fn insert(&mut self, a: A, b: B, c: C) {
        let a = Arc::new(a);
        let b = Arc::new(b);
        let c = Arc::new(c);

        if let Some(middle) = self.left_to_right.get_mut(&a) {
            if let Some(right) = middle.get_mut(&b) {
                right.insert(c.clone());
            } else {
                let mut right = HashSet::new();
                right.insert(c.clone());
                middle.insert(b.clone(), right);
            }
        } else {
            let mut middle = HashMap::new();
            let mut right = HashSet::new();
            right.insert(c.clone());
            middle.insert(b.clone(), right);
            self.left_to_right.insert(a.clone(), middle);
        }

        if let Some(middle) = self.right_to_left.get_mut(&c) {
            if let Some(left) = middle.get_mut(&b) {
                left.insert(a);
            } else {
                let mut left = HashSet::new();
                left.insert(a);
                middle.insert(b, left);
            }
        } else {
            let mut middle = HashMap::new();
            let mut left = HashSet::new();
            left.insert(a);
            middle.insert(b, left);
            self.right_to_left.insert(c, middle);
        }
    }

    pub fn remove(&mut self, a: &A, b: &B, c: &C) {
        if let Some(right) = self.left_to_right.get_mut(a).and_then(|ltr| ltr.get_mut(b)) {
            right.remove(c);
        }
        if let Some(left) = self.right_to_left.get_mut(c).and_then(|rtl| rtl.get_mut(b)) {
            left.remove(a);
        }
    }

    pub fn remove_by_a(&mut self, a: &A) {
        if let Some(map) = self.left_to_right.remove(a) {
            for (b, set) in map {
                for c in set {
                    if let Some(set) = self
                        .right_to_left
                        .get_mut(&c)
                        .and_then(|ltr| ltr.get_mut(&b))
                    {
                        set.remove(a);
                    }
                }
            }
        }
    }

    pub fn remove_by_c(&mut self, c: &C) {
        if let Some(map) = self.right_to_left.remove(c) {
            for (b, set) in map {
                for a in set {
                    if let Some(set) = self
                        .left_to_right
                        .get_mut(&a)
                        .and_then(|ltr| ltr.get_mut(&b))
                    {
                        set.remove(c);
                    }
                }
            }
        }
    }

    pub fn has(&self, a: &A, b: &B, c: &C) -> bool {
        self.left_to_right
            .get(a)
            .and_then(|ltr| ltr.get(b))
            .and_then(|ltr| ltr.get(c))
            .is_some()
    }

    pub fn get_by_ab(&self, a: &A, b: &B) -> HashSet<&C> {
        self.left_to_right
            .get(a)
            .and_then(|ltr| ltr.get(b))
            .map(|ltr| ltr.iter().map(|x| x.as_ref()).collect::<HashSet<_>>())
            .unwrap_or_default()
    }

    pub fn get_by_cb(&self, c: &C, b: &B) -> HashSet<&A> {
        self.right_to_left
            .get(c)
            .and_then(|rtl| rtl.get(b))
            .map(|rtl| rtl.iter().map(|x| x.as_ref()).collect::<HashSet<_>>())
            .unwrap_or_default()
    }

    pub fn get_by_a(&self, a: &A) -> HashMap<&B, HashSet<&C>> {
        self.left_to_right
            .get(a)
            .iter()
            .flat_map(|x| x.iter())
            .map(|(b, c)| {
                (
                    b.as_ref(),
                    c.iter().map(|x| x.as_ref()).collect::<HashSet<&C>>(),
                )
            })
            .collect::<_>()
    }

    pub fn get_by_c(&self, c: &C) -> HashMap<&B, HashSet<&A>> {
        self.right_to_left
            .get(c)
            .iter()
            .flat_map(|x| x.iter())
            .map(|(b, a)| {
                (
                    b.as_ref(),
                    a.iter().map(|x| x.as_ref()).collect::<HashSet<&A>>(),
                )
            })
            .collect::<_>()
    }
}

impl<A, B, C> Default for BidThreeMap<A, B, C> {
    fn default() -> Self {
        Self {
            left_to_right: Default::default(),
            right_to_left: Default::default(),
        }
    }
}
