//hello world

use crate::{Distanced, NodeId, RelationGraph};

#[test]
fn distanced_ordering() {
    let a = Distanced::new((), 0);
    let b = Distanced::one(());
    let c = Distanced::new((), 1);
    let d = Distanced::new((), 2);

    assert!(a < b);
    assert!(b == c);
    assert!(c < d);
    assert!(a < d);
}

#[tokio::test]
async fn simple_graph() {
    let graph = RelationGraph::default();

    let alice = ("user", "alice");
    let bob = ("user", "bob");
    let charlie = ("user", "charlie");

    let foo_read = ("application", "foo", "read");
    let bar_read = ("application", "bar", "read");

    graph.insert(alice, foo_read).await;
    graph.insert(bob, bar_read).await;

    assert!(graph.has_recursive(alice, foo_read, None).await);
    assert!(!graph.has_recursive(alice, bar_read, None).await);

    assert!(!graph.has_recursive(bob, foo_read, None).await);
    assert!(graph.has_recursive(bob, bar_read, None).await);

    assert!(!graph.has_recursive(charlie, foo_read, None).await);
    assert!(!graph.has_recursive(charlie, bar_read, None).await);

    graph.remove(alice, foo_read).await;
    graph.remove(alice, bar_read).await;

    assert!(!graph.has_recursive(alice, foo_read, None).await);
    assert!(!graph.has_recursive(alice, bar_read, None).await);

    graph.insert(charlie, foo_read).await;
    graph.insert(charlie, bar_read).await;

    assert!(graph.has_recursive(charlie, foo_read, None).await);
    assert!(graph.has_recursive(charlie, bar_read, None).await);
}
