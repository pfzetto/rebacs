use crate::{Object, RelationGraph, Set, WILDCARD_ID};

#[tokio::test]
async fn simple_graph() {
    let graph = RelationGraph::default();

    let alice: Object = ("user", "alice").into();
    let bob: Object = ("user", "bob").into();
    let charlie: Object = ("user", "charlie").into();

    let foo_read: Set = ("application", "foo", "read").into();
    let bar_read: Set = ("application", "bar", "read").into();

    graph.insert(&alice, &foo_read).await;
    graph.insert(&bob, &bar_read).await;

    assert!(graph.check(&alice, &foo_read, None).await);
    assert!(!graph.check(&alice, &bar_read, None).await);

    assert!(!graph.check(&bob, &foo_read, None).await);
    assert!(graph.check(&bob, &bar_read, None).await);

    assert!(!graph.check(&charlie, &foo_read, None).await);
    assert!(!graph.check(&charlie, &bar_read, None).await);

    graph.remove(&alice, &foo_read).await;
    graph.remove(&alice, &bar_read).await;

    assert!(!graph.check(&alice, &foo_read, None).await);
    assert!(!graph.check(&alice, &bar_read, None).await);

    graph.insert(&charlie, &foo_read).await;
    graph.insert(&charlie, &bar_read).await;

    assert!(graph.check(&charlie, &foo_read, None).await);
    assert!(graph.check(&charlie, &bar_read, None).await);
}

#[tokio::test]
async fn wildcard() {
    let graph = RelationGraph::default();

    let alice: Object = ("user", "alice").into();
    let bob: Object = ("user", "bob").into();
    let charlie: Object = ("user", "charlie").into();

    let user_wildcard: Object = ("user", WILDCARD_ID).into();

    let foo_read: Set = ("application", "foo", "read").into();
    let bar_read: Set = ("application", "bar", "read").into();

    let app_read: Set = ("application", WILDCARD_ID, "read").into();

    let some_app_read: Set = ("application", "bla", "read").into();

    graph.insert(&alice, &foo_read).await;
    graph.insert(&user_wildcard, &foo_read).await;
    graph.insert(&bob, &bar_read).await;

    assert!(graph.check(&alice, &foo_read, None).await);
    assert!(graph.check(&bob, &foo_read, None).await);
    assert!(graph.check(&charlie, &foo_read, None).await);
    assert!(graph.check(&bob, &bar_read, None).await);

    graph.insert(&alice, &app_read).await;

    assert!(graph.check(&alice, &some_app_read, None).await);
    assert!(graph.check(&alice, &bar_read, None).await);
    assert!(!graph.check(&bob, &some_app_read, None).await);
    assert!(!graph.check(&charlie, &some_app_read, None).await);
}
