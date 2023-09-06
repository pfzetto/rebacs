use crate::{RObject, RSet, RelationGraph, WILDCARD_ID};

#[tokio::test]
async fn simple_graph() {
    let graph = RelationGraph::default();

    let alice: RObject = ("user", "alice").into();
    let bob: RObject = ("user", "bob").into();
    let charlie: RObject = ("user", "charlie").into();

    let foo_read: RSet = ("application", "foo", "read").into();
    let bar_read: RSet = ("application", "bar", "read").into();

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

    let alice: RObject = ("user", "alice").into();
    let bob: RObject = ("user", "bob").into();
    let charlie: RObject = ("user", "charlie").into();

    let user_wildcard: RObject = ("user", WILDCARD_ID).into();

    let foo_read: RSet = ("application", "foo", "read").into();
    let bar_read: RSet = ("application", "bar", "read").into();

    let app_read: RSet = ("application", WILDCARD_ID, "read").into();

    let some_app_read: RSet = ("application", "bla", "read").into();

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
