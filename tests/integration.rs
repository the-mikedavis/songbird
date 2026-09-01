use bytes::Bytes;
use songbird::{Connection, OffsetSpec, PublishOutcome, SubscribeOptions};

#[tokio::test]
async fn publish_then_consume() {
    let connection = Connection::connect("localhost:5552", Default::default())
        .await
        .unwrap();
    let stream = "songbird-integration-test";
    connection
        .create_stream(stream, &Default::default())
        .await
        .unwrap();

    let (publisher, mut confirms) = connection.declare_publisher(stream, None).await.unwrap();
    let ids = publisher
        .send_batch(vec![
            Bytes::from_static(b"one"),
            Bytes::from_static(b"two"),
            Bytes::from_static(b"three"),
        ])
        .await
        .unwrap();

    let mut confirmed = Vec::new();
    while confirmed.len() < ids.len() {
        match confirms.recv().await.expect("confirm channel closed") {
            PublishOutcome::Confirmed(batch) => confirmed.extend(batch),
            PublishOutcome::Failed(failures) => panic!("publish failed: {failures:?}"),
        }
    }
    confirmed.sort_unstable();
    assert_eq!(confirmed, ids);

    let mut subscription = connection
        .subscribe(
            stream,
            SubscribeOptions {
                offset: OffsetSpec::First,
                credit: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut bodies = Vec::new();
    while bodies.len() < 3 {
        let chunk = subscription.next().await.unwrap().unwrap();
        for message in chunk.messages() {
            bodies.push(message.unwrap().body);
        }
    }
    assert_eq!(bodies, vec!["one", "two", "three"]);

    subscription.close().await.unwrap();
    publisher.close().await.unwrap();
    connection.delete_stream(stream).await.unwrap();
    connection.close().await.unwrap();
}
