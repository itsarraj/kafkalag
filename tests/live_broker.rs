//! Live end-to-end test against a real Kafka broker.
//!
//! `#[ignore]`d on purpose: it needs a broker listening on `localhost:59092`
//! (override with `$KAFKALAG_TEST_BROKER`), so plain `cargo test` — and
//! therefore CI — skips it. Bring one up and run it with:
//!
//! ```bash
//! docker run -d --name kafkalag-test-broker -p 59092:9092 \
//!   -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
//!   -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9094 \
//!   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:59092,INTERNAL://localhost:9094 \
//!   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT \
//!   -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
//!   -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//!   -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
//!   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
//!   -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
//!   -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
//!   apache/kafka:latest
//!
//! cargo test -- --ignored
//! ```
//!
//! Seed the topic/group first (see README) — the test asserts against real
//! offsets, not against a fixture it created itself.

use std::time::Duration;

use kafkalag::apiversions::ApiVersionsRequest;
use kafkalag::client::{parse_broker_addr, Connection};
use kafkalag::lag::describe_group;
use kafkalag::protocol::{
    Request, API_KEY_FIND_COORDINATOR, API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_OFFSET_FETCH,
};

fn broker() -> String {
    std::env::var("KAFKALAG_TEST_BROKER").unwrap_or_else(|_| "localhost:59092".to_string())
}

fn group() -> String {
    std::env::var("KAFKALAG_TEST_GROUP").unwrap_or_else(|_| "lagdemo-group".to_string())
}

fn topic() -> String {
    std::env::var("KAFKALAG_TEST_TOPIC").unwrap_or_else(|_| "lagdemo".to_string())
}

#[test]
#[ignore = "needs a live Kafka broker on localhost:59092"]
fn broker_advertises_every_api_version_we_pinned() {
    let (host, port) = parse_broker_addr(&broker()).unwrap();
    let mut conn =
        Connection::connect(&host, port, Duration::from_secs(10), "kafkalag-test").unwrap();
    let resp = conn.send(&ApiVersionsRequest::default()).unwrap();
    assert_eq!(resp.error_code, 0);
    assert!(!resp.api_keys.is_empty());
    for (key, version) in [
        (API_KEY_METADATA, 12),
        (API_KEY_LIST_OFFSETS, 7),
        (API_KEY_FIND_COORDINATOR, 4),
        (API_KEY_OFFSET_FETCH, 8),
        (ApiVersionsRequest::API_KEY, ApiVersionsRequest::API_VERSION),
    ] {
        assert!(
            resp.supports(key, version),
            "broker does not support api key {key} v{version}: {:?}",
            resp.range_for(key)
        );
    }
}

#[test]
#[ignore = "needs a live Kafka broker with a seeded topic and partially-consumed group"]
fn reports_real_lag_for_a_partially_consumed_group() {
    let report = describe_group(&broker(), &group(), Some(&topic()), Duration::from_secs(10))
        .expect("describe_group against the live broker");

    assert_eq!(report.group, group());
    assert!(
        !report.partitions.is_empty(),
        "expected committed offsets for {}/{}",
        group(),
        topic()
    );
    for p in &report.partitions {
        assert_eq!(p.topic, topic());
        assert!(p.high_watermark >= 0);
        if let (Some(committed), Some(lag)) = (p.committed_offset, p.lag) {
            assert_eq!(lag, p.high_watermark - committed);
        }
    }
    assert!(
        report.total_lag() > 0,
        "the seeded group should still be behind; got {report:?}"
    );
}
