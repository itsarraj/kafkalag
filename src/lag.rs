//! Ties the four APIs together into a lag report:
//!
//! 1. `ApiVersions` — confirm the broker speaks the versions we pinned.
//! 2. `FindCoordinator` — locate the group's coordinator.
//! 3. `OffsetFetch` (to the coordinator) — the group's committed offsets.
//! 4. `Metadata` — which broker leads each partition we care about.
//! 5. `ListOffsets` (to each leader) — the high-water mark per partition.
//!
//! Lag is then `high_watermark - committed_offset`, which is the same
//! arithmetic `kafka-consumer-groups.sh --describe` prints.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::apiversions::ApiVersionsRequest;
use crate::client::{parse_broker_addr, ConnectionPool};
use crate::coordinator::FindCoordinatorRequest;
use crate::listoffsets::ListOffsetsRequest;
use crate::metadata::MetadataRequest;
use crate::offsetfetch::OffsetFetchRequest;
use crate::protocol::{check_error, Request};

/// Lag for one topic-partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    /// Latest offset on the leader — what the next produced record will get.
    pub high_watermark: i64,
    /// `None` when the group has never committed for this partition.
    pub committed_offset: Option<i64>,
    /// `None` exactly when `committed_offset` is `None`.
    pub lag: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LagReport {
    pub group: String,
    pub coordinator: String,
    pub partitions: Vec<PartitionLag>,
}

impl LagReport {
    /// Sum of the known lags; partitions with no commit contribute nothing.
    pub fn total_lag(&self) -> i64 {
        self.partitions.iter().filter_map(|p| p.lag).sum()
    }
}

/// `high_watermark - committed`, with Kafka's `-1` "never committed" sentinel
/// mapped to `None`.
///
/// The `max(0)` clamp matters in practice: the two numbers come from two
/// different requests to two different brokers, so a commit landing between
/// them can briefly make the committed offset the larger of the two. Kafka's
/// own tooling clamps the same way rather than printing a negative lag.
pub fn compute_lag(high_watermark: i64, committed: Option<i64>) -> Option<i64> {
    committed.map(|c| (high_watermark - c).max(0))
}

/// Runs the full lag query against a cluster reached via `bootstrap`.
///
/// `topic` restricts the report to one topic (including partitions the group
/// has never committed to); `None` reports every topic the group has a commit
/// for, matching `kafka-consumer-groups.sh --describe`.
pub fn describe_group(
    bootstrap: &str,
    group: &str,
    topic: Option<&str>,
    timeout: Duration,
) -> Result<LagReport> {
    let (host, port) = parse_broker_addr(bootstrap)?;
    let mut pool = ConnectionPool::new(timeout, "kafkalag");

    // 1. Handshake. Cheap, and it turns "broker too old" into a clear message
    //    instead of a mystery parse failure three requests later.
    let versions = {
        let conn = pool.get(&host, port)?;
        conn.send(&ApiVersionsRequest::default())?
    };
    check_error(versions.error_code, "ApiVersions")?;
    for (api_key, api_version, name) in [
        (
            MetadataRequest::API_KEY,
            MetadataRequest::API_VERSION,
            MetadataRequest::api_name(),
        ),
        (
            ListOffsetsRequest::API_KEY,
            ListOffsetsRequest::API_VERSION,
            ListOffsetsRequest::api_name(),
        ),
        (
            FindCoordinatorRequest::API_KEY,
            FindCoordinatorRequest::API_VERSION,
            FindCoordinatorRequest::api_name(),
        ),
        (
            OffsetFetchRequest::API_KEY,
            OffsetFetchRequest::API_VERSION,
            OffsetFetchRequest::api_name(),
        ),
    ] {
        if !versions.supports(api_key, api_version) {
            let range = versions
                .range_for(api_key)
                .map(|r| format!("{}..={}", r.min_version, r.max_version))
                .unwrap_or_else(|| "not advertised at all".to_string());
            bail!(
                "broker at {bootstrap} does not support {name}: it advertises api key \
                 {api_key} as {range}. kafkalag pins one version per API and does not \
                 negotiate downwards."
            );
        }
    }

    // 2. Where do this group's commits live?
    let coordinator = {
        let conn = pool.get(&host, port)?;
        conn.send(&FindCoordinatorRequest::for_group(group))?
    };
    let coordinator = coordinator
        .for_key(group)
        .cloned()
        .with_context(|| format!("broker did not answer FindCoordinator for group {group:?}"))?;
    check_error(
        coordinator.error_code,
        &format!("FindCoordinator for group {group:?}"),
    )?;
    let coordinator_port = u16::try_from(coordinator.port)
        .with_context(|| format!("coordinator advertised a bad port {}", coordinator.port))?;
    let coordinator_addr = format!("{}:{}", coordinator.host, coordinator_port);

    // 3. If a topic was named, we need its partition list up front so the
    //    report can include partitions with no commit at all.
    let metadata_topics = topic.map(|t| vec![t.to_string()]);
    let mut metadata = match &metadata_topics {
        Some(topics) => {
            let conn = pool.get(&host, port)?;
            Some(conn.send(&MetadataRequest::for_topics(topics.clone()))?)
        }
        None => None,
    };

    // 4. Committed offsets, from the coordinator specifically.
    let fetch_request = match (topic, &metadata) {
        (Some(t), Some(md)) => {
            let topic_md = md
                .topic(t)
                .with_context(|| format!("broker returned no metadata for topic {t:?}"))?;
            check_error(topic_md.error_code, &format!("Metadata for topic {t:?}"))?;
            let partitions: Vec<i32> = topic_md
                .partitions
                .iter()
                .map(|p| p.partition_index)
                .collect();
            OffsetFetchRequest::for_topics(group, vec![(t.to_string(), partitions)])
        }
        _ => OffsetFetchRequest::all_topics(group),
    };
    let fetched = {
        let conn = pool.get(&coordinator.host, coordinator_port)?;
        conn.send(&fetch_request)?
    };
    let fetched_group = fetched
        .group(group)
        .with_context(|| format!("OffsetFetch response had no entry for group {group:?}"))?;
    check_error(
        fetched_group.error_code,
        &format!("OffsetFetch for group {group:?}"),
    )?;

    let mut committed: BTreeMap<(String, i32), Option<i64>> = BTreeMap::new();
    for t in &fetched_group.topics {
        for p in &t.partitions {
            check_error(
                p.error_code,
                &format!("OffsetFetch for {}-{}", t.name, p.partition_index),
            )?;
            committed.insert((t.name.clone(), p.partition_index), p.committed());
        }
    }
    if committed.is_empty() {
        return Ok(LagReport {
            group: group.to_string(),
            coordinator: coordinator_addr,
            partitions: Vec::new(),
        });
    }

    // 5. Metadata for whatever topics turned up (when we did not already ask).
    if metadata.is_none() {
        let topics: Vec<String> = committed
            .keys()
            .map(|(t, _)| t.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let conn = pool.get(&host, port)?;
        metadata = Some(conn.send(&MetadataRequest::for_topics(topics))?);
    }
    let metadata = metadata.expect("filled in directly above");

    // 6. High-water marks, asked of each partition's own leader. A follower
    //    would answer NOT_LEADER_OR_FOLLOWER, so this grouping is required
    //    rather than an optimisation.
    let mut by_leader: BTreeMap<i32, BTreeMap<String, Vec<i32>>> = BTreeMap::new();
    for (topic_name, partition) in committed.keys() {
        let topic_md = metadata
            .topic(topic_name)
            .with_context(|| format!("no metadata returned for topic {topic_name:?}"))?;
        check_error(
            topic_md.error_code,
            &format!("Metadata for topic {topic_name:?}"),
        )?;
        let part_md = topic_md
            .partitions
            .iter()
            .find(|p| p.partition_index == *partition)
            .with_context(|| {
                format!("topic {topic_name:?} has no partition {partition} in metadata")
            })?;
        check_error(
            part_md.error_code,
            &format!("Metadata for {topic_name}-{partition}"),
        )?;
        if part_md.leader_id < 0 {
            bail!(
                "{topic_name}-{partition} currently has no leader; cannot read its high-water mark"
            );
        }
        by_leader
            .entry(part_md.leader_id)
            .or_default()
            .entry(topic_name.clone())
            .or_default()
            .push(*partition);
    }

    let mut high_watermarks: BTreeMap<(String, i32), i64> = BTreeMap::new();
    for (leader_id, topics) in by_leader {
        let broker = metadata
            .broker(leader_id)
            .with_context(|| format!("metadata listed leader {leader_id} but not its address"))?
            .clone();
        let leader_port = u16::try_from(broker.port)
            .with_context(|| format!("broker {leader_id} advertised a bad port {}", broker.port))?;
        let conn = pool.get(&broker.host, leader_port)?;
        let listed = conn.send(&ListOffsetsRequest::latest(topics))?;
        for t in listed.topics {
            for p in t.partitions {
                check_error(
                    p.error_code,
                    &format!("ListOffsets for {}-{}", t.name, p.partition_index),
                )?;
                high_watermarks.insert((t.name.clone(), p.partition_index), p.offset);
            }
        }
    }

    // 7. Join the two sides.
    let mut partitions = Vec::with_capacity(committed.len());
    for ((topic_name, partition), committed_offset) in committed {
        let high_watermark = *high_watermarks
            .get(&(topic_name.clone(), partition))
            .with_context(|| format!("no high-water mark returned for {topic_name}-{partition}"))?;
        partitions.push(PartitionLag {
            topic: topic_name,
            partition,
            high_watermark,
            committed_offset,
            lag: compute_lag(high_watermark, committed_offset),
        });
    }
    partitions.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));

    Ok(LagReport {
        group: group.to_string(),
        coordinator: coordinator_addr,
        partitions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_is_high_watermark_minus_committed() {
        assert_eq!(compute_lag(100, Some(40)), Some(60));
        assert_eq!(compute_lag(100, Some(100)), Some(0));
        assert_eq!(compute_lag(0, Some(0)), Some(0));
    }

    #[test]
    fn never_committed_has_no_lag_rather_than_a_huge_one() {
        // Naively subtracting the -1 sentinel would report a lag of 101.
        assert_eq!(compute_lag(100, None), None);
    }

    #[test]
    fn a_commit_racing_the_high_watermark_clamps_to_zero() {
        assert_eq!(compute_lag(100, Some(101)), Some(0));
    }

    #[test]
    fn total_lag_skips_uncommitted_partitions() {
        let report = LagReport {
            group: "g1".to_string(),
            coordinator: "localhost:9092".to_string(),
            partitions: vec![
                PartitionLag {
                    topic: "demo".to_string(),
                    partition: 0,
                    high_watermark: 100,
                    committed_offset: Some(40),
                    lag: Some(60),
                },
                PartitionLag {
                    topic: "demo".to_string(),
                    partition: 1,
                    high_watermark: 50,
                    committed_offset: None,
                    lag: None,
                },
                PartitionLag {
                    topic: "demo".to_string(),
                    partition: 2,
                    high_watermark: 10,
                    committed_offset: Some(3),
                    lag: Some(7),
                },
            ],
        };
        assert_eq!(report.total_lag(), 67);
    }
}
