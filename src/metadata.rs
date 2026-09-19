//! Metadata v12 — discovers which partitions a topic has and which broker
//! leads each one, so ListOffsets can be aimed at the right node.

use anyhow::Result;

use crate::protocol::{Request, API_KEY_METADATA};
use crate::wire::{Decoder, Encoder};

#[derive(Debug, Clone, Default)]
pub struct MetadataRequest {
    /// `None` asks for every topic in the cluster; `Some(list)` asks for
    /// exactly those.
    pub topics: Option<Vec<String>>,
}

impl MetadataRequest {
    pub fn for_topics<I, S>(topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            topics: Some(topics.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: Option<String>,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerInfo>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponse {
    pub fn broker(&self, node_id: i32) -> Option<&BrokerInfo> {
        self.brokers.iter().find(|b| b.node_id == node_id)
    }

    pub fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.iter().find(|t| t.name.as_deref() == Some(name))
    }
}

impl Request for MetadataRequest {
    type Response = MetadataResponse;

    const API_KEY: i16 = API_KEY_METADATA;
    const API_VERSION: i16 = 12;
    const FLEXIBLE: bool = true;
    const RESPONSE_HEADER_VERSION: i16 = 1;

    fn api_name() -> &'static str {
        "Metadata v12"
    }

    fn encode_body(&self, e: &mut Encoder) {
        match &self.topics {
            Some(topics) => {
                e.put_compact_array_len(topics.len());
                for name in topics {
                    // v10+ carries a topic id alongside the name; the all-zero
                    // UUID means "I do not know the id, resolve by name".
                    e.put_zero_uuid();
                    e.put_compact_nullable_string(Some(name));
                    e.put_empty_tagged_fields();
                }
            }
            None => e.put_null_compact_array(),
        }
        e.put_bool(false); // allow_auto_topic_creation
        e.put_bool(false); // include_topic_authorized_operations
        e.put_empty_tagged_fields();
    }

    fn decode_response(d: &mut Decoder) -> Result<MetadataResponse> {
        let throttle_time_ms = d.i32()?;
        let brokers = d.compact_array(|d| {
            let node_id = d.i32()?;
            let host = d.compact_string()?;
            let port = d.i32()?;
            let rack = d.compact_nullable_string()?;
            d.skip_tagged_fields()?;
            Ok(BrokerInfo {
                node_id,
                host,
                port,
                rack,
            })
        })?;
        let cluster_id = d.compact_nullable_string()?;
        let controller_id = d.i32()?;
        let topics = d.compact_array(|d| {
            let error_code = d.i16()?;
            let name = d.compact_nullable_string()?;
            let _topic_id = d.uuid()?;
            let is_internal = d.bool()?;
            let partitions = d.compact_array(|d| {
                let error_code = d.i16()?;
                let partition_index = d.i32()?;
                let leader_id = d.i32()?;
                let leader_epoch = d.i32()?;
                skip_int32_array(d)?; // replica nodes
                skip_int32_array(d)?; // in-sync replicas
                skip_int32_array(d)?; // offline replicas
                d.skip_tagged_fields()?;
                Ok(PartitionMetadata {
                    error_code,
                    partition_index,
                    leader_id,
                    leader_epoch,
                })
            })?;
            let _topic_authorized_operations = d.i32()?;
            d.skip_tagged_fields()?;
            Ok(TopicMetadata {
                error_code,
                name,
                is_internal,
                partitions,
            })
        })?;
        d.skip_tagged_fields()?;
        Ok(MetadataResponse {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        })
    }
}

fn skip_int32_array(d: &mut Decoder) -> Result<()> {
    let n = d.compact_array_len()?.unwrap_or(0);
    d.raw(n * 4)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_v12_body_for_one_topic() {
        let mut e = Encoder::new();
        MetadataRequest::for_topics(["demo"]).encode_body(&mut e);
        let mut expected = vec![0x02]; // compact array, 1 entry
        expected.extend_from_slice(&[0u8; 16]); // zero topic id
        expected.extend_from_slice(&[0x05, b'd', b'e', b'm', b'o']);
        expected.push(0x00); // per-topic tagged fields
        expected.push(0x00); // allow_auto_topic_creation = false
        expected.push(0x00); // include_topic_authorized_operations = false
        expected.push(0x00); // request tagged fields
        assert_eq!(e.as_slice(), expected.as_slice());
    }

    #[test]
    fn encodes_null_topics_as_all_topics() {
        let mut e = Encoder::new();
        MetadataRequest::default().encode_body(&mut e);
        // A null compact array (0x00), not an empty one (0x01) — an empty
        // array would ask for *zero* topics instead of all of them.
        assert_eq!(e.as_slice(), &[0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn decodes_v12_body() {
        let body: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle 0
            0x02, // brokers: 1
            0x00, 0x00, 0x00, 0x01, // node id 1
            0x0a, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // "localhost"
            0x00, 0x00, 0xe6, 0xac, // port 59052
            0x00, // null rack
            0x00, // broker tagged fields
            0x06, b'a', b'b', b'c', b'd', b'e', // cluster id "abcde"
            0x00, 0x00, 0x00, 0x01, // controller id 1
            0x02, // topics: 1
            0x00, 0x00, // topic error code 0
            0x05, b'd', b'e', b'm', b'o', // "demo"
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // topic id (16 bytes)
            0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, //
            0x00, // is_internal false
            0x03, // partitions: 2
            0x00, 0x00, // p0 error
            0x00, 0x00, 0x00, 0x00, // p0 index
            0x00, 0x00, 0x00, 0x01, // p0 leader 1
            0x00, 0x00, 0x00, 0x07, // p0 leader epoch 7
            0x02, 0x00, 0x00, 0x00, 0x01, // replicas [1]
            0x02, 0x00, 0x00, 0x00, 0x01, // isr [1]
            0x01, // offline replicas []
            0x00, // p0 tagged fields
            0x00, 0x00, // p1 error
            0x00, 0x00, 0x00, 0x01, // p1 index
            0x00, 0x00, 0x00, 0x01, // p1 leader 1
            0x00, 0x00, 0x00, 0x07, // p1 leader epoch
            0x02, 0x00, 0x00, 0x00, 0x01, // replicas [1]
            0x02, 0x00, 0x00, 0x00, 0x01, // isr [1]
            0x01, // offline []
            0x00, // p1 tagged fields
            0xff, 0xff, 0xff, 0xff, // topic authorized operations
            0x00, // topic tagged fields
            0x00, // response tagged fields
        ];
        let mut d = Decoder::new(body);
        let resp = MetadataRequest::decode_response(&mut d).unwrap();
        assert_eq!(d.remaining(), 0, "decoder must consume the whole body");
        assert_eq!(
            resp.brokers,
            vec![BrokerInfo {
                node_id: 1,
                host: "localhost".to_string(),
                port: 59052,
                rack: None,
            }]
        );
        assert_eq!(resp.cluster_id.as_deref(), Some("abcde"));
        assert_eq!(resp.controller_id, 1);
        let topic = resp.topic("demo").expect("topic demo");
        assert!(!topic.is_internal);
        assert_eq!(topic.partitions.len(), 2);
        assert_eq!(topic.partitions[1].partition_index, 1);
        assert_eq!(topic.partitions[1].leader_id, 1);
        assert_eq!(topic.partitions[0].leader_epoch, 7);
        assert_eq!(resp.broker(1).unwrap().host, "localhost");
        assert!(resp.broker(2).is_none());
    }
}
