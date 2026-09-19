//! OffsetFetch v8 — reads a consumer group's last committed offset per
//! partition from its coordinator. This is the "CURRENT-OFFSET" column in
//! `kafka-consumer-groups.sh --describe`.

use anyhow::Result;

use crate::protocol::{Request, API_KEY_OFFSET_FETCH};
use crate::wire::{Decoder, Encoder};

/// A committed offset of `-1` is Kafka's sentinel for "this group has never
/// committed anything for this partition".
pub const NO_COMMITTED_OFFSET: i64 = -1;

#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    /// `None` fetches every topic-partition the group has ever committed to;
    /// `Some(list)` restricts it to the named topics and partitions.
    pub topics: Option<Vec<(String, Vec<i32>)>>,
    pub require_stable: bool,
}

impl OffsetFetchRequest {
    pub fn all_topics(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: None,
            require_stable: false,
        }
    }

    pub fn for_topics(group_id: impl Into<String>, topics: Vec<(String, Vec<i32>)>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: Some(topics),
            require_stable: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub metadata: Option<String>,
    pub error_code: i16,
}

impl CommittedPartition {
    /// `None` when the group has no commit for this partition.
    pub fn committed(&self) -> Option<i64> {
        if self.committed_offset < 0 {
            None
        } else {
            Some(self.committed_offset)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTopic {
    pub name: String,
    pub partitions: Vec<CommittedPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroup {
    pub group_id: String,
    pub topics: Vec<CommittedTopic>,
    pub error_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponse {
    pub throttle_time_ms: i32,
    pub groups: Vec<OffsetFetchGroup>,
}

impl OffsetFetchResponse {
    pub fn group(&self, group_id: &str) -> Option<&OffsetFetchGroup> {
        self.groups.iter().find(|g| g.group_id == group_id)
    }
}

impl Request for OffsetFetchRequest {
    type Response = OffsetFetchResponse;

    const API_KEY: i16 = API_KEY_OFFSET_FETCH;
    const API_VERSION: i16 = 8;
    const FLEXIBLE: bool = true;
    const RESPONSE_HEADER_VERSION: i16 = 1;

    fn api_name() -> &'static str {
        "OffsetFetch v8"
    }

    fn encode_body(&self, e: &mut Encoder) {
        // v8 moved the group id and topic list inside a `Groups` array so one
        // request can cover several groups; the top-level GroupId/Topics
        // fields only exist in versions 0-7 and are not encoded here.
        e.put_compact_array_len(1);
        e.put_compact_string(&self.group_id);
        match &self.topics {
            Some(topics) => {
                e.put_compact_array_len(topics.len());
                for (name, partitions) in topics {
                    e.put_compact_string(name);
                    e.put_compact_array_len(partitions.len());
                    for &p in partitions {
                        e.put_i32(p);
                    }
                    e.put_empty_tagged_fields();
                }
            }
            // Null, not empty: null means "all topics", empty means "none".
            None => e.put_null_compact_array(),
        }
        e.put_empty_tagged_fields(); // per-group tagged fields
        e.put_bool(self.require_stable);
        e.put_empty_tagged_fields();
    }

    fn decode_response(d: &mut Decoder) -> Result<OffsetFetchResponse> {
        let throttle_time_ms = d.i32()?;
        let groups = d.compact_array(|d| {
            let group_id = d.compact_string()?;
            let topics = d.compact_array(|d| {
                let name = d.compact_string()?;
                let partitions = d.compact_array(|d| {
                    let partition_index = d.i32()?;
                    let committed_offset = d.i64()?;
                    let committed_leader_epoch = d.i32()?;
                    let metadata = d.compact_nullable_string()?;
                    let error_code = d.i16()?;
                    d.skip_tagged_fields()?;
                    Ok(CommittedPartition {
                        partition_index,
                        committed_offset,
                        committed_leader_epoch,
                        metadata,
                        error_code,
                    })
                })?;
                d.skip_tagged_fields()?;
                Ok(CommittedTopic { name, partitions })
            })?;
            let error_code = d.i16()?;
            d.skip_tagged_fields()?;
            Ok(OffsetFetchGroup {
                group_id,
                topics,
                error_code,
            })
        })?;
        d.skip_tagged_fields()?;
        Ok(OffsetFetchResponse {
            throttle_time_ms,
            groups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_v8_body_for_all_topics() {
        let mut e = Encoder::new();
        OffsetFetchRequest::all_topics("g1").encode_body(&mut e);
        assert_eq!(
            e.as_slice(),
            &[
                0x02, // groups: 1
                0x03, b'g', b'1', // group id
                0x00, // null topics = every committed topic
                0x00, // group tagged fields
                0x00, // require_stable = false
                0x00, // request tagged fields
            ]
        );
    }

    #[test]
    fn encodes_v8_body_for_named_partitions() {
        let req = OffsetFetchRequest::for_topics("g1", vec![("demo".to_string(), vec![0, 1])]);
        let mut e = Encoder::new();
        req.encode_body(&mut e);
        assert_eq!(
            e.as_slice(),
            &[
                0x02, // groups: 1
                0x03, b'g', b'1', //
                0x02, // topics: 1
                0x05, b'd', b'e', b'm', b'o', //
                0x03, // partition indexes: 2 (a plain int32 array, no
                0x00, 0x00, 0x00, 0x00, // per-element tagged fields)
                0x00, 0x00, 0x00, 0x01, //
                0x00, // topic tagged fields
                0x00, // group tagged fields
                0x00, // require_stable
                0x00, // request tagged fields
            ]
        );
    }

    #[test]
    fn decodes_v8_body() {
        let body: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle
            0x02, // groups: 1
            0x03, b'g', b'1', //
            0x02, // topics: 1
            0x05, b'd', b'e', b'm', b'o', //
            0x03, // partitions: 2
            0x00, 0x00, 0x00, 0x00, // partition 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, // committed 40
            0x00, 0x00, 0x00, 0x07, // committed leader epoch
            0x01, // empty metadata string (not null)
            0x00, 0x00, // error 0
            0x00, // tagged fields
            0x00, 0x00, 0x00, 0x01, // partition 1
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // committed -1
            0xff, 0xff, 0xff, 0xff, // leader epoch -1
            0x00, // null metadata
            0x00, 0x00, // error 0
            0x00, // tagged fields
            0x00, // topic tagged fields
            0x00, 0x00, // group error code
            0x00, // group tagged fields
            0x00, // response tagged fields
        ];
        let mut d = Decoder::new(body);
        let resp = OffsetFetchRequest::decode_response(&mut d).unwrap();
        assert_eq!(d.remaining(), 0);
        let group = resp.group("g1").expect("group g1");
        assert_eq!(group.error_code, 0);
        let parts = &group.topics[0].partitions;
        assert_eq!(group.topics[0].name, "demo");
        assert_eq!(parts[0].committed_offset, 40);
        assert_eq!(parts[0].committed(), Some(40));
        assert_eq!(parts[0].metadata.as_deref(), Some(""));
        // -1 is the "never committed" sentinel, not a real offset.
        assert_eq!(parts[1].committed_offset, NO_COMMITTED_OFFSET);
        assert_eq!(parts[1].committed(), None);
        assert_eq!(parts[1].metadata, None);
    }
}
