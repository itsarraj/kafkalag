//! ListOffsets v7 — asks a partition's leader for the offset at a timestamp.
//! With the special timestamp `-1` that is the *latest* offset, i.e. the
//! high-water mark: the offset the next produced record will get.

use std::collections::BTreeMap;

use anyhow::Result;

use crate::protocol::{Request, API_KEY_LIST_OFFSETS};
use crate::wire::{Decoder, Encoder};

/// Special timestamp meaning "the latest offset" (the high-water mark).
pub const TIMESTAMP_LATEST: i64 = -1;
/// Special timestamp meaning "the earliest offset" (the log start offset).
pub const TIMESTAMP_EARLIEST: i64 = -2;

/// `replica_id = -1` marks the caller as an ordinary consumer rather than a
/// follower broker; anything else and the leader rejects the request.
const CONSUMER_REPLICA_ID: i32 = -1;
/// `read_uncommitted`, matching what `kafka-consumer-groups.sh` uses by
/// default — so the number reported here is the high-water mark, not the last
/// stable offset.
const ISOLATION_READ_UNCOMMITTED: i8 = 0;

#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    pub timestamp: i64,
    /// Topic name -> partition indexes, ordered so the encoding is
    /// deterministic and testable.
    pub topics: BTreeMap<String, Vec<i32>>,
}

impl ListOffsetsRequest {
    pub fn latest(topics: BTreeMap<String, Vec<i32>>) -> Self {
        Self {
            timestamp: TIMESTAMP_LATEST,
            topics,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedOffset {
    pub partition_index: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopicResponse {
    pub name: String,
    pub partitions: Vec<ListedOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<ListOffsetsTopicResponse>,
}

impl Request for ListOffsetsRequest {
    type Response = ListOffsetsResponse;

    const API_KEY: i16 = API_KEY_LIST_OFFSETS;
    const API_VERSION: i16 = 7;
    const FLEXIBLE: bool = true;
    const RESPONSE_HEADER_VERSION: i16 = 1;

    fn api_name() -> &'static str {
        "ListOffsets v7"
    }

    fn encode_body(&self, e: &mut Encoder) {
        e.put_i32(CONSUMER_REPLICA_ID);
        e.put_i8(ISOLATION_READ_UNCOMMITTED);
        e.put_compact_array_len(self.topics.len());
        for (name, partitions) in &self.topics {
            e.put_compact_string(name);
            e.put_compact_array_len(partitions.len());
            for &partition in partitions {
                e.put_i32(partition);
                e.put_i32(-1); // current_leader_epoch: -1 = do not fence me
                e.put_i64(self.timestamp);
                e.put_empty_tagged_fields();
            }
            e.put_empty_tagged_fields();
        }
        e.put_empty_tagged_fields();
    }

    fn decode_response(d: &mut Decoder) -> Result<ListOffsetsResponse> {
        let throttle_time_ms = d.i32()?;
        let topics = d.compact_array(|d| {
            let name = d.compact_string()?;
            let partitions = d.compact_array(|d| {
                let partition_index = d.i32()?;
                let error_code = d.i16()?;
                let timestamp = d.i64()?;
                let offset = d.i64()?;
                let leader_epoch = d.i32()?;
                d.skip_tagged_fields()?;
                Ok(ListedOffset {
                    partition_index,
                    error_code,
                    timestamp,
                    offset,
                    leader_epoch,
                })
            })?;
            d.skip_tagged_fields()?;
            Ok(ListOffsetsTopicResponse { name, partitions })
        })?;
        d.skip_tagged_fields()?;
        Ok(ListOffsetsResponse {
            throttle_time_ms,
            topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_v7_body() {
        let mut topics = BTreeMap::new();
        topics.insert("demo".to_string(), vec![0, 1]);
        let mut e = Encoder::new();
        ListOffsetsRequest::latest(topics).encode_body(&mut e);
        assert_eq!(
            e.as_slice(),
            &[
                0xff, 0xff, 0xff, 0xff, // replica id -1
                0x00, // isolation level read_uncommitted
                0x02, // topics: 1
                0x05, b'd', b'e', b'm', b'o', //
                0x03, // partitions: 2
                0x00, 0x00, 0x00, 0x00, // p0
                0xff, 0xff, 0xff, 0xff, // leader epoch -1
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // timestamp -1 (latest)
                0x00, // p0 tagged fields
                0x00, 0x00, 0x00, 0x01, // p1
                0xff, 0xff, 0xff, 0xff, //
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, //
                0x00, // p1 tagged fields
                0x00, // topic tagged fields
                0x00, // request tagged fields
            ]
        );
    }

    #[test]
    fn decodes_v7_body() {
        let body: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle
            0x02, // topics: 1
            0x05, b'd', b'e', b'm', b'o', //
            0x02, // partitions: 1
            0x00, 0x00, 0x00, 0x00, // partition 0
            0x00, 0x00, // error 0
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // timestamp -1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // offset 100
            0x00, 0x00, 0x00, 0x07, // leader epoch 7
            0x00, // partition tagged fields
            0x00, // topic tagged fields
            0x00, // response tagged fields
        ];
        let mut d = Decoder::new(body);
        let resp = ListOffsetsRequest::decode_response(&mut d).unwrap();
        assert_eq!(d.remaining(), 0);
        assert_eq!(
            resp.topics,
            vec![ListOffsetsTopicResponse {
                name: "demo".to_string(),
                partitions: vec![ListedOffset {
                    partition_index: 0,
                    error_code: 0,
                    timestamp: -1,
                    offset: 100,
                    leader_epoch: 7,
                }],
            }]
        );
    }
}
