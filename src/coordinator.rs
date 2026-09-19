//! FindCoordinator v4 — locates the broker that owns a consumer group's
//! committed offsets. Committed offsets live in `__consumer_offsets`, and only
//! the group's coordinator will answer an OffsetFetch for it, so this lookup
//! is mandatory even on a single-broker cluster.

use anyhow::Result;

use crate::protocol::{Request, API_KEY_FIND_COORDINATOR};
use crate::wire::{Decoder, Encoder};

/// `key_type = 0` is GROUP (as opposed to 1, TRANSACTION).
pub const KEY_TYPE_GROUP: i8 = 0;

#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    pub key_type: i8,
    /// v4 batched the lookup: one request can resolve several groups.
    pub keys: Vec<String>,
}

impl FindCoordinatorRequest {
    pub fn for_group(group: impl Into<String>) -> Self {
        Self {
            key_type: KEY_TYPE_GROUP,
            keys: vec![group.into()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coordinator {
    pub key: String,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub coordinators: Vec<Coordinator>,
}

impl FindCoordinatorResponse {
    pub fn for_key(&self, key: &str) -> Option<&Coordinator> {
        self.coordinators.iter().find(|c| c.key == key)
    }
}

impl Request for FindCoordinatorRequest {
    type Response = FindCoordinatorResponse;

    const API_KEY: i16 = API_KEY_FIND_COORDINATOR;
    const API_VERSION: i16 = 4;
    const FLEXIBLE: bool = true;
    const RESPONSE_HEADER_VERSION: i16 = 1;

    fn api_name() -> &'static str {
        "FindCoordinator v4"
    }

    fn encode_body(&self, e: &mut Encoder) {
        // v4 drops the single `Key` field (versions 0-3) entirely; KeyType
        // comes first, then the batched key array.
        e.put_i8(self.key_type);
        e.put_compact_array_len(self.keys.len());
        for key in &self.keys {
            // An array of plain strings, so no per-element tagged fields.
            e.put_compact_string(key);
        }
        e.put_empty_tagged_fields();
    }

    fn decode_response(d: &mut Decoder) -> Result<FindCoordinatorResponse> {
        let throttle_time_ms = d.i32()?;
        let coordinators = d.compact_array(|d| {
            let key = d.compact_string()?;
            let node_id = d.i32()?;
            let host = d.compact_string()?;
            let port = d.i32()?;
            let error_code = d.i16()?;
            let error_message = d.compact_nullable_string()?;
            d.skip_tagged_fields()?;
            Ok(Coordinator {
                key,
                node_id,
                host,
                port,
                error_code,
                error_message,
            })
        })?;
        d.skip_tagged_fields()?;
        Ok(FindCoordinatorResponse {
            throttle_time_ms,
            coordinators,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_v4_body() {
        let mut e = Encoder::new();
        FindCoordinatorRequest::for_group("g1").encode_body(&mut e);
        assert_eq!(
            e.as_slice(),
            &[
                0x00, // key type GROUP
                0x02, // compact array: 1 key
                0x03, b'g', b'1', // compact "g1"
                0x00, // request tagged fields
            ]
        );
    }

    #[test]
    fn decodes_v4_body() {
        let body: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle
            0x02, // coordinators: 1
            0x03, b'g', b'1', // key "g1"
            0x00, 0x00, 0x00, 0x01, // node id 1
            0x0a, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', //
            0x00, 0x00, 0xe6, 0xac, // port 59052
            0x00, 0x00, // error 0
            0x00, // null error message
            0x00, // coordinator tagged fields
            0x00, // response tagged fields
        ];
        let mut d = Decoder::new(body);
        let resp = FindCoordinatorRequest::decode_response(&mut d).unwrap();
        assert_eq!(d.remaining(), 0);
        assert_eq!(
            resp.for_key("g1"),
            Some(&Coordinator {
                key: "g1".to_string(),
                node_id: 1,
                host: "localhost".to_string(),
                port: 59052,
                error_code: 0,
                error_message: None,
            })
        );
        assert!(resp.for_key("nope").is_none());
    }

    #[test]
    fn decodes_a_per_coordinator_error() {
        let body: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle
            0x02, // coordinators: 1
            0x03, b'g', b'1', //
            0xff, 0xff, 0xff, 0xff, // node id -1
            0x01, // empty host
            0xff, 0xff, 0xff, 0xff, // port -1
            0x00, 0x0f, // error 15 COORDINATOR_NOT_AVAILABLE
            0x06, b'n', b'o', b'p', b'e', b'!', // error message
            0x00, 0x00,
        ];
        let mut d = Decoder::new(body);
        let resp = FindCoordinatorRequest::decode_response(&mut d).unwrap();
        let c = resp.for_key("g1").unwrap();
        assert_eq!(c.error_code, 15);
        assert_eq!(c.error_message.as_deref(), Some("nope!"));
        assert_eq!(c.host, "");
    }
}
