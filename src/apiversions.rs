//! ApiVersions v3 — the handshake every Kafka client opens with, used here to
//! confirm the broker actually supports the versions this client has pinned
//! before anything else is sent.

use anyhow::Result;

use crate::protocol::{Request, API_KEY_API_VERSIONS};
use crate::wire::{Decoder, Encoder};

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {
    pub client_software_name: String,
    pub client_software_version: String,
}

impl Default for ApiVersionsRequest {
    fn default() -> Self {
        Self {
            client_software_name: "kafkalag".to_string(),
            client_software_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersionRange>,
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    pub fn range_for(&self, api_key: i16) -> Option<&ApiVersionRange> {
        self.api_keys.iter().find(|r| r.api_key == api_key)
    }

    /// Whether the broker's advertised range covers a version we want to send.
    pub fn supports(&self, api_key: i16, version: i16) -> bool {
        self.range_for(api_key)
            .map(|r| r.min_version <= version && version <= r.max_version)
            .unwrap_or(false)
    }
}

impl Request for ApiVersionsRequest {
    type Response = ApiVersionsResponse;

    const API_KEY: i16 = API_KEY_API_VERSIONS;
    const API_VERSION: i16 = 3;
    const FLEXIBLE: bool = true;
    // ApiVersions is the one API whose response header is *always* v0, even
    // for flexible request versions: the broker has to be able to answer a
    // request whose version it does not understand.
    const RESPONSE_HEADER_VERSION: i16 = 0;

    fn api_name() -> &'static str {
        "ApiVersions v3"
    }

    fn encode_body(&self, e: &mut Encoder) {
        e.put_compact_string(&self.client_software_name);
        e.put_compact_string(&self.client_software_version);
        e.put_empty_tagged_fields();
    }

    fn decode_response(d: &mut Decoder) -> Result<ApiVersionsResponse> {
        let error_code = d.i16()?;
        // If the broker rejects v3 outright it replies in the *v0* body
        // format, so there is nothing flexible left to parse. Hand the error
        // code back and let the caller report it.
        if error_code != 0 {
            return Ok(ApiVersionsResponse {
                error_code,
                api_keys: Vec::new(),
                throttle_time_ms: 0,
            });
        }
        let api_keys = d.compact_array(|d| {
            let api_key = d.i16()?;
            let min_version = d.i16()?;
            let max_version = d.i16()?;
            d.skip_tagged_fields()?;
            Ok(ApiVersionRange {
                api_key,
                min_version,
                max_version,
            })
        })?;
        let throttle_time_ms = d.i32()?;
        d.skip_tagged_fields()?;
        Ok(ApiVersionsResponse {
            error_code,
            api_keys,
            throttle_time_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_v3_body() {
        let req = ApiVersionsRequest {
            client_software_name: "kafkalag".to_string(),
            client_software_version: "0.1.0".to_string(),
        };
        let mut e = Encoder::new();
        req.encode_body(&mut e);
        assert_eq!(
            e.as_slice(),
            &[
                0x09, b'k', b'a', b'f', b'k', b'a', b'l', b'a', b'g', // compact "kafkalag"
                0x06, b'0', b'.', b'1', b'.', b'0', // compact "0.1.0"
                0x00, // tagged fields
            ]
        );
    }

    #[test]
    fn decodes_v3_body() {
        let body: &[u8] = &[
            0x00, 0x00, // error code 0
            0x03, // compact array: 2 entries
            0x00, 0x12, 0x00, 0x00, 0x00, 0x04, 0x00, // ApiVersions(18) 0..=4
            0x00, 0x03, 0x00, 0x00, 0x00, 0x0d, 0x00, // Metadata(3) 0..=13
            0x00, 0x00, 0x00, 0x00, // throttle 0
            0x00, // tagged fields
        ];
        let mut d = Decoder::new(body);
        let resp = ApiVersionsRequest::decode_response(&mut d).unwrap();
        assert_eq!(d.remaining(), 0);
        assert_eq!(
            resp,
            ApiVersionsResponse {
                error_code: 0,
                api_keys: vec![
                    ApiVersionRange {
                        api_key: 18,
                        min_version: 0,
                        max_version: 4
                    },
                    ApiVersionRange {
                        api_key: 3,
                        min_version: 0,
                        max_version: 13
                    },
                ],
                throttle_time_ms: 0,
            }
        );
        assert!(resp.supports(3, 12));
        assert!(!resp.supports(3, 14));
        assert!(!resp.supports(9, 8), "api key absent entirely");
    }

    #[test]
    fn unsupported_version_short_circuits_instead_of_misparsing() {
        // A broker that does not know v3 replies with the v0 body shape. The
        // decoder must not try to read a compact array out of it.
        let body: &[u8] = &[0x00, 0x23];
        let mut d = Decoder::new(body);
        let resp = ApiVersionsRequest::decode_response(&mut d).unwrap();
        assert_eq!(resp.error_code, 35);
        assert!(resp.api_keys.is_empty());
    }
}
