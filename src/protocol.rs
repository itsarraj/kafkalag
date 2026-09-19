//! Request/response framing, headers, and the `Request` trait every API in
//! this crate implements.

use anyhow::{bail, Result};

use crate::wire::{Decoder, Encoder};

/// API keys, from the protocol guide's "Api Keys" table.
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_OFFSET_FETCH: i16 = 9;
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
pub const API_KEY_API_VERSIONS: i16 = 18;

/// One Kafka API this client knows how to speak, pinned to one version.
pub trait Request {
    type Response;

    const API_KEY: i16;
    const API_VERSION: i16;
    /// Whether this *request* version uses flexible encoding (compact
    /// lengths + tagged fields), which also selects request header v2.
    const FLEXIBLE: bool;
    /// Response header version. Normally 1 for a flexible version and 0
    /// otherwise — except ApiVersions, which always answers with a v0 header
    /// (the client cannot know whether the broker understood the flexible
    /// encoding until it has parsed the error code), so each API states it.
    const RESPONSE_HEADER_VERSION: i16;

    fn api_name() -> &'static str;
    fn encode_body(&self, e: &mut Encoder);
    fn decode_response(d: &mut Decoder) -> Result<Self::Response>;
}

/// Encodes a request header: v2 when `flexible`, v1 otherwise.
///
/// The quirk worth knowing: even in header v2, `client_id` is a *non-compact*
/// nullable string (`int16` length). It is the one field in a flexible-version
/// message that does not use a varint length, because the header predates
/// KIP-482 and was grandfathered in.
pub fn encode_request_header(
    e: &mut Encoder,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    flexible: bool,
) {
    e.put_i16(api_key);
    e.put_i16(api_version);
    e.put_i32(correlation_id);
    e.put_nullable_string(client_id);
    if flexible {
        e.put_empty_tagged_fields();
    }
}

/// Decodes a response header and returns the correlation id.
pub fn decode_response_header(d: &mut Decoder, header_version: i16) -> Result<i32> {
    let correlation_id = d.i32()?;
    if header_version >= 1 {
        d.skip_tagged_fields()?;
    }
    Ok(correlation_id)
}

/// Prefixes a header+body with its `int32` byte length, producing the exact
/// bytes that go on the socket.
pub fn frame(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 4);
    out.extend_from_slice(&(message.len() as i32).to_be_bytes());
    out.extend_from_slice(message);
    out
}

/// The subset of Kafka's error codes this tool can actually surface.
pub fn error_name(code: i16) -> &'static str {
    match code {
        -1 => "UNKNOWN_SERVER_ERROR",
        0 => "NONE",
        1 => "OFFSET_OUT_OF_RANGE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        5 => "LEADER_NOT_AVAILABLE",
        6 => "NOT_LEADER_OR_FOLLOWER",
        7 => "REQUEST_TIMED_OUT",
        9 => "REPLICA_NOT_AVAILABLE",
        13 => "NETWORK_EXCEPTION",
        14 => "COORDINATOR_LOAD_IN_PROGRESS",
        15 => "COORDINATOR_NOT_AVAILABLE",
        16 => "NOT_COORDINATOR",
        17 => "INVALID_TOPIC_EXCEPTION",
        25 => "UNKNOWN_MEMBER_ID",
        29 => "TOPIC_AUTHORIZATION_FAILED",
        30 => "GROUP_AUTHORIZATION_FAILED",
        35 => "UNSUPPORTED_VERSION",
        37 => "INVALID_PARTITIONS",
        41 => "NOT_CONTROLLER",
        43 => "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        68 => "FENCED_INSTANCE_ID",
        69 => "GROUP_ID_NOT_FOUND",
        _ => "UNKNOWN_ERROR_CODE",
    }
}

/// Turns a non-zero error code into an `Err` naming both the code and the
/// context it came from.
pub fn check_error(code: i16, context: &str) -> Result<()> {
    if code != 0 {
        bail!("{context}: broker returned {} ({code})", error_name(code));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_header_v2_layout() {
        let mut e = Encoder::new();
        encode_request_header(&mut e, API_KEY_API_VERSIONS, 3, 7, Some("kafkalag"), true);
        assert_eq!(
            e.as_slice(),
            &[
                0x00, 0x12, // api key 18
                0x00, 0x03, // api version 3
                0x00, 0x00, 0x00, 0x07, // correlation id 7
                // client id: NON-compact nullable string even in header v2
                0x00, 0x08, b'k', b'a', b'f', b'k', b'a', b'l', b'a', b'g',
                0x00, // empty tagged fields
            ]
        );
    }

    #[test]
    fn request_header_v1_has_no_tagged_fields() {
        let mut e = Encoder::new();
        encode_request_header(&mut e, API_KEY_METADATA, 1, 1, None, false);
        assert_eq!(
            e.as_slice(),
            &[
                0x00, 0x03, // api key 3
                0x00, 0x01, // api version 1
                0x00, 0x00, 0x00, 0x01, // correlation id 1
                0xff, 0xff, // null client id
            ]
        );
    }

    #[test]
    fn response_header_versions() {
        let v0: &[u8] = &[0x00, 0x00, 0x00, 0x2a];
        let mut d = Decoder::new(v0);
        assert_eq!(decode_response_header(&mut d, 0).unwrap(), 42);
        assert_eq!(d.remaining(), 0);

        let v1: &[u8] = &[0x00, 0x00, 0x00, 0x2a, 0x00];
        let mut d = Decoder::new(v1);
        assert_eq!(decode_response_header(&mut d, 1).unwrap(), 42);
        assert_eq!(d.remaining(), 0);
    }

    #[test]
    fn frame_prefixes_big_endian_length() {
        assert_eq!(
            frame(&[0xaa, 0xbb]),
            vec![0x00, 0x00, 0x00, 0x02, 0xaa, 0xbb]
        );
        assert_eq!(frame(&[]), vec![0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn error_codes_are_named() {
        assert!(check_error(0, "ctx").is_ok());
        let err = check_error(16, "offset fetch").unwrap_err().to_string();
        assert!(err.contains("NOT_COORDINATOR"), "{err}");
        assert!(err.contains("offset fetch"), "{err}");
        assert_eq!(error_name(3), "UNKNOWN_TOPIC_OR_PARTITION");
        assert_eq!(error_name(1234), "UNKNOWN_ERROR_CODE");
    }
}
