//! A blocking Kafka connection: one TCP socket, length-prefixed frames, and a
//! monotonically increasing correlation id that every response is checked
//! against.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};

use crate::protocol::{decode_response_header, encode_request_header, frame, Request};
use crate::wire::{Decoder, Encoder};

/// Refuse to allocate a buffer for an implausible response size. Kafka's own
/// default `socket.request.max.bytes` is 100 MiB; anything larger here means
/// we are misframed, not that the broker is chatty.
const MAX_RESPONSE_BYTES: usize = 100 * 1024 * 1024;

pub const DEFAULT_PORT: u16 = 9092;

/// Splits `host:port`, defaulting the port to 9092 when it is omitted.
pub fn parse_broker_addr(addr: &str) -> Result<(String, u16)> {
    let addr = addr.trim();
    if addr.is_empty() {
        bail!("empty broker address");
    }
    // Bracketed IPv6 literal, e.g. [::1]:9092
    if let Some(rest) = addr.strip_prefix('[') {
        let (host, tail) = rest
            .split_once(']')
            .ok_or_else(|| anyhow!("unterminated IPv6 literal in broker address {addr:?}"))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => p
                .parse::<u16>()
                .with_context(|| format!("bad port in broker address {addr:?}"))?,
            None => DEFAULT_PORT,
        };
        return Ok((host.to_string(), port));
    }
    match addr.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => {
            let port = port
                .parse::<u16>()
                .with_context(|| format!("bad port in broker address {addr:?}"))?;
            Ok((host.to_string(), port))
        }
        _ => Ok((addr.to_string(), DEFAULT_PORT)),
    }
}

#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    client_id: String,
    next_correlation_id: i32,
    peer: String,
}

impl Connection {
    pub fn connect(host: &str, port: u16, timeout: Duration, client_id: &str) -> Result<Self> {
        let peer = format!("{host}:{port}");
        let addr = (host, port)
            .to_socket_addrs()
            .with_context(|| format!("resolving {peer}"))?
            .next()
            .ok_or_else(|| anyhow!("{peer} resolved to no addresses"))?;
        let stream = TcpStream::connect_timeout(&addr, timeout)
            .with_context(|| format!("connecting to {peer}"))?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            client_id: client_id.to_string(),
            next_correlation_id: 1,
            peer,
        })
    }

    pub fn peer(&self) -> &str {
        &self.peer
    }

    /// Sends one request and blocks for its response.
    pub fn send<R: Request>(&mut self, request: &R) -> Result<R::Response> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1).max(1);

        let mut e = Encoder::new();
        encode_request_header(
            &mut e,
            R::API_KEY,
            R::API_VERSION,
            correlation_id,
            Some(&self.client_id),
            R::FLEXIBLE,
        );
        request.encode_body(&mut e);
        let payload = frame(e.as_slice());

        self.stream
            .write_all(&payload)
            .with_context(|| format!("sending {} to {}", R::api_name(), self.peer))?;
        self.stream.flush()?;

        let body = self
            .read_frame()
            .with_context(|| format!("reading {} response from {}", R::api_name(), self.peer))?;

        let mut d = Decoder::new(&body);
        let got = decode_response_header(&mut d, R::RESPONSE_HEADER_VERSION)?;
        if got != correlation_id {
            bail!(
                "{}: correlation id mismatch (sent {correlation_id}, got {got}) — the \
                 connection is out of sync",
                R::api_name()
            );
        }
        let response = R::decode_response(&mut d)
            .with_context(|| format!("decoding {} response", R::api_name()))?;
        if d.remaining() != 0 {
            bail!(
                "{}: {} trailing byte(s) after decoding — wire format mismatch",
                R::api_name(),
                d.remaining()
            );
        }
        Ok(response)
    }

    fn read_frame(&mut self) -> Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let len = i32::from_be_bytes(len_buf);
        if len < 0 || len as usize > MAX_RESPONSE_BYTES {
            bail!("implausible response frame length {len}");
        }
        let mut body = vec![0u8; len as usize];
        self.stream.read_exact(&mut body)?;
        Ok(body)
    }
}

/// Keeps one connection per broker address. A lag report touches the
/// bootstrap broker, the group coordinator, and every partition leader — on a
/// single-node cluster those are all the same socket, and this makes that the
/// cheap case without special-casing it.
#[derive(Debug)]
pub struct ConnectionPool {
    connections: HashMap<String, Connection>,
    timeout: Duration,
    client_id: String,
}

impl ConnectionPool {
    pub fn new(timeout: Duration, client_id: impl Into<String>) -> Self {
        Self {
            connections: HashMap::new(),
            timeout,
            client_id: client_id.into(),
        }
    }

    pub fn get(&mut self, host: &str, port: u16) -> Result<&mut Connection> {
        let key = format!("{host}:{port}");
        if !self.connections.contains_key(&key) {
            let conn = Connection::connect(host, port, self.timeout, &self.client_id)?;
            self.connections.insert(key.clone(), conn);
        }
        Ok(self.connections.get_mut(&key).expect("just inserted above"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_broker_addresses() {
        assert_eq!(
            parse_broker_addr("localhost:59092").unwrap(),
            ("localhost".to_string(), 59092)
        );
        assert_eq!(
            parse_broker_addr("kafka.internal").unwrap(),
            ("kafka.internal".to_string(), DEFAULT_PORT)
        );
        assert_eq!(
            parse_broker_addr("  10.0.0.5:9094  ").unwrap(),
            ("10.0.0.5".to_string(), 9094)
        );
        assert_eq!(
            parse_broker_addr("[::1]:9092").unwrap(),
            ("::1".to_string(), 9092)
        );
        assert_eq!(
            parse_broker_addr("[::1]").unwrap(),
            ("::1".to_string(), DEFAULT_PORT)
        );
    }

    #[test]
    fn rejects_bad_broker_addresses() {
        assert!(parse_broker_addr("").is_err());
        assert!(parse_broker_addr("host:notaport").is_err());
        assert!(parse_broker_addr("host:99999").is_err());
        assert!(parse_broker_addr("[::1:9092").is_err());
    }
}
