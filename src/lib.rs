//! A from-scratch Kafka wire-protocol client, just large enough to report
//! consumer-group lag.
//!
//! [`wire`] holds the protocol's primitive types, [`protocol`] the framing and
//! headers, one module per API ([`apiversions`], [`metadata`],
//! [`listoffsets`], [`coordinator`], [`offsetfetch`]), [`client`] the socket,
//! and [`lag`] the sequence that turns all of it into a lag report.

pub mod apiversions;
pub mod client;
pub mod coordinator;
pub mod lag;
pub mod listoffsets;
pub mod metadata;
pub mod offsetfetch;
pub mod protocol;
pub mod wire;
