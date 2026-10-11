# kafkalag

Report a Kafka consumer group's lag — per partition, current committed offset
vs. the log-end offset — from a single small binary. The canonical way to get
this number is `kafka-consumer-groups.sh --describe`, which means unpacking a
full Kafka distribution and starting a JVM on whatever box needs the answer.
That is a lot of machinery for four requests on a socket, and it is the reason
"just check the lag" is rarely wired into a healthcheck, a deploy gate, or a
laptop shell alias.

kafkalag speaks the Kafka wire protocol directly. No Kafka distribution, no
JVM, and no existing Kafka client crate underneath it — the whole thing is
`src/`, about 1,700 lines excluding tests.

## Installation

```sh
cargo install --path .
```

## Usage

```bash
kafkalag --broker localhost:9092 --group my-consumer-group
# GROUP              TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET   LAG
# my-consumer-group  events             0              41            1284  1243
# my-consumer-group  events             1             880             880     0
#
# total lag: 1243 across 2 partition(s); coordinator kafka-1.internal:9092

kafkalag -b localhost:9092 -g my-consumer-group -t events   # one topic only
kafkalag -b localhost:9092 -g my-consumer-group --json      # for a healthcheck
```

Without `--topic`, every topic the group has a committed offset for is
reported — the same set `kafka-consumer-groups.sh --describe` shows. With
`--topic`, the report covers *all* of that topic's partitions, including ones
the group has never committed to, which show as `-` rather than being silently
dropped. `--timeout` (seconds, default 10) bounds both the connect and each
read.

## What's actually implemented

Five Kafka APIs, each pinned to exactly one version, encoded and decoded by
hand from the protocol spec:

| API | Version | What it's for |
| --- | --- | --- |
| `ApiVersions` | v3 | Handshake; the broker's advertised ranges are checked against every version below before anything else is sent |
| `FindCoordinator` | v4 | Which broker owns this group's commits |
| `OffsetFetch` | v8 | The group's committed offset per partition (the `CURRENT-OFFSET` column) |
| `Metadata` | v12 | Partition list and the leader node for each one |
| `ListOffsets` | v7 | High-water mark per partition, via the special timestamp `-1` (the `LOG-END-OFFSET` column) |

All five are "flexible" versions, so the implementation covers the full
KIP-482 encoding: unsigned varint lengths, compact strings and arrays with
their off-by-one length prefix, `null` distinguished from empty, request
header v2, response headers v0 and v1, and a tagged-field section on every
struct that is *parsed and skipped* rather than assumed empty — a newer broker
is free to attach fields this client has never heard of, and it does.

Two protocol details that are easy to get wrong and are pinned down by tests:

- In request header v2 — a flexible header — `client_id` is still a
  **non-compact** nullable string with an `int16` length. It is the one field
  in a flexible message that does not use a varint, because the header
  predates the change.
- `ApiVersions` always answers with a **v0 response header**, no tagged
  fields, regardless of the request version. It has to: it is the call the
  client makes when it does not yet know what the broker understands. Every
  other API here uses a v1 response header.

The request path goes bootstrap broker → `FindCoordinator` → the coordinator
for `OffsetFetch`, and separately → each partition's leader for
`ListOffsets`. That routing is not an optimisation: a non-coordinator answers
`NOT_COORDINATOR` and a follower answers `NOT_LEADER_OR_FOLLOWER`. Connections
are pooled per address, so on a single-node cluster it is all one socket.

## Deliberately out of scope

- **No produce, no consume, no fetch.** This reads offsets; it is not a
  client library. `Fetch`, `Produce`, and the record-batch format (with its
  own compression and CRC) are not implemented at all.
- **No group membership.** `DescribeGroups`/`ListGroups` aren't implemented,
  so there are no `CONSUMER-ID`/`HOST`/`CLIENT-ID` columns and no way to list
  groups — you pass the group name in. A group rebalancing mid-query is
  simply read as it is at that instant; there's no retry-on-rebalance.
- **No version negotiation.** One pinned version per API. The `ApiVersions`
  handshake *checks* the broker supports them and fails with a clear message
  naming the advertised range if not — it does not fall back to an older
  encoding. In practice the pinned set needs Kafka 3.0 or newer.
- **No TLS, no SASL.** Plaintext listeners only.
- **No transactional read semantics.** `ListOffsets` is sent with
  `read_uncommitted`, so the reported log-end offset is the high-water mark,
  not the last stable offset. That matches `kafka-consumer-groups.sh`'s
  default, and means lag on a topic with open transactions includes records
  not yet committed.
- **Multi-broker clusters are handled but only single-broker-verified** (see
  below). The code groups partitions by leader and opens a connection per
  leader address, so a real cluster should work, but that path has not been
  run against more than one broker. A partition with no live leader is a hard
  error rather than a `-` row.

## Status: built and verified live against a real Kafka 4.3.1 broker, numbers matched against the broker's own tooling

- **32 unit tests** (`cargo test`, no Docker or network needed): exact-byte
  assertions on every request this tool sends — request header v2 (including
  the non-compact `client_id`) and v1, `ApiVersions` v3, `Metadata` v12 (both
  a named topic with its all-zero topic-id placeholder, and the null-vs-empty
  array distinction for "all topics"), `ListOffsets` v7 with the `-1`
  latest-offset timestamp, `FindCoordinator` v4, and `OffsetFetch` v8 in both
  its all-topics and named-partitions forms. Decoding is tested against
  hand-written response byte sequences — not bytes this crate produced, which
  would only prove it agrees with itself — with the decoder asserted to
  consume each body to exactly zero remaining bytes, so a field of the wrong
  width fails the test rather than silently shifting everything after it. Plus
  the primitives: varint boundaries at 127/128/16383/16384, compact lengths
  being `len + 1`, `null` vs. empty for both strings and arrays, big-endian
  integers, tagged-field sections with real unknown tags skipped correctly,
  and truncated input producing an error instead of a panic.
- **Live verification against a real broker**: `apache/kafka:latest` (Kafka
  **4.3.1**) in single-node KRaft mode, in Docker, mapped to host port 59092.
  A `lagdemo` topic was seeded with 160 real messages via the broker's own
  bundled `kafka-console-producer.sh`, then partially drained — 40 messages —
  by a real `kafka-console-consumer.sh --group lagdemo-group --max-messages
  40`, leaving a genuine known lag. More messages were then produced to widen
  it. Every number below is from the actual terminal output.

  kafkalag:

  ```
  GROUP          TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET         LAG
  lagdemo-group  lagdemo          0               3             151         148
  lagdemo-group  lagdemo          1              20              30          10
  lagdemo-group  lagdemo          2              17              39          22

  total lag: 180 across 3 partition(s); coordinator localhost:59092
  ```

  `kafka-consumer-groups.sh --describe --group lagdemo-group`, run inside the
  same container against the same broker at the same moment:

  ```
  GROUP           TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
  lagdemo-group   lagdemo   0          3               151             148
  lagdemo-group   lagdemo   1          20              30              10
  lagdemo-group   lagdemo   2          17              39              22
  ```

  Identical in all three columns on all three partitions. The log-end offsets
  were independently confirmed a third way with `kafka-get-offsets.sh`
  (`lagdemo:0:151`, `lagdemo:1:30`, `lagdemo:2:39`), so the high-water marks
  are not just kafkalag and one other tool agreeing on the same mistake. An
  earlier round at a different lag matched too (`120 / 0 / 0` against
  `120 / 0 / 0`).
- **The never-committed sentinel was verified against the real broker, not
  just in a unit test**: the topic was grown to 4 partitions and a few
  messages produced into the new partition 3, which the group had no commit
  for. kafkalag reported `3  -  3  -` (no current offset, log-end offset 3, no
  lag) and left it out of the total — `156 + 14 + 28 = 198`, with partition 3
  contributing nothing. `kafka-consumer-groups.sh` omits that partition from
  its output entirely, and the log-end offset of 3 was confirmed by
  `kafka-get-offsets.sh`. Reading Kafka's `-1` sentinel as a literal offset
  would have reported a lag of 4 there and a wrong total.
- **Error paths exercised live**: a group that has never existed
  (`does-not-exist`) reports no committed offsets rather than failing; an
  unknown topic surfaces the broker's real error code by name
  (`Metadata for topic "nosuchtopic": broker returned
  UNKNOWN_TOPIC_OR_PARTITION (3)`); a dead port fails with `connecting to
  localhost:59099: Connection refused` instead of hanging.
- **`tests/live_broker.rs`** holds the two live tests — one asserting the real
  broker advertises every pinned API version, one asserting real lag
  arithmetic end-to-end. Both are `#[ignore]`d, so plain `cargo test` and CI
  skip them; both pass against the broker above with `cargo test -- --ignored`
  (`2 passed`). The container was removed afterwards.

**Not done / deliberately deferred**, beyond the scope limits above: no
watch/poll mode (no `--interval` that re-reports every N seconds, the obvious
next thing for a terminal left open during an incident); no exit-code
threshold (`--max-lag N` returning non-zero, which is what would make this
drop straight into a healthcheck); and no verification against a real
multi-broker cluster, where the leader-grouping code path would actually open
more than one connection.
