# CockroachDB Source Connector for PeerDB — Strategy Draft

> **Status (2026-07-06):** Phases 0–2 are implemented on `feat/cockroachdb-sinkless-source` (sinkless-changefeed design, chosen over the webhook sink discussed below) and verified against live CockroachDB v26.1.6, including a full CRDB→ClickHouse e2e suite through the real Temporal workflows and connector-level chaos tests. Remaining: Phase 3 hardening (schema deltas, add/remove tables, lag observability) and CI verification of the nexus (Rust) and UI builds.

## Context

PR [#3861](https://github.com/PeerDB-io/peerdb/pull/3861) ("Add native CockroachDB connector support", by viragtripathi, opened 2026-01-21, +936/−2) claims "full CockroachDB integration as a source peer" but actually only implements **peer registration**:

- `flow/connectors/cockroachdb/` — pgx-based client, schema introspection (GetSchemas/GetTables/GetColumns), connection validation, version/variant detection
- Proto config, nexus (Rust) analyzer/catalog plumbing, UI forms/logo

**Missing entirely:** any CDC implementation (`CDCPullConnector`), any snapshot/initial-load strategy (QRep pull), replication setup, type mapping to qvalue kinds, offset/checkpoint handling. The user's suspicion is correct: as written, the PR lets you create a CockroachDB peer that can't actually mirror anything.

Goal: design a real strategy for CockroachDB CDC in PeerDB, with PeerDB acting as the **webhook sink** for a CockroachDB changefeed (CRDB CDC is push-based; it does not support Postgres logical replication).

## Research (in progress)

- [ ] PeerDB source-connector contracts (interfaces, PullRecords, snapshot, type mapping)
- [x] CDC workflow lifecycle + deployment topology (where a webhook listener could live)
- [x] CockroachDB changefeed semantics (webhook sink format, resolved timestamps, ordering, initial_scan, GC/protected timestamps, licensing)

### Findings: CockroachDB changefeeds

- **No PG logical replication**: CRDB speaks pgwire but has no replication slots/pgoutput. Changefeeds are the only third-party CDC egress (PCR/LDR are CRDB-to-CRDB only).
- **Webhook sink** (`webhook-https://`): HTTPS-only POSTs, JSON body `{"payload":[...],"length":N}`; PK in `key` array; deletes = `after:null`; `WITH diff` adds `before`. Batching/retry via `webhook_sink_config` (Flush.Messages/Bytes/Frequency, Retry.Max/Backoff); auth via `webhook_auth_header`; mTLS via `client_cert/client_key`; `ca_cert` for private CAs. At-least-once with duplicates; per-key ordering only; ack = 2xx returned only after durable write.
- **Enriched envelope (v25.2+)**: Debezium-compatible — `op` (c/u/d), `ts_ns`, optional `source` (ts_hlc, cluster_id, table_name) and schema block. Best fit for typed ingestion.
- **Checkpointing**: `resolved` messages = high-water HLC (floor: `min_checkpoint_frequency`, default 30s). Consumer persists last resolved HLC; restart via `CREATE CHANGEFEED ... WITH cursor='<hlc>', initial_scan='no'`. HLC = `<nanos>.<logical>` decimal string → fits `CdcCheckpoint.Text`.
- **Initial load**: `initial_scan='yes'` (backfill then stream), `'only'` (one-shot), `'no'`. Alternative: PeerDB does its own snapshot via `AS OF SYSTEM TIME <hlc>` SQL reads (CRDB supports it), then starts changefeed with `cursor=<same hlc>` — mirrors the Mongo resume-token/MySQL GTID pattern.
- **GC interplay**: changefeeds hold protected timestamps; a paused/dead consumer pins MVCC history until `gc_protect_expires_after`. If protection expires, changefeed must be recreated + full re-snapshot.
- **Schema changes**: `schema_change_policy` backfill (default, re-emits all rows) / nobackfill / stop. TRUNCATE/DROP fail the feed.
- **Ops**: requires `kv.rangefeed.enabled=true`; `CHANGEFEED` privilege; multi-table per feed OK (≤~80 feeds/cluster guidance); managed via CREATE/ALTER/PAUSE/RESUME CHANGEFEED + SHOW CHANGEFEED JOBS.
- **Licensing**: post-v24.3 single Enterprise license (free tier for <$10M revenue orgs); sink-backed changefeeds available in Cloud Standard/Advanced.
- **Prior art**: Debezium (official incubating connector, CRDB 25.2+, consumes changefeed via Kafka, `envelope='enriched'`), Materialize/Striim/Fivetran all ride changefeeds, mostly Kafka sink. Webhook-sink-direct is a legitimate, less-trodden path — avoids Kafka dependency, but PeerDB owns the HTTPS receiver + durable ack.
- **Open items to verify against `changefeedccl` source**: exact webhook resolved-message JSON shape; exact HTTP status-code ack/retry contract; behavior at Retry.Max exhaustion.

### Findings: PeerDB CDC lifecycle & topology

- Repo is greenfield for CRDB: only hit is `github.com/cockroachdb/pebble/v2` used as on-disk KV cache in `flow/connectors/utils/cdc_store.go` (useful precedent for durable local buffering).
- **Pull model contract**: `CDCFlowWorkflow` (flow/workflows/cdc_flow.go) runs a single year-long `SyncFlow` activity; `pullAndSyncCore` (flow/activities/flowable_core.go:121-256) calls the connector's blocking `PullRecords`, which owns the source connection and pushes into a buffered `CDCStream` channel; batch ends on `MaxBatchSize`/`IdleTimeout`. Non-Postgres sources use the `TypeSystem_Q` path (generic RecordItems).
- **Source branching**: connector factory `getConnector` in flow/connectors/core.go:552-579 switches on proto peer config oneof; interface assertions at core.go:674-679. New source = new `DBType` + config proto + factory case + interfaces: `CDCPullConnectorCore` (EnsurePullability, SetupReplication, SetupReplConn, ExportTxSnapshot, PullFlowCleanup), `PullRecords`, GetTableSchema, QRep pull for snapshot.
- **Checkpoints are dual-typed**: `model.CdcCheckpoint{Text string; ID int64}` → catalog `metadata_last_sync_state.last_offset/last_text`. MySQL GTID and Mongo resume tokens use `Text` — a CRDB HLC timestamp fits `Text` naturally.
- **Setup sequence**: CheckConnection → EnsurePullability → CreateRawTable → SetupTableSchema → CreateNormalizedTable → SnapshotFlow (one child QRepFlow per table, idempotent via `metadata_qrep_partitions`) → ContinueAsNew into sync loop.
- **No inbound HTTP anywhere**: flow-api serves only gRPC (8112) + grpc-gateway (8113, proto-generated routes only); flow-worker/snapshot-worker expose no ports. A webhook sink requires a brand-new inbound HTTPS listener + durable buffer bridging push→pull, or using a different changefeed sink (Kafka/cloud storage) that can be pulled.
- Destinations: post-deprecation (commit 477439e5), ClickHouse + Postgres are the first-class destinations; ClickHouse normalization is raw table → ReplacingMergeTree dedup, at-least-once friendly.

### Findings: PeerDB source-connector contracts (details)

- A source connector = one Go package implementing `CDCPullConnector` (PullRecords + EnsurePullability/SetupReplication/SetupReplConn/UpdateReplStateLastOffset/PullFlowCleanup), `QRepPullConnector` (GetQRepPartitions/GetDefaultPartitionKeyForTables/PullQRepRecords), `GetTableSchemaConnector`, `GetSchemaConnector`, `ValidationConnector`/`MirrorSourceValidationConnector`. Registration = 3 type switches (`getConnector` core.go:553, `BuildPeerConfig` core.go:461, protos DBType+oneof) + compile-time assertions core.go:674-790.
- MySQL/Mongo embed `*metadataStore.PostgresMetadata` for offset persistence (catalog `metadata_last_sync_state`); `CdcCheckpoint.Text` holds GTID/resume-token — CRDB resolved HLC goes there.
- `PullRecords` contract: blocking batch call; fill `CDCStream` until MaxBatchSize or IdleTimeout; `SignalAsNotEmpty`/`SignalAsEmpty`; `UpdateLatestCheckpointText` as safe restart points advance; `defer Close()`. Runs inside the year-long `SyncFlow` Temporal activity in flow-worker.
- Snapshot ordering: `SetupReplication` captures the CDC start position BEFORE snapshot (MySQL GTID, Mongo resume token); snapshot = one child QRepFlow per table with range partitions. CRDB can beat both via `AS OF SYSTEM TIME <hlc>` reads bound to the changefeed cursor.
- Type mapping: PG OID→QValueKind (`postgres/qvalue_convert.go`) reusable for pgwire snapshot reads; CDC values arrive as changefeed JSON and need a JSON→QValue decoder keyed by column QValueKind (decimal-as-string, bytes-as-base64, HLC strings).
- Schema deltas: additive-only `TableSchemaDelta{AddedColumns}` pattern (MySQL binlog DDL parsing) flows through `SyncResponse.TableSchemaDeltas` → destination `ReplayTableSchemaDeltas`.
- **No always-on data-plane HTTP receiver exists**; flow-api HTTP is grpc-gateway control plane only; Kafka is destination-only (no Kafka source). The webhook must land in a durable **shared** buffer (receiver runs in api/ingest process; PullRecords runs in flow-worker — different processes, so local disk/pebble won't work; catalog Postgres is the natural buffer).

---

## Strategy

### Verdict on PR #3861

Salvage the plumbing (proto config shape, connector skeleton, UI, nexus wiring) but treat it as ~15% of a real connector. It has no `CDCPullConnector`/`QRepPullConnector` implementation, no changefeed management, no type mapping, no offset handling. Recommend: don't merge as-is; fold its useful parts into Phase 1 below (and note its TLS-off default should be revisited — changefeed webhook sinks are HTTPS-only anyway).

### Core architecture: push→pull bridge

CockroachDB has no PG logical replication; changefeeds are the only CDC egress, and they **push**. PeerDB's CDC contract is a blocking **pull** (`PullRecords` inside a Temporal activity). Bridge with three pieces:

1. **Webhook receiver** — a new HTTPS listener (new port, e.g. 8114) hosted in the flow-api process (or a new lightweight `flow-ingest` service for isolation; start in flow-api, split later). Route: `POST /webhooks/cdc/crdb/v1/{flow_job_name}` with a per-mirror bearer token checked against `webhook_auth_header`. Handler: parse `{"payload":[...],"length":N}`, validate, durably write to the buffer, **only then** return 200. Any failure → 5xx (CRDB retries per `Retry` config; at-least-once).
2. **Durable buffer** — catalog Postgres table, e.g. `crdb_changefeed_events(flow_name, kind smallint /*row|resolved*/, topic text, key jsonb, hlc numeric/text, payload jsonb, id bigserial)`, insert with `ON CONFLICT (flow_name, topic, key, hlc) DO NOTHING` for dedup of redeliveries. Trimmed after each synced batch checkpoint. (Catalog PG throughput is the known ceiling — see Risks; schema allows swapping to object-store segments later without changing the connector contract.)
3. **`PullRecords` drains the buffer** — the CRDB connector's PullRecords polls/`LISTEN-NOTIFY`s the buffer, converts JSON rows to `RecordItems` (Insert/Update/Delete via envelope), streams into `CDCStream`, and calls `UpdateLatestCheckpointText(<resolved HLC>)` **only when a resolved message is consumed** (per-row HLCs are not safe cursors across keys). Batch ends on MaxBatchSize/IdleTimeout as usual. `CdcCheckpoint.Text` = last resolved HLC.

CRDB's changefeed **job** is itself durable and checkpointed: while PeerDB is down, CRDB retries webhook delivery and holds a protected timestamp, so the buffer+job covers most restarts without cursor games. The persisted resolved HLC is the recovery cursor only when the changefeed job must be recreated (`CREATE CHANGEFEED ... WITH cursor='<hlc>', initial_scan='no'`).

### Changefeed lifecycle (owned by the connector)

- `SetupReplication` (runs before snapshot): capture `SELECT cluster_logical_timestamp()` as t₀ → store as initial checkpoint; `CREATE CHANGEFEED FOR TABLE <tables> INTO 'webhook-https://<peerdb_ingest_url>/...' WITH cursor='t₀', initial_scan='no', resolved='10s', min_checkpoint_frequency='10s', diff, updated, mvcc_timestamp, webhook_auth_header='...', webhook_sink_config='{"Flush":{"Messages":500,"Frequency":"1s"}}'` (+ `envelope='enriched', enriched_properties='source'` on CRDB ≥25.2). Store the job ID in catalog.
- Envelope handling: enriched → `op` c/u/d directly; wrapped+diff fallback → `before==null`→insert, `after==null`→delete, else update.
- `PullFlowCleanup` → `CANCEL JOB`; mirror pause/resume → `PAUSE/RESUME JOB`; table add/remove mid-flow → `ALTER CHANGEFEED ... ADD/DROP TABLE` (pause-first), new tables snapshotted via the existing child-mirror `InitialSnapshotOnly` path.
- Schema changes: `schema_change_policy='nobackfill'`; detect added columns by diffing JSON keys against the catalog schema → emit `TableSchemaDelta{AddedColumns}` (MySQL pattern). Drops/renames logged, not propagated (parity with MySQL).

### Snapshot strategy

Primary: **PeerDB-native parallel snapshot via pgwire with `AS OF SYSTEM TIME t₀`** — implement `QRepPullConnector` reusing Postgres-style range partitioning (numeric/UUID PK min/max via pgwire; CRDB speaks pg SQL) with every SELECT suffixed `AS OF SYSTEM TIME 't₀'`. Because the changefeed was created with `cursor=t₀` first, its protected timestamp should pin MVCC history at t₀ so long snapshots don't lose to GC (verify; else bump `gc.ttlseconds` for the duration or take an explicit protected timestamp). This gives exact snapshot↔CDC consistency — cleaner than MySQL's GTID capture and keeps ClickHouse initial load on the fast QRep path.

Fallback/simple mode: `initial_scan='yes'` and let the backfill flow through the webhook as inserts (no QRep needed, but slow, unpartitioned, hammers the buffer) — useful for tiny tables/testing only.

### Type mapping

- Schema (`GetTableSchema`): pgwire introspection; reuse `PostgresOIDToQValueKind` with a CRDB overlay (CRDB-specific types/OIDs, no PG arrays of arrays, INTERVAL/DECIMAL nuances).
- CDC decode: JSON→QValue keyed by the column's QValueKind: DECIMAL from string, BYTES from base64, TIMESTAMP(TZ) from strings, JSONB passthrough (`encode_json_value_null_as_object` to disambiguate SQL NULL vs JSON null).
- Snapshot decode: native pgx values — the Postgres QRep value path largely reuses.

### New peer config (`CockroachConfig` in protos/peers.proto)

PostgresConfig fields (host/port/user/password/database/TLS) **plus**: `ingest_base_url` (externally reachable HTTPS URL of the PeerDB receiver), `webhook_auth_token`, `resolved_interval`, `envelope` (auto|enriched|wrapped), `flush_messages/frequency`, optional `changefeed_extra_options`. Validation (`ValidateCheck`/`ValidateMirrorSource`): version ≥ supported, `kv.rangefeed.enabled=true`, license present, `CHANGEFEED` privilege, all mirrored tables have PKs, ingest URL reachability probe (self-call).

### Phasing

1. **Peer + validation** (salvage PR #3861): proto/DBType, connector skeleton w/ `PostgresMetadata` embed, schema introspection, validation checks, UI, nexus. CRDB→ClickHouse only.
2. **Snapshot-only mirrors**: QRepPullConnector with AOST range-partitioned reads → initial loads work end-to-end.
3. **CDC**: webhook receiver + buffer tables + catalog migration; PullRecords drain; changefeed lifecycle mgmt; checkpoint on resolved.
4. **Hardening**: schema deltas, add/remove tables, pause/resume/resync, heartbeat-lag metrics (resolved-HLC age as replication lag), e2e CI (single-node CRDB container; webhook needs HTTPS → self-signed cert + `ca_cert`/`insecure_tls_skip_verify` in tests), buffer GC job.

### Serious alternative worth deciding on first: sinkless changefeed (pure pull, no new infra)

`CREATE CHANGEFEED FOR TABLE ... WITH cursor=...` **without** a sink streams changes as rows over a plain pgwire session — a blocking read loop that fits `PullRecords` *exactly* like MySQL binlog/Mongo change streams: no HTTP listener, no buffer, no ingress/URL config, connector owns the connection, restart from last resolved HLC via `cursor`. Costs: not a managed job (no server-side checkpoint/protected-timestamp autonomy beyond session), one session per mirror, historically weaker guarantees on very large table counts. If the webhook's operational burden (public HTTPS endpoint per deployment, buffer throughput) is unattractive, sinkless is the idiomatic-PeerDB v1 and the webhook sink can be a v2 for managed-job robustness. Recommend prototyping sinkless first unless an externally reachable ingest endpoint is already a given in target deployments.

## Implementation plan — sinkless-first, swarm-decomposed

Decision (from discussion): v1 = sinkless changefeed over pgwire (pure pull, fits `PullRecords` like MySQL binlog; downtime bounded by `gc.ttlseconds`, same trade as MySQL binlog retention). Webhook sink = v2 behind the same connector/checkpoint format.

### Contracts fixed up front (before fan-out)

- **Checkpoint**: `CdcCheckpoint.Text` = last consumed resolved HLC as CRDB decimal string (`<nanos>.<logical>`); `ID` unused. Restart = `CREATE CHANGEFEED ... WITH cursor='<text>', initial_scan='no'`.
- **Snapshot consistency**: `SetupReplication` captures `cluster_logical_timestamp()` → t₀ stored as initial checkpoint; all QRep reads `AS OF SYSTEM TIME 't₀'`.
- **Decoder interfaces** (package `flow/connectors/cockroach/decode`): `PgwireValueToQValue(fd FieldDescription, v any) (qvalue.QValue, error)` and `ChangefeedJSONToRecordItems(schema *protos.TableSchema, before, after map[string]json.RawMessage) (...)`.
- **Envelope**: wrapped+`diff`+`updated`+`resolved` baseline (insert: before==null; delete: after==null; else update); enriched auto-detected on ≥25.2.
- **Connector struct**: `CockroachConnector{ *metadataStore.PostgresMetadata; conn *pgx.Conn; config *protos.CockroachConfig; ... }`.

### Phase 0 — Foundation (serial, 1 agent, blocks all)
Owns ALL shared files; no other WP touches them afterward.
- `protos/peers.proto`: `DBType COCKROACH`, `CockroachConfig` (PG-like conn fields + `resolved_interval`, `envelope`, `changefeed_extra_options`; NO ingest URL for v1), `Peer.config` oneof; regenerate (flow/generated, nexus/pt, ui/grpc_generated).
- `flow/connectors/core.go`: `BuildPeerConfig` + `getConnector` cases, compile-time assertions.
- Skeleton `flow/connectors/cockroach/` (struct, constructor, Close/ConnectionActive, PostgresMetadata embed) with all interface methods stubbed `ErrUnsupported`.
- Salvage PR #3861 schema-introspection SQL where correct.
Gate: repo compiles, peer create/validate via API works against local CRDB.

### Phase 1 — Parallel work packages (6 agents)
- **WP-A Validation+introspection** (`cockroach/validate.go`, `schema.go`, `client.go`): GetTableSchema/GetSchema/GetTables/GetColumns via pgwire; ValidateCheck (version, `kv.rangefeed.enabled`, license, `CHANGEFEED` privilege); ValidateMirrorSource (tables exist + have PK, `SourceTablesMissingError`); GetVersion/DatabaseVariant/TableSizeEstimator.
- **WP-B Decoders** (`cockroach/decode/`): pure library, table-driven tests. PG-OID→QValueKind overlay (reuse `postgres/qvalue_convert.go`); pgwire→QValue for snapshot; changefeed-JSON→QValue (DECIMAL string, BYTES base64, TIMESTAMP strings, JSONB, SQL-NULL vs JSON-null); envelope parsing (wrapped/enriched) → op + before/after; resolved-message detection; HLC parse/compare.
- **WP-C Snapshot/QRep** (`cockroach/qrep.go`): GetDefaultPartitionKeyForTables (PK-based), GetQRepPartitions (min/max/count range partitioning, numeric/UUID/string PK — reuse patterns from `postgres/qrep_partition` + #4522 string partitioning), PullQRepRecords with AOST t₀ → QRecordStream. Dep: WP-B pgwire decoder (mock until ready).
- **WP-D CDC PullRecords** (`cockroach/cdc.go`): SetupReplication (capture t₀, verify prereqs); SetupReplConn (dedicated non-pooled pgx conn, results buffering off); PullRecords: issue sinkless `CREATE CHANGEFEED FOR <tables> WITH cursor, resolved='10s', min_checkpoint_frequency, diff, updated, envelope=...`, blocking row loop → CDCStream, `UpdateLatestCheckpointText` ONLY on resolved rows, MaxBatchSize/IdleTimeout exits, reconnect-with-backoff inside pull (MySQL pattern), PullFlowCleanup. Dep: WP-B JSON decoder.
- **WP-E Test infra** (`docker-compose-dev.yml`, Tiltfile, `flow/e2e/cockroach/`): single-node CRDB service (`--insecure` fine for sinkless), e2e suite scaffold cloned from mongo/mysql e2e, seed helpers, CI wiring incl. test-naming convention (#4518) and non-exercised-test check (#4533).
- **WP-F UI+nexus** (salvage #3861: `ui/`, `nexus/analyzer`, `nexus/catalog`): peer form (drop the SSH-less/TLS-off choices as needed), handlers, logo, nexus peer parsing.
Gate: unit tests green per WP; WP-B is the only cross-WP dependency (stub/mock until it lands).

### Phase 2 — Integration + e2e (1-2 agents, serial gate)
- Wire EnsurePullability (relid mapping via pgwire OIDs), ExportTxSnapshot/FinishExport no-ops, snapshot_flow → QRep → CDC handoff at t₀.
- E2E CRDB→ClickHouse: initial load; insert/update/delete propagation; delete with `diff`; TOAST-equivalent wide rows; all core types round-trip.
- Chaos tests: kill repl conn mid-stream (cursor resume, no loss, duplicate-tolerant), worker restart mid-batch, resync path, pause/resume mirror.

### Phase 3 — Hardening (parallel again)
- Schema deltas: added-column detection (JSON keys vs catalog schema) → `TableSchemaDelta{AddedColumns}` + `ReplayTableSchemaDeltas`; `schema_change_policy` handling.
- Add/remove tables mid-mirror: restart sinkless query with new table set at current cursor + `InitialSnapshotOnly` child mirror for new tables.
- Observability: replication-lag metric from resolved-HLC age; checkpoint-age vs `gc.ttlseconds` alert; error classification (GC-threshold error → needs-resync signal, like Maria #4530 pattern).
- Docs: peerdb-architecture.md connector matrix, deep-dive section, gc.ttlseconds guidance (recommend ≥24h on mirrored tables).

### Phase 4 — v2: webhook sink mode (only if managed-job robustness needed)
- HTTPS receiver (flow-api or new flow-ingest), catalog buffer table + migration, ON CONFLICT dedup, PullRecords buffer-drain mode, changefeed job lifecycle (CREATE INTO webhook-https / PAUSE / RESUME / CANCEL / ALTER), config additions (ingest_base_url, auth token). Reuses WP-B decoders + identical checkpoint format. Pre-work: verify resolved-message JSON + HTTP ack contract in `changefeedccl` source.

### Risks / open questions

- Exact webhook **resolved-message JSON** and **HTTP status-code ack contract** are under-documented — verify against `pkg/ccl/changefeedccl/sink_webhook.go` before freezing the receiver.
- Catalog-PG buffer throughput ceiling for high-write mirrors; mitigation: batch inserts, partitioned buffer table, later object-store segments.
- PeerDB must expose a public HTTPS URL to the CRDB cluster (ingress/TLS certs per deployment) — a real ops burden for self-hosted users; N/A for sinkless.
- Changefeed limits: ~80 feeds/cluster guidance → one multi-table feed per mirror, not per table.
- Long consumer downtime beyond `gc_protect_expires_after`/GC TTL ⇒ changefeed unrecoverable ⇒ auto-resync path needed. **Mitigated for the initial-snapshot window (implemented):** `crdb_internal.protect_mvcc_history` pins history at t₀ (auto-expiring, extended during QRep pulls, released on first resolved checkpoint), so snapshots longer than `gc.ttlseconds` no longer break AOST reads or the changefeed cursor. Steady-state CDC downtime remains GC-TTL-bounded (ratcheting protection = future work).
- Licensing: customer clusters need an (Enterprise/Enterprise Free) license for sink-backed feeds; sinkless historically ran license-free pre-24.3 — post-24.3 everything requires a (possibly free) license.
