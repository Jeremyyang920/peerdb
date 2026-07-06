# CockroachDB source connector — test infrastructure (WP-E)

This directory holds the CI-wiring plan for the CockroachDB source connector.

> **Status (updated):** the connector, e2e suite, and CI job now exist. The e2e
> tests live in the `e2e` package as `flow/e2e/cockroach.go` (source helper) and
> `flow/e2e/cockroach_test.go` (`TestGenericCH_Cockroach` + `TestCockroachClickhouseSuite`);
> the CI job runs a CockroachDB service in `.github/workflows/flow.yml`. The
> "Why no Go tests / CI job yet" section below is retained as historical rationale.
> Tests run green against **v26.1.6** (the currently pinned version); the captured
> changefeed samples further down were recorded on v25.4.12 — see the note there.

The v1 connector uses **sinkless (core) changefeeds over pgwire** — a blocking
`CREATE CHANGEFEED FOR TABLE ... WITH resolved=...` (no `INTO` sink) streamed over a
normal SQL session, consumed like the MySQL binlog / Mongo change stream. This means
the test infra needs nothing more than a single CockroachDB node reachable over pgwire;
no HTTPS ingress, no webhook receiver, no extra buffer.

## What already exists (this work package)

| Piece | Location |
|-------|----------|
| Single-node CRDB service | `cockroach` service in `ancillary-docker-compose.yml` |
| Image version pin | `COCKROACH_VERSION` (default `v26.1.6`) → `COCKROACH_IMAGE` in `generate-test-environment.sh` |
| Connection env vars | `CI_COCKROACH_HOST/PORT/USER/DATABASE` in `.env.example` |
| Rangefeed provisioning | `local_provision_scripts/cockroach.sh` |
| Tilt resources | `dc_resource('cockroach')` + `provision-cockroach` in `Tiltfile` (label `Ancillary-DB`, `auto_init=False`) |

### Local dev usage (Tilt)

```bash
tilt --port 10352 enable cockroach
tilt --port 10352 wait --for=condition=Ready uiresource/cockroach --timeout=120s
tilt --port 10352 wait --for=condition=Ready uiresource/provision-cockroach --timeout=120s
```

`provision-cockroach` runs `SET CLUSTER SETTING kv.rangefeed.enabled = true` (required
for sinkless changefeeds). The node is insecure single-node, SQL on host port
**26257**, user `root`, default database `defaultdb`.

Or without Tilt:

```bash
docker compose --env-file ancillary.env -f ancillary-docker-compose.yml up -d cockroach
./local_provision_scripts/cockroach.sh
```

## Why no Go tests / CI job yet

`.github/workflows/flow.yml` has a final step **"Check all present test functions in the
source branch were exercised in CI"** (PR #4533). It compares every top-level `Test*`
function found by `go test -list ./...` against the tests that actually ran in CI, and
**fails the build if the branch defines a test that CI never ran**.

Consequences for this connector:

- Do **not** add a `*_test.go` with a new `TestCockroach...` function until the CI job
  that runs it exists in the same PR. A test file without a matching CI runner fails the
  check for everyone.
- The Go e2e scaffold also depends on Phase 0 (the `COCKROACH` proto `DBType` +
  `CockroachConfig` + `getConnector` wiring) and on a `CockroachSource` e2e helper, none
  of which exist yet.

So this WP stops at infrastructure. The steps below are the exact future wiring.

## Future CI wiring (do this when the connector lands)

### 1. Suite naming convention (PR #4518)

E2E suites in `flow/e2e/` are top-level `Test*` functions that call
`e2eshared.RunSuite`. The established source-to-ClickHouse names are `TestGenericCH_PG`,
`TestGenericCH_MySQL`, `TestGenericCH_MariaDB`, `TestMongoClickhouseSuite`,
`TestSwitchboardMongo`, `TestApiMongo`. Mirror them for CockroachDB:

| Purpose | Test function | Model after |
|---------|---------------|-------------|
| Generic type/CDC round-trip → ClickHouse | `TestGenericCH_Cockroach` | `TestGenericCH_MySQL` in `generic_test.go` |
| Dedicated suite (initial load + CDC insert/update/delete) | `TestCockroachClickhouseSuite` | `TestMongoClickhouseSuite` in `mongo_test.go` |
| Switchboard | `TestSwitchboardCockroach` | `TestSwitchboardMongo` |
| API | `TestApiCockroach` | `TestApiMongo` |

Each new top-level `Test*` MUST be added to the CI run set (below) in the **same PR** to
satisfy the non-exercised-test check.

The generic suite needs a `CockroachSource` e2e helper + `SetupCockroach(t, suffix)`
(clone `MySqlSource`/`MongoSource`); then:

```go
func TestGenericCH_Cockroach(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, false,
		func(t *testing.T) (*CockroachSource, string, error) {
			t.Helper()
			suffix := "crchg_" + strings.ToLower(common.RandomString(8))
			source, err := SetupCockroach(t, suffix)
			return source, suffix, err
		})))
}
```

### 2. Service setup in `.github/workflows/flow.yml`

CI starts source DBs with `docker run` inline (see the "MySQL"/"Mongo" steps), not from
`ancillary-docker-compose.yml`. Add an analogous step. Because `--insecure` cannot set a
cluster setting via container args, enable rangefeed after startup (mirrors the Mongo
`rs.initiate` step):

```yaml
      - name: CockroachDB
        run: |
          docker run -d --rm --name cockroach -p 26257:26257 \
            cockroachdb/cockroach:v26.1.6 start-single-node --insecure
          until docker exec cockroach cockroach sql --insecure -e "SELECT 1" &>/dev/null; do
            echo "waiting for CockroachDB..."; sleep 2
          done
          docker exec cockroach cockroach sql --insecure \
            -e "SET CLUSTER SETTING kv.rangefeed.enabled = true;"
```

Add the connection env vars to the `run tests` step `env:` block (alongside
`CI_MYSQL_*`, `CI_MONGO_*`):

```yaml
          CI_COCKROACH_HOST: localhost
          CI_COCKROACH_PORT: 26257
          CI_COCKROACH_USER: root
          CI_COCKROACH_DATABASE: defaultdb
```

The whole test binary runs in one `gotestsum ... ./...` invocation, so once the source
DB is up and the connector code exists, the new `TestCockroach*` / `TestGenericCH_Cockroach`
functions are picked up automatically — no per-suite `-run` entry is needed in CI (that
matrixing is a Tilt-only convenience). This is also what satisfies the non-exercised-test
check: the tests exist AND run in the same job.

Consider adding CockroachDB to the `db-version` matrix (a `crdb:` key) only if multiple
CRDB versions need coverage; a single pinned version in the step above is sufficient to
start.

### 3. Tilt test launcher (optional, local convenience)

When `TestGenericCH_Cockroach` exists, add to the `Tiltfile` alongside the other
`e2e_test(...)` calls:

```python
e2e_test('cockroach', 'TestGenericCH_Cockroach', ['provision-cockroach'])
```

and a `connector_test('cockroach', ['provision-cockroach'])` once
`flow/connectors/cockroach/*_test.go` exists.

### 4. Seed SQL example (what an e2e SetupCockroach should run)

```sql
-- one-time cluster prerequisite (done by provision-cockroach / the CI step above)
SET CLUSTER SETTING kv.rangefeed.enabled = true;

-- per-test schema: every mirrored table MUST have a primary key
CREATE DATABASE IF NOT EXISTS e2e_test;
CREATE TABLE e2e_test.orders (
    id     INT PRIMARY KEY,
    sku    STRING,
    price  DECIMAL,
    ts     TIMESTAMPTZ DEFAULT now()
);

-- initial-load rows (snapshot via AS OF SYSTEM TIME), then CDC rows post-changefeed
INSERT INTO e2e_test.orders (id, sku, price) VALUES (1, 'ABC', 19.99);
```

The connector captures `cluster_logical_timestamp()` as t0 before snapshot, reads the
snapshot `AS OF SYSTEM TIME t0`, then starts the sinkless changefeed at `cursor=t0`.

## Observed changefeed message shapes (captured on v25.4.12)

> **Version note:** the samples below are v25.4.12-era ground truth. The suite now
> runs against **v26.1.6**; the full e2e round-trip (wrapped envelope, `diff`,
> `updated`, `mvcc_timestamp`, resolved-HLC checkpoints, DECIMAL-as-JSON-number)
> was re-verified green on v26.1.6 with no observed wire-format drift.

Captured from a real sinkless changefeed on the compose service (insecure single node).
DML: `INSERT (1,'alice',10.50)` → `UPDATE amount=99.99` → `DELETE`, then a second table
with the enriched envelope. `cockroach sql --format=csv` hex-encodes the JSON; values
below are decoded.

### Wrapped envelope — `envelope='wrapped', diff, updated, mvcc_timestamp`

The default v1 baseline. Key column carries the PK as a JSON array; resolved messages
arrive on their own row with `table=NULL, key=NULL`.

```
key      = [1]
resolved = {"resolved":"1783372607043326006.0000000000"}

insert   = {"after": {"amount": 10.50, "id": 1, "name": "alice"}, "before": null,
            "mvcc_timestamp": "1783372618230329595.0000000000",
            "updated": "1783372618230329595.0000000000"}

update   = {"after": {"amount": 99.99, "id": 1, "name": "alice"},
            "before": {"amount": 10.50, "id": 1, "name": "alice"},
            "mvcc_timestamp": "...", "updated": "..."}

delete   = {"after": null,
            "before": {"amount": 99.99, "id": 1, "name": "alice"},
            "mvcc_timestamp": "...", "updated": "..."}
```

Op inference (wrapped): `before==null` → insert; `after==null` → delete; else update.

### Enriched envelope — `envelope='enriched', enriched_properties='source,schema', diff, updated`

Debezium-compatible; available v25.2+. Carries an explicit `op` (`c`/`u`/`d`), a `source`
block, and (with `schema` requested) a large `schema` block. `payload` wraps the row.

```
key (payload) = {"id": 2}   (also carries a schema block)

insert (op=c):
  payload.before = null
  payload.after  = {"amount": 42.0, "id": 2, "name": "bob"}
  payload.op     = "c"
  payload.ts_ns  = 1783372650971924221
  payload.source = {"ts_hlc":"1783372650063855845.0000000000",
                    "ts_ns":1783372650063855845, "table_name":"t",
                    "database_name":"testdb", "primary_keys":["id"],
                    "origin":"cockroachdb", "cluster_id":"ea5cae6e-...",
                    "changefeed_sink":"sinkless buffer", "db_version":"v25.4.12", ...}

update (op=u): before = old row, after = new row
delete (op=d): before = old row, after = null
resolved     : {"resolved":"1783372675000000000.0000000000"} (same NULL/NULL row form)
```

### Notes for the decoder team (WP-B)

- **HLC checkpoint format:** resolved value is a CRDB decimal string
  `"<nanos>.<logical>"` (e.g. `1783372607043326006.0000000000`) — store verbatim in
  `CdcCheckpoint.Text`; restart via `CREATE CHANGEFEED ... WITH cursor='<text>',
  initial_scan='no'`. `updated` / `mvcc_timestamp` (wrapped) and `source.ts_hlc`
  (enriched) use the same format. Only advance the checkpoint on **resolved** rows.
- **DECIMAL arrives as a JSON number, not a string.** Observed `10.50`, `99.99`, `42.0`
  (enriched schema even labels it `type: float64, name: decimal`). This contradicts the
  plan's "DECIMAL from string" assumption. JSON-number decoding risks precision loss for
  wide decimals — WP-B should decode DECIMAL from `json.Number`/raw token (not float64),
  and consider `format=json` + `WITH ...` options if a string encoding is later needed
  for high-precision columns. **Flag/verify per-type.**
- **Resolved messages** have `table` and `key` both `NULL` in the row stream — detect by
  the top-level `{"resolved": ...}` object (wrapped) / `payload` absence.
- **Key** is a JSON array of PK column values (wrapped), or `payload` object (enriched).
- **Licensing:** sinkless changefeeds ran with **no enterprise license** on an insecure
  single node (v25.4.12) — no `SET CLUSTER SETTING cluster.organization/enterprise.license`
  was needed. Customer clusters may still require a (possibly free) license; validate
  against target deployments.
- **GC / `gc.ttlseconds`:** sinkless downtime is bounded by the table's GC TTL (like
  MySQL binlog retention). Tests that exercise resume-from-cursor should keep runs short
  or bump `gc.ttlseconds` on the mirrored tables.
