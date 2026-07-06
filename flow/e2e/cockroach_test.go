package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// TestGenericCH_Cockroach runs the shared generic source→ClickHouse suite against
// CockroachDB, inheriting the standard simple-flow / schema-change scenarios. The
// Postgres-only scenarios in that suite self-skip on a non-Postgres source.
func TestGenericCH_Cockroach(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, false, func(t *testing.T) (*CockroachSource, string, error) {
		t.Helper()
		suffix := "crchg_" + strings.ToLower(common.RandomString(8))
		source, err := SetupCockroach(t, suffix)
		return source, suffix, err
	})))
}

// skipCockroachNoSchemaDelta skips a shared generic schema-change scenario when
// the source is CockroachDB. Schema-delta propagation (added/dropped columns
// reaching the destination mid-stream) is Phase 3 and not implemented yet, and
// CRDB's SERIAL primary keys surface as Int64 rather than the Int32 those tests
// assume. CockroachDB's supported ADD COLUMN behavior (mirror survives; new-column
// values dropped until Phase 3) is covered by
// TestCockroachClickhouseSuite/Test_Add_Column_Mid_Stream.
func skipCockroachNoSchemaDelta(s Generic) {
	if _, ok := s.Source().(*CockroachSource); ok {
		s.T().Skip("CockroachDB source: schema-delta propagation is Phase 3 " +
			"(supported ADD COLUMN behavior is covered by TestCockroachClickhouseSuite/Test_Add_Column_Mid_Stream)")
	}
}

// --- Dedicated CockroachDB → ClickHouse suite -------------------------------

type CockroachClickhouseSuite struct {
	GenericSuite
}

func TestCockroachClickhouseSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupCockroachClickhouseSuite)
}

func SetupCockroachClickhouseSuite(t *testing.T) CockroachClickhouseSuite {
	t.Helper()
	return CockroachClickhouseSuite{SetupClickHouseSuite(t, false, func(t *testing.T) (*CockroachSource, string, error) {
		t.Helper()
		suffix := "crch_" + strings.ToLower(common.RandomString(8))
		source, err := SetupCockroach(t, suffix)
		return source, suffix, err
	})(t)}
}

func (s CockroachClickhouseSuite) source() *CockroachSource {
	return s.Source().(*CockroachSource)
}

// crdbCDCConfig builds a standard CDC mirror config with an initial snapshot for
// the given src→dst table pairs.
func (s CockroachClickhouseSuite) crdbCDCConfig(flowName string, tables ...string) *protos.FlowConnectionConfigs {
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, flowName),
		TableMappings: TableMappings(s, tables...),
		Destination:   s.Peer().Name,
	}
	cfg := connectionGen.GenerateFlowConnectionConfigs(s)
	cfg.DoInitialSnapshot = true
	return cfg
}

// Test_Snapshot_CDC_Consistency is the core correctness claim: a mirror with an
// initial snapshot on a large table, with concurrent inserts/updates/deletes
// applied WHILE the snapshot runs, must converge to exactly the source's final
// state — no rows lost in the t0 snapshot↔changefeed handoff, no phantom rows.
//
// The snapshot reads AS OF SYSTEM TIME t0 (captured in SetupReplication) and the
// changefeed resumes at cursor=t0, so every mutation is covered by exactly one of
// the two paths (or both, idempotently, on the ReplacingMergeTree destination).
func (s CockroachClickhouseSuite) Test_Snapshot_CDC_Consistency() {
	t := s.T()
	srcTable := "consistency"
	dstTable := "consistency_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT NOT NULL, n INT NOT NULL)`, src)))

	// Seed 50k rows so the snapshot has real duration and spans several partitions.
	const seedRows = 50000
	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, val, n) SELECT g, 'seed_'||g::STRING, g FROM generate_series(1, %d) g`,
		src, seedRows)))

	// A row that exists before mirror creation and is updated DURING the snapshot;
	// it must land at its final value.
	require.NoError(t, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val, n) VALUES (%d, 'pre_mirror', 0)`, src, seedRows+1)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	cfg.SnapshotNumRowsPerPartition = 5000

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)

	// Apply mutations immediately so they overlap the worker's snapshot:
	//  - update the pre-mirror row to its final value
	//  - delete a band of seeded rows
	//  - update another band of seeded rows
	//  - insert brand-new rows past the seed range
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`UPDATE %s SET val = 'pre_mirror_final', n = 999 WHERE id = %d`, src, seedRows+1)))
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`DELETE FROM %s WHERE id BETWEEN 1 AND 5000`, src)))
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`UPDATE %s SET val = 'updated', n = n + 1000000 WHERE id BETWEEN 10001 AND 15000`, src)))
	EnvNoError(t, env, s.source().Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, val, n) SELECT g, 'cdc_'||g::STRING, g FROM generate_series(%d, %d) g`,
		src, seedRows+100, seedRows+2100)))

	// The destination must converge to the exact source state across all columns.
	EnvWaitForEqualTablesWithNames(env, s, "snapshot+cdc converge", srcTable, dstTable, "id,val,n")

	// Explicitly assert the pre-mirror row ended at its updated value.
	dstRows, err := s.GetRows(dstTable, "id,val,n")
	EnvNoError(t, env, err)
	var found bool
	for _, r := range dstRows.Records {
		if r[0].Value().(int64) == int64(seedRows+1) {
			found = true
			require.Equal(t, "pre_mirror_final", r[1].Value())
		}
	}
	EnvTrue(t, env, found)

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_CDC_Insert_Update_Delete covers the three basic operations propagating,
// including a delete (which carries a before-image via `diff` and is filtered out
// of the ClickHouse destination via _peerdb_is_deleted).
func (s CockroachClickhouseSuite) Test_CDC_Insert_Update_Delete() {
	t := s.T()
	srcTable := "iud"
	dstTable := "iud_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT, amount DECIMAL(10,2))`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, name, amount) VALUES (1, 'alice', 10.50)`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "insert", srcTable, dstTable, "id,name,amount")

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`UPDATE %s SET amount = 99.99 WHERE id = 1`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "update", srcTable, dstTable, "id,name,amount")

	EnvNoError(t, env, s.source().Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "delete", srcTable, dstTable, "id,name,amount")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_CDC_Update_Preserves_Unchanged_Columns updates a single non-key column and
// asserts the other columns keep their prior values on the destination (the
// changefeed `after` image carries the full row, so no column should be lost).
func (s CockroachClickhouseSuite) Test_CDC_Update_Preserves_Unchanged_Columns() {
	t := s.T()
	srcTable := "update_preserve"
	dstTable := "update_preserve_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, a TEXT, b TEXT, c INT)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, a, b, c) VALUES (1, 'keep_a', 'keep_b', 42)`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "insert", srcTable, dstTable, "id,a,b,c")

	// Change only column b; a and c must be preserved.
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`UPDATE %s SET b = 'new_b' WHERE id = 1`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "partial update", srcTable, dstTable, "id,a,b,c")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_CDC_Multi_Row_Transaction commits many rows across tables in a single
// transaction and asserts all rows arrive (changefeeds emit per-row messages; the
// resolved checkpoint after the commit covers the whole transaction).
func (s CockroachClickhouseSuite) Test_CDC_Multi_Row_Transaction() {
	t := s.T()
	srcTable := "multirow"
	dstTable := "multirow_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	EnvNoError(t, env, s.source().Exec(t.Context(), fmt.Sprintf(
		`BEGIN;
		 INSERT INTO %[1]s (id, val) SELECT g, 'tx_'||g::STRING FROM generate_series(1, 500) g;
		 COMMIT;`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "500-row txn", srcTable, dstTable, "id,val")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_CDC_Same_Key_Multiple_Times_In_Txn touches one key insert→update→delete
// within a single transaction. CockroachDB changefeeds emit only the FINAL state
// per key per transaction — here the key is deleted, so it must NOT appear on the
// destination. A second key inserted→updated (final = updated) in the same txn
// must appear at its final value. This documents the intermediate-state semantics:
// intermediate versions within a txn are not observable downstream.
func (s CockroachClickhouseSuite) Test_CDC_Same_Key_Multiple_Times_In_Txn() {
	t := s.T()
	srcTable := "samekey"
	dstTable := "samekey_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	// Seed a key that we'll leave alone so the tables aren't empty.
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (100, 'stable')`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "seed", srcTable, dstTable, "id,val")

	// id=1: insert→update→delete in one txn (final: gone).
	// id=2: insert→update in one txn (final: 'v2_final').
	EnvNoError(t, env, s.source().Exec(t.Context(), fmt.Sprintf(
		`BEGIN;
		 INSERT INTO %[1]s (id, val) VALUES (1, 'v1_a');
		 UPDATE %[1]s SET val = 'v1_b' WHERE id = 1;
		 DELETE FROM %[1]s WHERE id = 1;
		 INSERT INTO %[1]s (id, val) VALUES (2, 'v2_a');
		 UPDATE %[1]s SET val = 'v2_final' WHERE id = 2;
		 COMMIT;`, src)))

	// Convergence to source state: id=1 absent, id=2='v2_final', id=100='stable'.
	EnvWaitForEqualTablesWithNames(env, s, "same-key txn converge", srcTable, dstTable, "id,val")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_CDC_All_Nulls inserts a row with NULL in every nullable column and asserts
// the NULLs round-trip through the changefeed and normalization.
//
// NULLs only survive to ClickHouse when nullable columns are enabled
// (PEERDB_NULLABLE); otherwise PeerDB materializes non-nullable columns and NULL
// collapses to the type default (0/''/epoch), which is expected PeerDB behavior.
// JSONB is intentionally excluded here: with PEERDB_NULLABLE the JSON column would
// be Nullable(JSON), a type the e2e ClickHouse row-reader
// (connclickhouse.GetTableSchemaForTable) cannot resolve — a shared-helper gap,
// not CRDB-specific. JSONB round-trip is covered (non-null) in Test_Type_Matrix.
func (s CockroachClickhouseSuite) Test_CDC_All_Nulls() {
	t := s.T()
	srcTable := "allnulls"
	dstTable := "allnulls_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (
			id INT PRIMARY KEY,
			s TEXT, n INT, d DECIMAL, f FLOAT8, b BOOL,
			ts TIMESTAMP, tz TIMESTAMPTZ, dt DATE
		)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	cfg.Env = map[string]string{"PEERDB_NULLABLE": "true"}
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id) VALUES (1)`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "all-null cdc row",
		srcTable, dstTable, "id,s,n,d,f,b,ts,tz,dt")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_Type_Matrix round-trips CockroachDB's type spectrum through both the
// snapshot (AS OF SYSTEM TIME, pgx-native decode) and CDC (changefeed JSON decode)
// paths. id=1 is seeded before the mirror (snapshot path); id=2 carries identical
// values inserted after the mirror starts (CDC path). Columns are compared in
// groups so any type that does not round-trip is isolated and characterized rather
// than hidden behind a monolithic failure.
//
// JSON is enabled (PEERDB_CLICKHOUSE_ENABLE_JSON) so JSONB maps to a ClickHouse
// JSON column. Nullable is deliberately NOT enabled here (NULL round-trip is
// covered by Test_CDC_All_Nulls): with nullable, JSONB would become Nullable(JSON),
// which the e2e ClickHouse row-reader cannot resolve.
func (s CockroachClickhouseSuite) Test_Type_Matrix() {
	t := s.T()
	srcTable := "types_matrix"
	dstTable := "types_matrix_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id INT PRIMARY KEY,
		i2 INT2, i4 INT4, i8 INT8, big_i8 INT8,
		dec_hi DECIMAL(38,9), dec_neg DECIMAL(38,9), dec_tz DECIMAL(38,9),
		f8 FLOAT8, f8_negzero FLOAT8, f8_bigexp FLOAT8,
		b BOOL, s STRING, s_big STRING, by BYTES, u UUID,
		d DATE, tm TIME, ts TIMESTAMP, tstz TIMESTAMPTZ, iv INTERVAL,
		j JSONB, j_null JSONB, arr_s STRING[], arr_i8 INT8[]
	)`, src)))

	// One reusable VALUES tuple (parameterized by id) covering edge values:
	// INT8 beyond 2^53, high-precision/negative/trailing-zero decimals, -0.0 and
	// large-exponent floats, unicode+emoji and a 1MB string, bytes with 0x00/0xff,
	// microsecond timestamps, a pre-1970 timestamptz, an interval, nested JSONB
	// (incl. a null array element, which is preserved), a JSONB with a null-valued
	// object key (see the null-key characterization below), and string/int8 arrays.
	bigString := strings.Repeat("x", 1<<20)
	valuesRow := func(id int) string {
		return fmt.Sprintf(`(%d,
			32767, 2147483647, 9223372036854775807, 9007199254740993,
			12345678901234567890123456789.123456789, -9876543210.123456789, 100.500000000,
			3.141592653589793, -0.0, 1e300,
			true, 'h`+"é"+`llo`+"\U0001F680"+`世界', '%s', x'00ff00', 'a1b2c3d4-e5f6-7890-1234-567890abcdef',
			'2024-03-15', '12:34:56.789012', '2024-03-15 12:34:56.789012', '1960-06-15 08:00:00+00', '1 day 2 hours 3 minutes',
			'{"a":{"b":[1,2,null]},"s":"txt"}', '{"keep":1,"drop":null,"arr":[1,null,2]}', ARRAY['x','y','z'], ARRAY[1,2,9007199254740993]
		)`, id, bigString)
	}

	// id=1 via snapshot path.
	require.NoError(t, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES %s`, src, valuesRow(1))))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	// JSON so JSONB maps to a ClickHouse JSON column (exercises the real JSONB path).
	cfg.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	// id=2 identical values via CDC path.
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES %s`, src, valuesRow(2))))

	EnvWaitForCount(env, s, "2 type-matrix rows", dstTable, "id", 2)

	// Group 1: integers + bool + string + uuid + temporal + bytes — round-trip
	// exactly on both snapshot and CDC paths (verified locally, incl. INT8 > 2^53,
	// unicode/emoji, a 1MB string, bytes with 0x00/0xff, microsecond timestamps,
	// and a pre-1970 TIMESTAMPTZ).
	EnvWaitForEqualTablesWithNames(env, s, "scalars round-trip", srcTable, dstTable,
		"id,i2,i4,i8,big_i8,b,s,s_big,by,u,d,tm,ts,tstz")

	// Group 2: floats, including -0.0 and a large exponent (1e300).
	EnvWaitForEqualTablesWithNames(env, s, "floats round-trip", srcTable, dstTable,
		"id,f8,f8_negzero,f8_bigexp")

	// Group 3: decimals. High-precision DECIMAL(38,9) stresses the changefeed's
	// JSON-number decode (the WP-B decoder reads json.Number, not float64). Verified
	// locally that the 29-integer-digit value, a negative, and a trailing-zero value
	// all round-trip exactly on the CDC path — no float64 precision loss.
	EnvWaitForEqualTablesWithNames(env, s, "decimals round-trip", srcTable, dstTable,
		"id,dec_hi,dec_neg,dec_tz")

	// Group 4: JSONB round-trip. The value has NO null-valued object key (those are
	// dropped by ClickHouse's JSON type — characterized separately below); a null
	// ARRAY element is included and IS preserved.
	EnvWaitForEqualTablesWithNames(env, s, "jsonb round-trip", srcTable, dstTable, "id,j")

	// Group 5: arrays (STRING[] and INT8[], incl. an element > 2^53).
	EnvWaitForEqualTablesWithNames(env, s, "arrays round-trip", srcTable, dstTable,
		"id,arr_s,arr_i8")

	// LOUD CHARACTERIZATION — INTERVAL lands as a ClickHouse String carrying CRDB's
	// interval JSON representation ({"days":..,"hours":..,"minutes":..,"valid":true}),
	// not a native interval type. The content is preserved end-to-end, but the
	// destination QValue kind is String while the source kind is Interval, so a
	// kind-sensitive table comparison (EnvWaitForEqualTablesWithNames) reports a
	// mismatch even though the data is correct. Assert the stored representation
	// directly instead of via table equality.
	ivRows, err := s.GetRows(dstTable, "id,iv")
	EnvNoError(t, env, err)
	EnvTrue(t, env, len(ivRows.Records) == 2)
	for _, r := range ivRows.Records {
		var m map[string]any
		EnvNoError(t, env, json.Unmarshal([]byte(r[1].Value().(string)), &m))
		// '1 day 2 hours 3 minutes'
		EnvTrue(t, env, m["days"] == float64(1) && m["hours"] == float64(2) && m["minutes"] == float64(3))
	}

	// LOUD CHARACTERIZATION — JSON null semantics on the ClickHouse JSON type.
	// A JSONB object key whose value is JSON `null` is NOT stored by ClickHouse's
	// JSON type (it is indistinguishable from an absent key), so `"drop":null` is
	// silently removed downstream. A null ELEMENT inside an array IS preserved.
	// This is ClickHouse JSON-type behavior, not a decoder bug; anyone relying on
	// null-valued JSON object keys surviving to ClickHouse must know this.
	jNullRows, err := s.GetRows(dstTable, "id,j_null")
	EnvNoError(t, env, err)
	EnvTrue(t, env, len(jNullRows.Records) == 2)
	for _, r := range jNullRows.Records {
		var m map[string]any
		EnvNoError(t, env, json.Unmarshal([]byte(r[1].Value().(string)), &m))
		_, hasDrop := m["drop"]
		EnvTrue(t, env, !hasDrop)      // null-valued object key dropped
		EnvTrue(t, env, m["keep"] != nil)
		arr, ok := m["arr"].([]any)
		EnvTrue(t, env, ok && len(arr) == 3 && arr[1] == nil) // null array element preserved
	}

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_Multi_Table_Single_Changefeed mirrors three tables in one changefeed and
// asserts each propagates independently.
func (s CockroachClickhouseSuite) Test_Multi_Table_Single_Changefeed() {
	t := s.T()
	src1, dst1 := "mt_a", "mt_a_dst"
	src2, dst2 := "mt_b", "mt_b_dst"
	src3, dst3 := "mt_c", "mt_c_dst"

	for _, tbl := range []string{src1, src2, src3} {
		require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, AttachSchema(s, tbl))))
	}

	cfg := s.crdbCDCConfig("multitable", src1, dst1, src2, dst2, src3, dst3)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	for i, tbl := range []string{src1, src2, src3} {
		EnvNoError(t, env, s.source().Exec(t.Context(), fmt.Sprintf(
			`INSERT INTO %s (id, val) VALUES (%d, 'row_%d')`, AttachSchema(s, tbl), i+1, i+1)))
	}
	EnvWaitForEqualTablesWithNames(env, s, "table a", src1, dst1, "id,val")
	EnvWaitForEqualTablesWithNames(env, s, "table b", src2, dst2, "id,val")
	EnvWaitForEqualTablesWithNames(env, s, "table c", src3, dst3, "id,val")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_Pause_Resume pauses the mirror, writes rows while paused, resumes, and
// asserts the rows written during the pause arrive.
func (s CockroachClickhouseSuite) Test_Pause_Resume() {
	t := s.T()
	srcTable := "pauseresume"
	dstTable := "pauseresume_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (1, 'before_pause')`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "pre-pause row", srcTable, dstTable, "id,val")

	SignalWorkflow(t.Context(), env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, env, time.Minute, "workflow paused", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (2, 'during_pause')`, src)))

	SignalWorkflow(t.Context(), env, model.FlowSignal, model.NoopSignal)
	EnvWaitFor(t, env, time.Minute, "workflow resumed", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})

	EnvWaitForEqualTablesWithNames(env, s, "row written during pause arrives",
		srcTable, dstTable, "id,val")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_Idle_Checkpoint_Advances asserts that with NO source writes, the changefeed
// still emits resolved messages and the persisted checkpoint (last_text HLC in the
// catalog) advances past the initial t0 — bounding recovery cost on idle mirrors.
func (s CockroachClickhouseSuite) Test_Idle_Checkpoint_Advances() {
	t := s.T()
	srcTable := "idle"
	dstTable := "idle_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, src)))
	require.NoError(t, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (1, 'seed')`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	EnvWaitForEqualTablesWithNames(env, s, "seed synced", srcTable, dstTable, "id,val")

	catalog, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	EnvNoError(t, env, err)

	readCheckpoint := func() string {
		var text string
		if err := catalog.QueryRow(t.Context(),
			`SELECT last_text FROM metadata_last_sync_state WHERE job_name = $1`,
			cfg.FlowJobName).Scan(&text); err != nil {
			t.Log("checkpoint read error", err)
			return ""
		}
		return text
	}

	// With no further writes, the checkpoint must still advance as resolved
	// messages arrive (default resolved interval is 10s).
	initial := readCheckpoint()
	EnvTrue(t, env, initial != "")
	initialHLC, err := decode.ParseHLC(initial)
	EnvNoError(t, env, err)

	EnvWaitFor(t, env, 2*time.Minute, "idle checkpoint advances", func() bool {
		cur := readCheckpoint()
		if cur == "" || cur == initial {
			return false
		}
		curHLC, err := decode.ParseHLC(cur)
		if err != nil {
			t.Log("bad hlc", cur, err)
			return false
		}
		return curHLC.Compare(initialHLC) > 0
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_Slow_First_Resolved_Empty_Table creates a mirror on an empty table and
// then, only after a delay, writes a row. This exercises the sync loop waiting for
// the changefeed's first resolved without erroring, then converging once data
// arrives. Temperamental changefeeds can take >10s to first-resolve, so the wait
// timeout is generous.
func (s CockroachClickhouseSuite) Test_Slow_First_Resolved_Empty_Table() {
	t := s.T()
	srcTable := "slowfirst"
	dstTable := "slowfirst_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	// Let the changefeed idle on an empty table for a while — the sync loop must
	// not error while waiting for its first resolved message.
	EnvWaitFor(t, env, 40*time.Second, "mirror stays running while idle", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})
	time.Sleep(20 * time.Second)
	EnvTrue(t, env, env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING)

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (1, 'late')`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "late row arrives", srcTable, dstTable, "id,val")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_Large_Transaction_Burst commits 10k+ rows in one transaction with a small
// MaxBatchSize so the burst spans multiple sync batches. All rows must arrive and
// the sync batch id must increment across batches.
func (s CockroachClickhouseSuite) Test_Large_Transaction_Burst() {
	t := s.T()
	srcTable := "burst"
	dstTable := "burst_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	cfg.MaxBatchSize = 1000
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	const burst = 12000
	EnvNoError(t, env, s.source().Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %[1]s (id, val) SELECT g, 'b_'||g::STRING FROM generate_series(1, %d) g`, src, burst)))
	EnvWaitForCount(env, s, "all burst rows arrive", dstTable, "id,val", burst)
	EnvWaitForEqualTablesWithNames(env, s, "burst converges", srcTable, dstTable, "id,val")

	catalog, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	EnvNoError(t, env, err)
	var syncBatchID int64
	EnvNoError(t, env, catalog.QueryRow(t.Context(),
		`SELECT sync_batch_id FROM metadata_last_sync_state WHERE job_name = $1`,
		cfg.FlowJobName).Scan(&syncBatchID))
	// 12000 rows at MaxBatchSize 1000 must have taken more than one sync batch.
	EnvTrue(t, env, syncBatchID > 1)

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Test_Add_Column_Mid_Stream adds a column to a mirrored table mid-stream. With
// CockroachDB's default schema_change_policy=backfill the changefeed re-emits all
// existing rows after the schema change. The mirror MUST survive (no error) and
// the data present on both sides must converge. New-column values are NOT
// propagated until schema-delta support lands (Phase 3); the assertion below
// deliberately compares only the pre-existing columns and documents that the new
// column is dropped downstream for now.
//
// If the mirror does not survive an ADD COLUMN, this test will fail at the
// convergence wait — that failure is the characterization to report (the fix
// belongs in the connector, owned by the chaos WP), not to be papered over here.
func (s CockroachClickhouseSuite) Test_Add_Column_Mid_Stream() {
	t := s.T()
	srcTable := "addcol"
	dstTable := "addcol_dst"
	src := AttachSchema(s, srcTable)

	require.NoError(t, s.source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, src)))

	cfg := s.crdbCDCConfig(srcTable, srcTable, dstTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)

	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (1, 'before_ddl')`, src)))
	EnvWaitForEqualTablesWithNames(env, s, "pre-ddl row", srcTable, dstTable, "id,val")

	// Add a column and insert a row using it. Default backfill re-emits rows.
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN extra TEXT`, src)))
	EnvNoError(t, env, s.source().Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, val, extra) VALUES (2, 'after_ddl', 'x2')`, src)))

	// The mirror must survive and both old columns must converge (id=1 and id=2).
	// The `extra` column is intentionally excluded — its values are dropped
	// downstream until Phase 3 schema-delta support exists.
	EnvWaitForEqualTablesWithNames(env, s, "mirror survives ADD COLUMN", srcTable, dstTable, "id,val")
	EnvTrue(t, env, env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING)

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
