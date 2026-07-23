package conncockroach

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	extmeta "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// --- pure unit tests (no CockroachDB needed) ---

func TestBuildChangefeedStatement(t *testing.T) {
	tables := []string{`"public"."orders"`, `"public"."items"`}

	stmt, err := buildChangefeedStatement(tables, "1783372607043326006.0000000000", 0, nil)
	require.NoError(t, err)
	require.Contains(t, stmt, `CREATE CHANGEFEED FOR TABLE "public"."orders", "public"."items" WITH `)
	require.Contains(t, stmt, "envelope = 'wrapped'")
	require.Contains(t, stmt, "diff")
	require.Contains(t, stmt, "updated")
	require.Contains(t, stmt, "mvcc_timestamp")
	require.Contains(t, stmt, "initial_scan = 'no'")
	// unset resolved interval falls back to the default
	require.Contains(t, stmt, fmt.Sprintf("resolved = '%ds'", defaultResolvedIntervalSeconds))
	require.Contains(t, stmt, fmt.Sprintf("min_checkpoint_frequency = '%ds'", defaultResolvedIntervalSeconds))
	require.Contains(t, stmt, "cursor = '1783372607043326006.0000000000'")

	// explicit interval is honored
	stmt, err = buildChangefeedStatement(tables, "", 5, nil)
	require.NoError(t, err)
	require.Contains(t, stmt, "resolved = '5s'")
	require.NotContains(t, stmt, "cursor = ", "no cursor clause when cursor is empty")

	// extra options: flag form and key=value form
	stmt, err = buildChangefeedStatement(tables, "", 5, map[string]string{
		"gc_protect_expires_after": "24h",
		"schema_change_events":     "",
	})
	require.NoError(t, err)
	require.Contains(t, stmt, "gc_protect_expires_after = '24h'")
	require.Contains(t, stmt, ", schema_change_events")

	// reserved options are rejected
	for _, reserved := range []string{"cursor", "envelope", "initial_scan", "RESOLVED", "Diff"} {
		_, err = buildChangefeedStatement(tables, "", 5, map[string]string{reserved: "x"})
		require.Error(t, err, "reserved option %q should be rejected", reserved)
	}

	// injection-y keys are rejected
	_, err = buildChangefeedStatement(tables, "", 5, map[string]string{"foo'; DROP": "x"})
	require.Error(t, err)
}

func TestQuoteLiteral(t *testing.T) {
	require.Equal(t, "abc", quoteLiteral("abc"))
	require.Equal(t, "O''Brien", quoteLiteral("O'Brien"))
	require.Equal(t, "'' OR 1=1 --", quoteLiteral("' OR 1=1 --"))
}

func TestTableResolver(t *testing.T) {
	mapping := map[string]model.NameAndExclude{
		"public.orders": model.NewNameAndExclude("dst_orders", nil),
		"public.items":  model.NewNameAndExclude("dst_items", nil),
	}
	r := newTableResolver(mapping)

	// bare name (what a sinkless changefeed reports by default)
	src, nae, ok := r.resolve("orders")
	require.True(t, ok)
	require.Equal(t, "public.orders", src)
	require.Equal(t, "dst_orders", nae.Name)

	// exact source identifier
	src, _, ok = r.resolve("public.items")
	require.True(t, ok)
	require.Equal(t, "public.items", src)

	// database.schema.table form (WITH full_table_name) resolves on last component
	src, _, ok = r.resolve("defaultdb.public.orders")
	require.True(t, ok)
	require.Equal(t, "public.orders", src)

	// unmapped table
	_, _, ok = r.resolve("widgets")
	require.False(t, ok)
}

func TestTableResolverAmbiguousBare(t *testing.T) {
	// two source tables sharing a bare name across schemas: the bare form is
	// ambiguous and must not resolve, but the exact identifiers still do.
	mapping := map[string]model.NameAndExclude{
		"public.orders": model.NewNameAndExclude("dst_a", nil),
		"sales.orders":  model.NewNameAndExclude("dst_b", nil),
	}
	r := newTableResolver(mapping)

	_, _, ok := r.resolve("orders")
	require.False(t, ok, "ambiguous bare name should not resolve")

	src, _, ok := r.resolve("public.orders")
	require.True(t, ok)
	require.Equal(t, "public.orders", src)
}

func TestChangefeedTables(t *testing.T) {
	tables, err := changefeedTables(map[string]model.NameAndExclude{
		"public.orders": model.NewNameAndExclude("dst_orders", nil),
		"public.items":  model.NewNameAndExclude("dst_items", nil),
	})
	require.NoError(t, err)
	require.Equal(t, []string{`"public"."items"`, `"public"."orders"`}, tables)

	_, err = changefeedTables(map[string]model.NameAndExclude{"noschema": model.NewNameAndExclude("x", nil)})
	require.Error(t, err)
}

// --- live integration test (gated on CI_COCKROACH_HOST) ---

// cockroachTestConfig reads a CockroachDB connection from the CI_COCKROACH_* env
// vars, returning nil to signal the caller to skip when the host is unset.
func cockroachTestConfig(t *testing.T) *protos.CockroachConfig {
	t.Helper()
	host := os.Getenv("CI_COCKROACH_HOST")
	if host == "" {
		return nil
	}
	port := uint32(26257)
	if p := os.Getenv("CI_COCKROACH_PORT"); p != "" {
		parsed, err := strconv.ParseUint(p, 10, 32)
		require.NoError(t, err)
		port = uint32(parsed)
	}
	user := os.Getenv("CI_COCKROACH_USER")
	if user == "" {
		user = "root"
	}
	database := os.Getenv("CI_COCKROACH_DATABASE")
	if database == "" {
		database = "defaultdb"
	}
	disableTLS := true
	// Test hygiene: the CRDB container is shared with the e2e suite, so every
	// protection job a SetupReplication-driven test creates must auto-expire fast
	// rather than pin GC for the 24h production default. Tests that specifically
	// exercise protection override this with their own window and cancel explicitly.
	protectionWindow := uint32(30)
	return &protos.CockroachConfig{
		Host:                           host,
		Port:                           port,
		User:                           user,
		Database:                       database,
		DisableTls:                     &disableTLS,
		ResolvedIntervalSeconds:        2, // fast resolved cadence for the test
		HistoryProtectionWindowSeconds: &protectionWindow,
	}
}

// newIntegrationConnector builds a connector wired to a CRDB-backed metadata
// table so SetupReplication/GetLastOffset exercise the real persistence path.
// CockroachDB is Postgres-wire compatible, so it doubles as the catalog surrogate.
func newIntegrationConnector(t *testing.T, ctx context.Context, config *protos.CockroachConfig) *CockroachConnector {
	t.Helper()

	connStr := GetConnectionString(config, "")
	connConfig, err := ParseConfig(connStr, config)
	require.NoError(t, err)
	connConfig.Config.RuntimeParams["timezone"] = "UTC"
	conn, err := NewCockroachConnFromConfig(ctx, connConfig, nil)
	require.NoError(t, err)

	// Catalog surrogate: a metadata_last_sync_state table on the same CRDB node.
	metaPool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	_, err = metaPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS metadata_last_sync_state (
		job_name TEXT PRIMARY KEY NOT NULL,
		last_offset BIGINT NOT NULL DEFAULT 0,
		last_text TEXT,
		sync_batch_id BIGINT NOT NULL DEFAULT 0,
		normalize_batch_id BIGINT,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	require.NoError(t, err)

	logger := internal.LoggerFromCtx(ctx)
	c := &CockroachConnector{
		PostgresMetadata: extmeta.NewPostgresMetadataFromCatalog(logger, shared.CatalogPool{Pool: metaPool}),
		config:           config,
		conn:             conn,
		typeMap:          pgtype.NewMap(),
		logger:           logger,
		connStr:          connStr,
		metadataSchema:   "_peerdb_internal",
	}
	t.Cleanup(func() {
		c.closeCDC() // stop the persistent changefeed pump goroutine + its connection
		_ = conn.Close(context.Background())
		metaPool.Close()
	})
	return c
}

func TestIntegrationCockroachPullRecordsCDC(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_cdc_it")

	c := newIntegrationConnector(t, ctx, config)

	tableName := "crdb_cdc_" + strings.ToLower(common.RandomString(6))
	srcTable := "public." + tableName
	dstTable := "dst_" + tableName
	qualified := `"public"."` + tableName + `"`

	// Include BYTES and INTERVAL to pin down the changefeed JSON wire form the
	// decoder must handle (verified live: BYTES arrives as a `\x`-hex string,
	// INTERVAL as the postgres-verbose form, e.g. "1 year 2 mons 3 days 04:05:06.789").
	_, err := c.conn.Exec(ctx, "CREATE TABLE "+qualified+" (id INT PRIMARY KEY, name STRING, amount DECIMAL, b BYTES, iv INTERVAL)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = c.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified) })

	flowName := "crdb_cdc_it"
	// SetupReplication captures t0 and persists it as the initial checkpoint.
	_, err = c.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{FlowJobName: flowName})
	require.NoError(t, err)
	require.NoError(t, c.SetupReplConn(ctx, nil))

	// WP-C reads the initial checkpoint via GetLastOffset for the AOST snapshot; we
	// verify it landed and feed it to PullRecords as the changefeed cursor.
	t0, err := c.GetLastOffset(ctx, flowName)
	require.NoError(t, err)
	require.NotEmpty(t, t0.Text, "SetupReplication must persist t0 before snapshot")
	t0HLC, err := decode.ParseHLC(t0.Text)
	require.NoError(t, err)

	// DML after t0: insert -> update -> delete on id=1, plus a fresh insert id=2.
	_, err = c.conn.Exec(ctx, "INSERT INTO "+qualified+
		" (id, name, amount, b, iv) VALUES (1, 'alice', 10.50, b'\\x00\\x01\\xffhello', INTERVAL '1 year 2 mons 3 days 04:05:06.789')")
	require.NoError(t, err)
	_, err = c.conn.Exec(ctx, "UPDATE "+qualified+" SET amount = 99.99 WHERE id = 1")
	require.NoError(t, err)
	_, err = c.conn.Exec(ctx, "DELETE FROM "+qualified+" WHERE id = 1")
	require.NoError(t, err)
	_, err = c.conn.Exec(ctx, "INSERT INTO "+qualified+" (id, name, amount) VALUES (2, 'bob', 5.00)")
	require.NoError(t, err)

	// Real schema via WP-A introspection, keyed the way PullRecords expects.
	schemas, err := c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q, []*protos.TableMapping{
		{SourceTableIdentifier: srcTable, DestinationTableIdentifier: dstTable},
	})
	require.NoError(t, err)
	tableSchema := schemas[srcTable]
	require.NotNil(t, tableSchema)

	stream := model.NewCDCStream[model.RecordItems](1 << 10)
	otelManager, err := otel_metrics.NewOtelManager(ctx, "test", false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelManager.Close(context.Background()) })

	req := &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName:            flowName,
		RecordStream:           stream,
		TableNameMapping:       map[string]model.NameAndExclude{srcTable: model.NewNameAndExclude(dstTable, nil)},
		TableNameSchemaMapping: map[string]*protos.TableSchema{dstTable: tableSchema},
		LastOffset:             t0,
		MaxBatchSize:           100,
		// A freshly created changefeed's first resolved message trails its initial
		// catch-up + closed-timestamp latency (seconds), so the idle timeout is set
		// generously; the resolved-based batch cut ends the batch as soon as the
		// first resolved arrives after our records, so this does not slow the test.
		IdleTimeout: 30 * time.Second,
		Env:         nil,
	}

	// PullRecords blocks until idle timeout; run it and drain the stream after.
	pullErr := make(chan error, 1)
	go func() { pullErr <- c.PullRecords(ctx, shared.CatalogPool{}, otelManager, req) }()

	// The drain goroutine only collects; all assertions run on the test goroutine
	// (require.FailNow must not be called from a non-test goroutine).
	var records []model.Record[model.RecordItems]
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for rec := range stream.GetRecords() {
			records = append(records, rec)
		}
	}()

	require.NoError(t, <-pullErr)
	<-drainDone

	var inserts, updates, deletes int
	var sawDeleteBefore, sawTypedInsert bool
	for _, rec := range records {
		switch r := rec.(type) {
		case *model.InsertRecord[model.RecordItems]:
			inserts++
			id, err := r.Items.GetValueByColName("id")
			require.NoError(t, err)
			if id.Value() == int64(1) {
				// BYTES decodes from the changefeed's `\x`-hex form to raw bytes.
				b, err := r.Items.GetValueByColName("b")
				require.NoError(t, err)
				require.Equal(t, types.QValueKindBytes, b.Kind())
				require.Equal(t, []byte{0x00, 0x01, 0xff, 'h', 'e', 'l', 'l', 'o'}, b.Value())
				// INTERVAL decodes from the postgres-verbose form to a QValueInterval.
				iv, err := r.Items.GetValueByColName("iv")
				require.NoError(t, err)
				require.Equal(t, types.QValueKindInterval, iv.Kind())
				require.NotEmpty(t, iv.Value())
				sawTypedInsert = true
			}
		case *model.UpdateRecord[model.RecordItems]:
			updates++
			// diff gives us the pre-image: amount should have been 10.50
			old, err := r.OldItems.GetValueByColName("amount")
			require.NoError(t, err)
			require.Equal(t, types.QValueKindNumeric, old.Kind())
		case *model.DeleteRecord[model.RecordItems]:
			deletes++
			// delete carries the before-image (from `diff`): id=1, name='alice'
			name, err := r.Items.GetValueByColName("name")
			require.NoError(t, err)
			require.Equal(t, "alice", name.Value())
			sawDeleteBefore = true
		}
	}

	require.Equal(t, 2, inserts, "expected 2 inserts (id=1 initial, id=2)")
	require.Equal(t, 1, updates, "expected 1 update")
	require.Equal(t, 1, deletes, "expected 1 delete")
	require.True(t, sawDeleteBefore, "delete record should carry the before-image via diff")
	require.True(t, sawTypedInsert, "expected the id=1 insert carrying BYTES and INTERVAL")

	// Checkpoint must have advanced past t0 on a resolved message.
	checkpoint := stream.GetLastCheckpoint()
	require.NotEmpty(t, checkpoint.Text)
	cpHLC, err := decode.ParseHLC(checkpoint.Text)
	require.NoError(t, err)
	require.Positive(t, cpHLC.Compare(t0HLC), "resolved checkpoint %s should be > t0 %s", checkpoint.Text, t0.Text)
}
