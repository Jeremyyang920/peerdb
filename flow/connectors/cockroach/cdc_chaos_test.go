package conncockroach

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// These tests exercise CockroachDB sinkless-changefeed CDC against a live cluster
// and are gated on CI_COCKROACH_HOST (see cockroachTestConfig). CRDB changefeeds
// are temperamental — a freshly created feed's first resolved message trails its
// initial rangefeed catch-up plus closed-timestamp latency (can exceed 10s) — so
// every assertion polls with a deadline rather than sleeping a fixed interval.
//
// IMPORTANT: the live CRDB container is shared with the e2e suite. These tests
// therefore NEVER stop/pause the container and NEVER cancel changefeeds by a
// bare CHANGEFEED match; every server-side disruption is scoped to the test's
// own uniquely-named table so a sibling mirror is never touched.

// chaosFixture bundles a connector wired to live CRDB with a uniquely-named
// source table, plus a dedicated admin connection used to induce disruptions
// (canceling the changefeed's backend query) and drive the workload.
type chaosFixture struct {
	c         *CockroachConnector
	admin     *pgx.Conn
	flowName  string
	tableName string
	srcTable  string
	dstTable  string
	qualified string
	schema    *protos.TableSchema
	otel      *otel_metrics.OtelManager
}

// newChaosFixture creates a fresh table (id INT PRIMARY KEY, v STRING), runs
// SetupReplication to capture t0, and introspects the schema — the same setup a
// real mirror performs before the CDC handoff.
func newChaosFixture(t *testing.T, ctx context.Context, config *protos.CockroachConfig, cols string) *chaosFixture {
	t.Helper()
	c := newIntegrationConnector(t, ctx, config)

	tableName := "crdb_chaos_" + strings.ToLower(common.RandomString(8))
	srcTable := "public." + tableName
	dstTable := "dst_" + tableName
	qualified := `"public"."` + tableName + `"`

	_, err := c.conn.Exec(ctx, "CREATE TABLE "+qualified+" ("+cols+")")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = c.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified) })

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	_, err = c.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{FlowJobName: flowName})
	require.NoError(t, err)

	schemas, err := c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q, []*protos.TableMapping{
		{SourceTableIdentifier: srcTable, DestinationTableIdentifier: dstTable},
	})
	require.NoError(t, err)
	require.NotNil(t, schemas[srcTable])

	connStr := GetConnectionString(config, "")
	admin, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	otel, err := otel_metrics.NewOtelManager(ctx, "test", false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otel.Close(context.Background()) })

	return &chaosFixture{
		c: c, admin: admin, flowName: flowName, tableName: tableName,
		srcTable: srcTable, dstTable: dstTable, qualified: qualified,
		schema: schemas[srcTable], otel: otel,
	}
}

func (f *chaosFixture) mapping() (map[string]model.NameAndExclude, map[string]*protos.TableSchema) {
	return map[string]model.NameAndExclude{f.srcTable: model.NewNameAndExclude(f.dstTable, nil)},
		map[string]*protos.TableSchema{f.dstTable: f.schema}
}

// cancelChangefeed cancels the backend query running this fixture's changefeed,
// scoped to the fixture's table name and anchored on the CHANGEFEED statement so
// a concurrent lookup or a sibling mirror's feed is never matched. Returns true
// if a query was found and canceled.
func (f *chaosFixture) cancelChangefeed(t *testing.T, ctx context.Context) bool {
	t.Helper()
	// Anchoring on 'CHANGEFEED%'/'EXPERIMENTAL CHANGEFEED%' guarantees this SELECT
	// (which contains the table name as a literal) never matches its own session.
	q := fmt.Sprintf(`SELECT query_id FROM crdb_internal.cluster_queries
		WHERE (query ILIKE 'CREATE CHANGEFEED%%' OR query ILIKE 'EXPERIMENTAL CHANGEFEED%%')
		  AND query ILIKE '%%%s%%' LIMIT 1`, f.tableName)
	var qid string
	if err := f.admin.QueryRow(ctx, q).Scan(&qid); err != nil {
		return false
	}
	_, err := f.admin.Exec(ctx, fmt.Sprintf("CANCEL QUERY '%s'", qid))
	require.NoError(t, err)
	return true
}

// pullBatch runs one PullRecords batch to completion, draining the stream
// concurrently (assertions stay on the caller goroutine). It returns the records
// pulled, the batch checkpoint, and the PullRecords error (if any).
func (f *chaosFixture) pullBatch(
	ctx context.Context, offset model.CdcCheckpoint, maxBatch uint32, idle time.Duration,
) ([]model.Record[model.RecordItems], model.CdcCheckpoint, error) {
	mapping, schemaMapping := f.mapping()
	stream := model.NewCDCStream[model.RecordItems](1 << 16)
	req := &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName:            f.flowName,
		RecordStream:           stream,
		TableNameMapping:       mapping,
		TableNameSchemaMapping: schemaMapping,
		LastOffset:             offset,
		MaxBatchSize:           maxBatch,
		IdleTimeout:            idle,
	}

	errc := make(chan error, 1)
	go func() { errc <- f.c.PullRecords(ctx, shared.CatalogPool{}, f.otel, req) }()

	var records []model.Record[model.RecordItems]
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for rec := range stream.GetRecords() {
			records = append(records, rec)
		}
	}()

	err := <-errc
	<-drainDone
	return records, stream.GetLastCheckpoint(), err
}

// insertPKs records the primary-key ids of insert records into seen.
func insertPKs(t *testing.T, records []model.Record[model.RecordItems], seen map[int64]int) {
	t.Helper()
	for _, rec := range records {
		ins, ok := rec.(*model.InsertRecord[model.RecordItems])
		if !ok {
			continue
		}
		id, err := ins.Items.GetValueByColName("id")
		require.NoError(t, err)
		seen[id.Value().(int64)]++
	}
}

// --- 1a/1b/1c: reconnect/resume under repeated mid-stream kills ---

// TestIntegrationCockroachChaosMidStreamKill proves that killing the changefeed's
// backend query mid-stream (3 times) never loses a committed row: the pump
// reconnects with backoff from the last resolved HLC and replay covers the gap
// between the last checkpoint and the kill. Duplicates are tolerated; absences
// are not. Also asserts the checkpoint only ever advances to a valid resolved HLC
// (never past unresolved queued rows), which is what makes the replay lossless.
func TestIntegrationCockroachChaosMidStreamKill(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_kill")
	f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")

	const totalRows = 300
	writer, err := pgx.Connect(ctx, GetConnectionString(config, ""))
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Steady insert workload on its own connection.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= totalRows; i++ {
			if _, err := writer.Exec(ctx, "INSERT INTO "+f.qualified+" (id, v) VALUES ($1, $2)", i, fmt.Sprintf("v%d", i)); err != nil {
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	offset, err := f.c.GetLastOffset(ctx, f.flowName)
	require.NoError(t, err)
	t0HLC, err := decode.ParseHLC(offset.Text)
	require.NoError(t, err)

	seen := make(map[int64]int)
	kills := 0
	deadline := time.Now().Add(90 * time.Second)
	prevCheckpoint := offset
	for len(seen) < totalRows && time.Now().Before(deadline) {
		records, checkpoint, perr := f.pullBatch(ctx, offset, 50, 3*time.Second)
		insertPKs(t, records, seen)

		// The checkpoint must always be a valid resolved HLC that never regresses:
		// it advances only on resolved messages, so it can never point past an
		// unresolved queued row (1b — this is what keeps replay lossless).
		if checkpoint.Text != "" {
			cpHLC, perr2 := decode.ParseHLC(checkpoint.Text)
			require.NoError(t, perr2)
			require.GreaterOrEqual(t, cpHLC.Compare(t0HLC), 0, "checkpoint regressed below t0")
			if prevHLC, e := decode.ParseHLC(prevCheckpoint.Text); e == nil {
				require.GreaterOrEqual(t, cpHLC.Compare(prevHLC), 0, "checkpoint regressed across batches")
			}
			offset = checkpoint
			prevCheckpoint = checkpoint
		}

		// A recoverable error (e.g. the batch ended because the kill exhausted the
		// short in-loop ladder) is fine: the next PullRecords resumes from offset.
		if perr != nil {
			require.NotErrorIs(t, perr, context.Canceled)
		}

		// Kill the changefeed backend up to 3 times, spaced out, to force reconnects.
		if kills < 3 && len(seen) > (kills+1)*40 {
			if f.cancelChangefeed(t, ctx) {
				kills++
			}
		}
	}

	wg.Wait()
	// Drain any tail after the writer finished.
	drainDeadline := time.Now().Add(40 * time.Second)
	for len(seen) < totalRows && time.Now().Before(drainDeadline) {
		records, checkpoint, _ := f.pullBatch(ctx, offset, 200, 3*time.Second)
		insertPKs(t, records, seen)
		if checkpoint.Text != "" {
			offset = checkpoint
		}
	}

	require.GreaterOrEqual(t, kills, 3, "expected at least 3 changefeed kills during the run")
	require.Len(t, seen, totalRows, "every committed row must appear in the union of pulled batches (no loss)")
	for id := int64(1); id <= totalRows; id++ {
		require.Positive(t, seen[id], "row id=%d committed at source but never pulled", id)
	}
}

// --- 1d: backoff exhaustion returns an error (does not hang), fresh connector resumes ---

// TestIntegrationCockroachChaosBackoffExhaustion proves that when the changefeed
// cannot be re-established, PullRecords returns an error after exhausting the
// reconnect ladder instead of hanging forever. It induces this WITHOUT stopping
// the shared container: after a healthy batch it points the connector's reconnect
// at a dead endpoint, then cancels the feed's backend query so the next
// PullRecords must reconnect and fails every attempt.
func TestIntegrationCockroachChaosBackoffExhaustion(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_backoff")
	f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")

	// Fast, bounded ladder so exhaustion is quick and deterministic.
	f.c.reconnectMaxAttempts = 3
	f.c.reconnectBaseBackoff = 50 * time.Millisecond
	f.c.reconnectMaxBackoff = 100 * time.Millisecond

	offset, err := f.c.GetLastOffset(ctx, f.flowName)
	require.NoError(t, err)

	// Batch 1: establish the persistent feed and advance to a resolved checkpoint.
	_, err = f.c.conn.Exec(ctx, "INSERT INTO "+f.qualified+" (id, v) VALUES (1, 'a')")
	require.NoError(t, err)
	var checkpoint model.CdcCheckpoint
	deadline := time.Now().Add(30 * time.Second)
	for checkpoint.Text == "" && time.Now().Before(deadline) {
		_, cp, perr := f.pullBatch(ctx, offset, 50, 4*time.Second)
		require.NoError(t, perr)
		checkpoint = cp
	}
	require.NotEmpty(t, checkpoint.Text, "batch 1 should have produced a resolved checkpoint")

	// Point reconnect at a dead endpoint (no PullRecords running → no race on connStr),
	// then cancel the live feed so the next PullRecords is forced to reconnect.
	f.c.connStr = "postgres://root@127.0.0.1:1/defaultdb?sslmode=disable"
	require.True(t, f.cancelChangefeed(t, ctx), "expected to find and cancel the changefeed query")

	start := time.Now()
	_, _, perr := f.pullBatch(ctx, checkpoint, 50, 30*time.Second)
	elapsed := time.Since(start)
	require.Error(t, perr, "PullRecords must return an error once reconnects are exhausted")
	require.Contains(t, perr.Error(), "exhausted", "error should report reconnect exhaustion")
	require.Less(t, elapsed, 15*time.Second, "PullRecords must not hang; bounded by the ladder")

	// A fresh connector resumes from the persisted checkpoint with no loss.
	f2 := &chaosFixture{
		c: newIntegrationConnector(t, ctx, config), admin: f.admin, flowName: f.flowName,
		tableName: f.tableName, srcTable: f.srcTable, dstTable: f.dstTable,
		qualified: f.qualified, schema: f.schema, otel: f.otel,
	}
	for i := 2; i <= 6; i++ {
		_, err = f2.c.conn.Exec(ctx, "INSERT INTO "+f.qualified+" (id, v) VALUES ($1, $2)", i, fmt.Sprintf("v%d", i))
		require.NoError(t, err)
	}
	seen := make(map[int64]int)
	off := checkpoint
	rdeadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(rdeadline) {
		allSeen := true
		for i := int64(2); i <= 6; i++ {
			if seen[i] == 0 {
				allSeen = false
			}
		}
		if allSeen {
			break
		}
		records, cp, _ := f2.pullBatch(ctx, off, 50, 3*time.Second)
		insertPKs(t, records, seen)
		if cp.Text != "" {
			off = cp
		}
	}
	for i := int64(2); i <= 6; i++ {
		require.Positive(t, seen[i], "fresh connector should resume and pull row id=%d", i)
	}
}

// --- 2: GC-threshold cursor surfaces as a terminal, classified error ---

// TestIntegrationCockroachChaosGCThresholdCursor proves that a changefeed started
// from a cursor older than CRDB's retained MVCC history surfaces as a terminal
// error (not an infinite reconnect loop) and is classified needs-resync.
func TestIntegrationCockroachChaosGCThresholdCursor(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_gc")
	f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")

	// A cursor ~10 days in the past predates any retained history for this table.
	ancient := fmt.Sprintf("%d.0000000000", time.Now().Add(-10*24*time.Hour).UnixNano())

	start := time.Now()
	_, _, perr := f.pullBatch(ctx, model.CdcCheckpoint{Text: ancient}, 50, 30*time.Second)
	elapsed := time.Since(start)

	require.Error(t, perr, "an ancient cursor must surface a terminal error")
	require.Less(t, elapsed, 20*time.Second, "GC-threshold error must be terminal, not an infinite reconnect")

	var cfErr *exceptions.CockroachChangefeedError
	require.ErrorAs(t, perr, &cfErr, "GC-threshold failure must be a CockroachChangefeedError")
	require.Equal(t, exceptions.CockroachChangefeedGCThreshold, cfErr.Code)
}

// --- 4: TRUNCATE / DROP kill the feed and are classified needs-resync ---

func TestIntegrationCockroachChaosTruncateAndDrop(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}

	for _, tc := range []struct {
		name    string
		ddl     string
		wantErr string
	}{
		{"truncate", "TRUNCATE TABLE %s", exceptions.CockroachChangefeedTableTruncated},
		{"drop", "DROP TABLE %s", exceptions.CockroachChangefeedTableDropped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_"+tc.name)
			f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")
			offset, err := f.c.GetLastOffset(ctx, f.flowName)
			require.NoError(t, err)

			// No pre-insert: a row event would let the batch cut at the first resolved
			// and return before the DDL. With the table quiet, the batch stays open
			// (bounded by the long IdleTimeout) until the DDL fails the feed, which is
			// the condition under test. TRUNCATE/DROP fail the changefeed regardless of
			// whether any rows have been emitted.

			// Run PullRecords; issue the DDL from the admin conn shortly after so it
			// lands while the feed is streaming.
			mapping, schemaMapping := f.mapping()
			stream := model.NewCDCStream[model.RecordItems](1 << 12)
			req := &model.PullRecordsRequest[model.RecordItems]{
				FlowJobName: f.flowName, RecordStream: stream,
				TableNameMapping: mapping, TableNameSchemaMapping: schemaMapping,
				LastOffset: offset, MaxBatchSize: 1000, IdleTimeout: 30 * time.Second,
			}
			errc := make(chan error, 1)
			go func() { errc <- f.c.PullRecords(ctx, shared.CatalogPool{}, f.otel, req) }()
			go func() {
				for range stream.GetRecords() {
				}
			}()

			// Give the feed a moment to catch up, then issue the fatal DDL.
			time.Sleep(6 * time.Second)
			_, err = f.admin.Exec(ctx, fmt.Sprintf(tc.ddl, f.qualified))
			require.NoError(t, err)

			select {
			case perr := <-errc:
				require.Error(t, perr)
				var cfErr *exceptions.CockroachChangefeedError
				require.ErrorAs(t, perr, &cfErr, "%s must be a CockroachChangefeedError", tc.name)
				require.Equal(t, tc.wantErr, cfErr.Code)
			case <-time.After(30 * time.Second):
				t.Fatalf("%s did not terminate PullRecords within 30s", tc.name)
			}
		})
	}
}

// --- 3: temperamental timing ---

// TestIntegrationCockroachChaosIdleTableCleanEmpty proves an idle table returns a
// clean empty batch (no error) even before the first resolved arrives, and that a
// subsequent batch eventually persists an advanced checkpoint via the idle-advance
// path (the no-records workflow path does not call UpdateReplStateLastOffset).
func TestIntegrationCockroachChaosIdleTableCleanEmpty(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_idle")
	f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")
	offset, err := f.c.GetLastOffset(ctx, f.flowName)
	require.NoError(t, err)
	t0HLC, err := decode.ParseHLC(offset.Text)
	require.NoError(t, err)

	// First batch on a brand-new feed with a short idle: returns cleanly empty even
	// though the first resolved has almost certainly not arrived yet.
	records, _, perr := f.pullBatch(ctx, offset, 100, 2*time.Second)
	require.NoError(t, perr, "idle table must return cleanly, not error")
	require.Empty(t, records, "idle table must produce no records")

	// Subsequent batches: the idle-advance path persists an advanced checkpoint once
	// resolveds start flowing (bounded window incl. closed-timestamp lag).
	deadline := time.Now().Add(30 * time.Second)
	advanced := false
	for time.Now().Before(deadline) && !advanced {
		records, _, perr := f.pullBatch(ctx, offset, 100, 3*time.Second)
		require.NoError(t, perr)
		require.Empty(t, records)
		persisted, err := f.c.GetLastOffset(ctx, f.flowName)
		require.NoError(t, err)
		if ph, e := decode.ParseHLC(persisted.Text); e == nil && ph.Compare(t0HLC) > 0 {
			advanced = true
		}
	}
	require.True(t, advanced, "idle-advance path should persist an advanced checkpoint within the window")
}

// TestIntegrationCockroachChaosBurstSingleTxn commits a single 50k-row transaction
// and proves every row arrives across MaxBatchSize-cut batches without the pump
// channel deadlocking.
func TestIntegrationCockroachChaosBurstSingleTxn(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_burst")
	f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")
	offset, err := f.c.GetLastOffset(ctx, f.flowName)
	require.NoError(t, err)

	const burst = 50000
	_, err = f.c.conn.Exec(ctx,
		"INSERT INTO "+f.qualified+" (id, v) SELECT g, 'x' FROM generate_series(1, $1) AS g", burst)
	require.NoError(t, err)

	seen := make(map[int64]int)
	batches := 0
	deadline := time.Now().Add(150 * time.Second)
	for len(seen) < burst && time.Now().Before(deadline) {
		records, cp, perr := f.pullBatch(ctx, offset, 5000, 5*time.Second)
		require.NoError(t, perr)
		insertPKs(t, records, seen)
		if cp.Text != "" {
			offset = cp
		}
		batches++
	}
	require.Len(t, seen, burst, "all burst rows must arrive across batches")
	require.Greater(t, batches, 1, "a 50k burst with MaxBatchSize=5000 must span multiple batches")
}

// TestIntegrationCockroachChaosEmptyTableMaxBatchOne covers the zero-row-table +
// MaxBatchSize=1 edge: PullRecords must return cleanly empty rather than block or
// error.
func TestIntegrationCockroachChaosEmptyTableMaxBatchOne(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_empty1")
	f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")
	offset, err := f.c.GetLastOffset(ctx, f.flowName)
	require.NoError(t, err)

	records, _, perr := f.pullBatch(ctx, offset, 1, 3*time.Second)
	require.NoError(t, perr)
	require.Empty(t, records)
}

// TestIntegrationCockroachChaosMultiTableQuietOne proves that with two tables in
// one feed where only one is written, the written table's rows arrive and the
// checkpoint advances — the quiet table does not block resolved progress.
func TestIntegrationCockroachChaosMultiTableQuietOne(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_multi")
	c := newIntegrationConnector(t, ctx, config)
	flowName := "crdb_chaos_multi"

	suffix := strings.ToLower(common.RandomString(8))
	tblA := "crdb_multi_a_" + suffix
	tblB := "crdb_multi_b_" + suffix
	qA := `"public"."` + tblA + `"`
	qB := `"public"."` + tblB + `"`
	for _, q := range []string{qA, qB} {
		_, err := c.conn.Exec(ctx, "CREATE TABLE "+q+" (id INT PRIMARY KEY, v STRING)")
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_, _ = c.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+qA)
		_, _ = c.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+qB)
	})

	_, err := c.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{FlowJobName: flowName})
	require.NoError(t, err)
	offset, err := c.GetLastOffset(ctx, flowName)
	require.NoError(t, err)
	t0HLC, err := decode.ParseHLC(offset.Text)
	require.NoError(t, err)

	srcA, dstA := "public."+tblA, "dst_"+tblA
	srcB, dstB := "public."+tblB, "dst_"+tblB
	schemas, err := c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q, []*protos.TableMapping{
		{SourceTableIdentifier: srcA, DestinationTableIdentifier: dstA},
		{SourceTableIdentifier: srcB, DestinationTableIdentifier: dstB},
	})
	require.NoError(t, err)

	// Only table A is written.
	for i := 1; i <= 20; i++ {
		_, err = c.conn.Exec(ctx, "INSERT INTO "+qA+" (id, v) VALUES ($1, 'a')", i)
		require.NoError(t, err)
	}

	otel, err := otel_metrics.NewOtelManager(ctx, "test", false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otel.Close(context.Background()) })

	mapping := map[string]model.NameAndExclude{
		srcA: model.NewNameAndExclude(dstA, nil),
		srcB: model.NewNameAndExclude(dstB, nil),
	}
	schemaMapping := map[string]*protos.TableSchema{dstA: schemas[srcA], dstB: schemas[srcB]}

	seen := make(map[int64]int)
	var lastCp model.CdcCheckpoint
	deadline := time.Now().Add(40 * time.Second)
	// Keep pulling until both all rows are seen and a resolved has advanced the
	// checkpoint: a fresh feed's first resolved trails its catch-up, so an early
	// batch can carry all 20 rows yet not have consumed a resolved checkpoint yet.
	for (len(seen) < 20 || lastCp.Text == "") && time.Now().Before(deadline) {
		stream := model.NewCDCStream[model.RecordItems](1 << 12)
		req := &model.PullRecordsRequest[model.RecordItems]{
			FlowJobName: flowName, RecordStream: stream,
			TableNameMapping: mapping, TableNameSchemaMapping: schemaMapping,
			LastOffset: offset, MaxBatchSize: 100, IdleTimeout: 3 * time.Second,
		}
		errc := make(chan error, 1)
		go func() { errc <- c.PullRecords(ctx, shared.CatalogPool{}, otel, req) }()
		var records []model.Record[model.RecordItems]
		done := make(chan struct{})
		go func() {
			defer close(done)
			for r := range stream.GetRecords() {
				records = append(records, r)
			}
		}()
		require.NoError(t, <-errc)
		<-done
		insertPKs(t, records, seen)
		if cp := stream.GetLastCheckpoint(); cp.Text != "" {
			lastCp = cp
			offset = cp
		}
	}
	c.closeCDC()

	require.Len(t, seen, 20, "all rows from the written table must arrive")
	require.NotEmpty(t, lastCp.Text, "checkpoint must advance despite the quiet second table")
	cpHLC, err := decode.ParseHLC(lastCp.Text)
	require.NoError(t, err)
	require.Positive(t, cpHLC.Compare(t0HLC), "resolved checkpoint must advance past t0")
}

// --- 5: Close() during active PullRecords does not panic or leak goroutines ---

// TestIntegrationCockroachChaosCloseDuringPullNoLeak starts PullRecords, closes
// the connector while it is blocked, and asserts the call returns without panic
// and the changefeed pump goroutine does not leak.
func TestIntegrationCockroachChaosCloseDuringPullNoLeak(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, "crdb_chaos_close")
	f := newChaosFixture(t, ctx, config, "id INT PRIMARY KEY, v STRING")
	offset, err := f.c.GetLastOffset(ctx, f.flowName)
	require.NoError(t, err)

	baseline := runtime.NumGoroutine()

	mapping, schemaMapping := f.mapping()
	stream := model.NewCDCStream[model.RecordItems](1 << 12)
	req := &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName: f.flowName, RecordStream: stream,
		TableNameMapping: mapping, TableNameSchemaMapping: schemaMapping,
		LastOffset: offset, MaxBatchSize: 100, IdleTimeout: 60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errc <- fmt.Errorf("PullRecords panicked: %v", r)
			}
		}()
		errc <- f.c.PullRecords(ctx, shared.CatalogPool{}, f.otel, req)
	}()
	go func() {
		for range stream.GetRecords() {
		}
	}()

	// Let the feed establish, then close the connector mid-pull.
	time.Sleep(6 * time.Second)
	_ = f.c.Close()

	select {
	case perr := <-errc:
		require.NotContains(t, fmt.Sprintf("%v", perr), "panicked")
	case <-time.After(15 * time.Second):
		t.Fatal("PullRecords did not return within 15s after closeCDC")
	}

	// The pump goroutine should be gone; poll to allow scheduler cleanup.
	leaked := true
	for i := 0; i < 20; i++ {
		if runtime.NumGoroutine() <= baseline+2 {
			leaked = false
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.False(t, leaked, "changefeed pump goroutine leaked after close (baseline=%d, now=%d)", baseline, runtime.NumGoroutine())
}
