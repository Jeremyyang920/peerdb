package conncockroach

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// --- pure unit tests (no CockroachDB needed) ---

func TestProtectionDescriptionBuilding(t *testing.T) {
	desc, err := protectionDescription("crdb_to_ch_orders")
	require.NoError(t, err)
	require.Equal(t, "peerdb crdb_to_ch_orders", desc)

	full, err := fullProtectionDescription("crdb_to_ch_orders")
	require.NoError(t, err)
	require.Equal(t, "History Retention for peerdb crdb_to_ch_orders", full)
	// The full form must be exactly prefix+namespace+flow so the SHOW JOBS lookup
	// (which matches on the stored description) round-trips what protect_mvcc_history
	// records.
	require.True(t, strings.HasSuffix(full, "crdb_to_ch_orders"))

	// An empty flow name would create an unfindable job; reject it up front.
	_, err = protectionDescription("")
	require.Error(t, err)
	_, err = fullProtectionDescription("")
	require.Error(t, err)
}

func TestValidateProtectionWindowAndInterval(t *testing.T) {
	require.NoError(t, validateProtectionWindow(time.Second))
	require.NoError(t, validateProtectionWindow(24*time.Hour))
	require.Error(t, validateProtectionWindow(0))
	require.Error(t, validateProtectionWindow(-time.Second))

	require.Equal(t, "86400s", windowInterval(24*time.Hour))
	require.Equal(t, "2s", windowInterval(2*time.Second))
	require.Equal(t, "30s", windowInterval(30*time.Second))
}

func TestClassifyProtectionError(t *testing.T) {
	require.NoError(t, classifyProtectionError(nil))

	// Missing builtin (older CockroachDB) → unsupported.
	unknownFn := &pgconn.PgError{Code: "42883", Message: "unknown function: crdb_internal.protect_mvcc_history()"}
	require.ErrorIs(t, classifyProtectionError(unknownFn), errProtectionUnsupported)

	// Lacking the REPLICATION privilege → unsupported (degrade, don't fail).
	noPriv := &pgconn.PgError{Code: "42501", Message: "user does not have REPLICATION privilege"}
	require.ErrorIs(t, classifyProtectionError(noPriv), errProtectionUnsupported)

	// The original error is preserved in the chain for logging.
	require.ErrorContains(t, classifyProtectionError(unknownFn), "protect_mvcc_history")

	// An unrelated failure is passed through unchanged (NOT unsupported).
	other := &pgconn.PgError{Code: "40001", Message: "restart transaction"}
	got := classifyProtectionError(other)
	require.NotErrorIs(t, got, errProtectionUnsupported)
	require.Equal(t, other, got)

	// A plain (non-pg) error is passed through and is not unsupported.
	plain := errors.New("connection reset")
	require.NotErrorIs(t, classifyProtectionError(plain), errProtectionUnsupported)
}

func TestProtectionRemediation(t *testing.T) {
	// Missing builtin → version-upgrade guidance.
	require.Contains(t,
		protectionRemediation(&pgconn.PgError{Code: "42883", Message: "unknown function"}),
		"v23.2+")

	// 42501 from the unsafesql restriction (SET did not apply) → session/version
	// guidance, NOT a REPLICATION grant (the lead's v26.1 distinction).
	restricted := protectionRemediation(&pgconn.PgError{
		Code: "42501", Message: "Access to crdb_internal and system is restricted",
	})
	require.Contains(t, restricted, "allow_unsafe_internals")
	require.NotContains(t, restricted, "GRANT SYSTEM REPLICATION")

	// 42501 without the restriction message → REPLICATION privilege guidance.
	noPriv := protectionRemediation(&pgconn.PgError{
		Code: "42501", Message: "user bob does not have REPLICATION privilege",
	})
	require.Contains(t, noPriv, "GRANT SYSTEM REPLICATION")
}

func TestHistoryProtectionWindowConfig(t *testing.T) {
	// Absent field → default 24h, enabled.
	c := &CockroachConnector{config: &protos.CockroachConfig{}}
	window, enabled := c.historyProtectionWindow()
	require.True(t, enabled)
	require.Equal(t, 24*time.Hour, window)

	// Explicit 0 → disabled.
	zero := uint32(0)
	c = &CockroachConnector{config: &protos.CockroachConfig{HistoryProtectionWindowSeconds: &zero}}
	_, enabled = c.historyProtectionWindow()
	require.False(t, enabled)

	// Explicit non-zero → that many seconds, enabled.
	n := uint32(3600)
	c = &CockroachConnector{config: &protos.CockroachConfig{HistoryProtectionWindowSeconds: &n}}
	window, enabled = c.historyProtectionWindow()
	require.True(t, enabled)
	require.Equal(t, time.Hour, window)
}

// --- live integration tests (gated on CI_COCKROACH_HOST) ---
//
// NOTE ON THE DROPPED "GC-HOLD PROOF" TEST: an end-to-end proof that protection
// lets an AS OF SYSTEM TIME t₀ read survive aggressive GC was attempted but not
// kept, because it could not be made deterministic on the single-node dev
// container. Even after setting gc.ttlseconds=1, waiting for zone-config
// propagation, generating superseded/deleted versions, and repeatedly enqueuing
// the range into the mvccGC queue via crdb_internal.kv_enqueue_replica, the GC
// threshold did not reliably advance past t₀ — the UNPROTECTED control read kept
// succeeding, so the test could not distinguish protected from unprotected
// behavior and would give false confidence. The protection mechanics are instead
// proven deterministically below: the protected-timestamp record is created,
// extended, released on demand, and auto-expires, all observed directly in
// system.protected_ts_records and SHOW JOBS.

// countProtectionPTS counts the live protected-timestamp records that reference
// the given history-retention job. The job id is stored in the record's `meta`
// as its UTF8 text form. Needs allow_unsafe_internals on the querying session.
func countProtectionPTS(t *testing.T, ctx context.Context, conn *pgx.Conn, jobID int64) int {
	t.Helper()
	enableUnsafeInternals(ctx, conn)
	var n int
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count(*) FROM system.protected_ts_records WHERE meta_type = 'jobs' AND convert_from(meta, 'UTF8') = $1",
		fmt.Sprintf("%d", jobID)).Scan(&n))
	return n
}

// pollUntil calls fn until it returns true or the deadline elapses, failing the
// test otherwise. Used for CockroachDB's asynchronous state transitions (a
// canceled job reverts before its PTS record disappears; a tiny-window job
// expires on its own).
func pollUntil(t *testing.T, timeout time.Duration, msg string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Second)
	}
	require.Fail(t, "poll timed out", msg)
}

// currentHLC reads a fresh cluster_logical_timestamp() as a parsed HLC.
func currentHLC(t *testing.T, ctx context.Context, conn *pgx.Conn) decode.HLC {
	t.Helper()
	var s string
	require.NoError(t, conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()::STRING").Scan(&s))
	hlc, err := decode.ParseHLC(s)
	require.NoError(t, err)
	return hlc
}

func TestIntegrationCockroachProtectionLifecycle(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.Background()
	c := newIntegrationConnector(t, ctx, config)

	flow := "crdb_prot_life_" + strings.ToLower(common.RandomString(6))
	t0 := currentHLC(t, ctx, c.conn)

	// protectHistory creates a job discoverable via SHOW JOBS + a PTS record.
	jobID, err := protectHistory(ctx, c.conn, t0, time.Hour, flow)
	require.NoError(t, err)
	require.NotZero(t, jobID)
	t.Cleanup(func() { _ = cancelProtectionByFlow(context.Background(), c.conn, flow) })

	foundID, found, err := findProtectionJob(ctx, c.conn, flow)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, jobID, foundID)
	require.Equal(t, 1, countProtectionPTS(t, ctx, c.conn, jobID), "a PTS record should exist while protection is live")

	// extend succeeds against the running job.
	require.NoError(t, extendProtection(ctx, c.conn, jobID))

	// cancelProtectionByFlow removes it. Cleanup is async (canceled → reverting →
	// PTS record gone), so poll rather than asserting immediately.
	require.NoError(t, cancelProtectionByFlow(ctx, c.conn, flow))
	// The job leaves the running set promptly (cancel-requested), so it is no
	// longer "found" for extension almost immediately.
	pollUntil(t, 30*time.Second, "protection job should leave the active set after cancel", func() bool {
		_, stillFound, ferr := findProtectionJob(ctx, c.conn, flow)
		require.NoError(t, ferr)
		return !stillFound
	})
	// The PTS record itself disappears only after the job finishes reverting.
	pollUntil(t, 2*time.Minute, "PTS record should be removed after async cancel cleanup", func() bool {
		return countProtectionPTS(t, ctx, c.conn, jobID) == 0
	})

	// Double-cancel and cancel-of-nonexistent are both no-ops.
	require.NoError(t, cancelProtectionByFlow(ctx, c.conn, flow))
	require.NoError(t, cancelProtectionByFlow(ctx, c.conn, "crdb_prot_nonexistent_"+strings.ToLower(common.RandomString(6))))
}

func TestIntegrationCockroachProtectionAutoExpiry(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	ctx := context.Background()
	c := newIntegrationConnector(t, ctx, config)

	flow := "crdb_prot_expiry_" + strings.ToLower(common.RandomString(6))
	t0 := currentHLC(t, ctx, c.conn)

	// A tiny window must auto-expire and clear its PTS record without any cancel,
	// so a degraded PeerDB (that never extends) never pins GC indefinitely.
	jobID, err := protectHistory(ctx, c.conn, t0, 2*time.Second, flow)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cancelProtectionByFlow(context.Background(), c.conn, flow) })

	pollUntil(t, 90*time.Second, "tiny-window protection should auto-expire and drop its PTS record", func() bool {
		return countProtectionPTS(t, ctx, c.conn, jobID) == 0
	})
}

func TestIntegrationCockroachProtectionSetupAndRelease(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	// Enable protection with a 1h window for this mirror.
	window := uint32(3600)
	config.HistoryProtectionWindowSeconds = &window

	flow := "crdb_prot_setup_" + strings.ToLower(common.RandomString(6))
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, flow)
	c := newIntegrationConnector(t, ctx, config)

	tableName := "crdb_prot_" + strings.ToLower(common.RandomString(6))
	srcTable := "public." + tableName
	dstTable := "dst_" + tableName
	qualified := `"public"."` + tableName + `"`
	_, err := c.conn.Exec(ctx, "CREATE TABLE "+qualified+" (id INT PRIMARY KEY, name STRING)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = c.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified) })
	t.Cleanup(func() { _ = cancelProtectionByFlow(context.Background(), c.conn, flow) })

	// SetupReplication captures t0 AND creates the protection job.
	_, err = c.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{FlowJobName: flow})
	require.NoError(t, err)
	require.NoError(t, c.SetupReplConn(ctx, nil))

	_, found, err := findProtectionJob(ctx, c.conn, flow)
	require.NoError(t, err)
	require.True(t, found, "SetupReplication should have created a protection job for the flow")

	t0, err := c.GetLastOffset(ctx, flow)
	require.NoError(t, err)
	require.NotEmpty(t, t0.Text)

	// DML after t0 so the changefeed has something to emit, then a resolved.
	_, err = c.conn.Exec(ctx, "INSERT INTO "+qualified+" (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

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
		FlowJobName:            flow,
		RecordStream:           stream,
		TableNameMapping:       map[string]model.NameAndExclude{srcTable: model.NewNameAndExclude(dstTable, nil)},
		TableNameSchemaMapping: map[string]*protos.TableSchema{dstTable: tableSchema},
		LastOffset:             t0,
		MaxBatchSize:           100,
		IdleTimeout:            30 * time.Second,
	}

	pullErr := make(chan error, 1)
	go func() { pullErr <- c.PullRecords(ctx, shared.CatalogPool{}, otelManager, req) }()
	go func() {
		for range stream.GetRecords() { //nolint:revive
		}
	}()
	require.NoError(t, <-pullErr)

	// Having advanced past t0 on a resolved, the connector should have released
	// the protection (cancel accepted → job leaves the active set).
	pollUntil(t, 30*time.Second, "protection should be released once CDC advances past t0", func() bool {
		_, stillFound, ferr := findProtectionJob(ctx, c.conn, flow)
		require.NoError(t, ferr)
		return !stillFound
	})
}

func TestIntegrationCockroachProtectionCleanup(t *testing.T) {
	config := cockroachTestConfig(t)
	if config == nil {
		t.Skip("CI_COCKROACH_HOST not set; skipping CockroachDB integration test")
	}
	window := uint32(3600)
	config.HistoryProtectionWindowSeconds = &window

	flow := "crdb_prot_cleanup_" + strings.ToLower(common.RandomString(6))
	ctx := context.WithValue(context.Background(), shared.FlowNameKey, flow)
	c := newIntegrationConnector(t, ctx, config)
	t.Cleanup(func() { _ = cancelProtectionByFlow(context.Background(), c.conn, flow) })

	// SetupReplication creates the protection job; PullFlowCleanup (mirror drop)
	// must cancel it even when CDC never reached a resolved.
	_, err := c.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{FlowJobName: flow})
	require.NoError(t, err)

	_, found, err := findProtectionJob(ctx, c.conn, flow)
	require.NoError(t, err)
	require.True(t, found)

	require.NoError(t, c.PullFlowCleanup(ctx, flow))
	pollUntil(t, 30*time.Second, "PullFlowCleanup should cancel the protection job", func() bool {
		_, stillFound, ferr := findProtectionJob(ctx, c.conn, flow)
		require.NoError(t, ferr)
		return !stillFound
	})

	// Cleanup again is a no-op.
	require.NoError(t, c.PullFlowCleanup(ctx, flow))
}
