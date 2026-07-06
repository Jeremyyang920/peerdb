package conncockroach

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	// defaultResolvedIntervalSeconds is the changefeed `resolved`/
	// `min_checkpoint_frequency` used when the peer config leaves it unset. It
	// bounds how often a safe restart checkpoint (resolved HLC) is emitted, and
	// therefore the worst-case volume of duplicate rows replayed after a batch
	// that ended (on MaxBatchSize) before a resolved arrived.
	defaultResolvedIntervalSeconds = 10

	// cdcReconnectMaxAttempts bounds in-loop reconnect tries before PullRecords
	// returns the error and lets the SyncFlow retry the whole batch from the
	// persisted cursor.
	cdcReconnectMaxAttempts = 5
	cdcReconnectBaseBackoff = time.Second
	cdcReconnectMaxBackoff  = 30 * time.Second

	// rowChannelBuffer is the buffer between the pump goroutine (which owns the
	// blocking changefeed row iterator) and the PullRecords select loop.
	rowChannelBuffer = 1 << 14
)

// changefeedReservedOptions are WITH options the connector sets itself; a user
// may not override them via ChangefeedExtraOptions. Compared case-insensitively.
var changefeedReservedOptions = map[string]struct{}{
	"envelope":                 {},
	"diff":                     {},
	"updated":                  {},
	"mvcc_timestamp":           {},
	"resolved":                 {},
	"min_checkpoint_frequency": {},
	"cursor":                   {},
	"initial_scan":             {},
	"full_table_name":          {},
	"format":                   {},
}

// changefeedOptionKeyPattern guards ChangefeedExtraOptions keys against
// injection (they are interpolated into the WITH clause unquoted).
var changefeedOptionKeyPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// SetupReplication captures the CDC start position before the snapshot runs, the
// same role MySQL's SetupReplication plays with the master GTID/position. It
// records `cluster_logical_timestamp()` (t0, a CRDB HLC) as the flow's initial
// checkpoint. The snapshot (WP-C) reads t0 back via GetLastOffset for its
// AS OF SYSTEM TIME reads, and the changefeed then resumes from cursor=t0, so
// t0 MUST be persisted before the snapshot starts.
func (c *CockroachConnector) SetupReplication(
	ctx context.Context, catalogPool shared.CatalogPool, req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	var t0 string
	if err := c.conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()::STRING").Scan(&t0); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[cockroach] SetupReplication failed to capture cluster_logical_timestamp: %w", err)
	}
	// Validate it parses as an HLC so a malformed t0 fails here rather than later
	// when it is used as a changefeed cursor.
	if _, err := decode.ParseHLC(t0); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[cockroach] SetupReplication captured invalid HLC %q: %w", t0, err)
	}

	if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: t0}); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[cockroach] SetupReplication failed to persist initial checkpoint: %w", err)
	}
	c.logger.Info("[cockroach] SetupReplication captured initial checkpoint", slog.String("t0", t0))

	// Like MySQL, the checkpoint lives in the metadata store; no server-side
	// object (slot/job) is created for a sinkless changefeed.
	return model.SetupReplicationResult{}, nil
}

// SetupReplConn is a no-op. The sinkless changefeed needs the request's table
// set and cursor to start, which are not available here, so the persistent
// changefeed (see cdcReplState) is established lazily on the first PullRecords
// and then kept alive across batches until Close.
func (c *CockroachConnector) SetupReplConn(context.Context, map[string]string) error {
	return nil
}

// UpdateReplStateLastOffset persists the confirmed checkpoint (a resolved HLC)
// via the embedded metadata store, matching MySQL.
func (c *CockroachConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

// PullFlowCleanup has nothing to drop: a sinkless changefeed is a client-side
// streaming query with no server-side job, publication, or slot. The dedicated
// connection is owned by and closed within each PullRecords call.
func (c *CockroachConnector) PullFlowCleanup(context.Context, string) error {
	return nil
}

// ExportTxSnapshot / FinishExport are no-ops, like MySQL: snapshot consistency
// comes from AS OF SYSTEM TIME t0 (captured in SetupReplication), not from an
// exported transaction snapshot.
func (c *CockroachConnector) ExportTxSnapshot(context.Context, string, map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
	return nil, nil, nil
}

func (c *CockroachConnector) FinishExport(any) error {
	return nil
}

// changefeedRow is one raw message pumped off the blocking changefeed iterator.
// For resolved messages `table` is empty (the changefeed emits table=NULL).
type changefeedRow struct {
	table string
	value []byte
}

// tableResolver maps the (unqualified) table name a sinkless changefeed reports
// in its `table` column onto the mirror's source-table identifier and mapping.
// CRDB emits the bare table name (e.g. "orders") by default, whereas the mirror
// keys tables by their PeerDB source identifier (e.g. "public.orders"), so a
// direct map lookup misses; we index by the bare table name to bridge that.
type tableResolver struct {
	exact         map[string]model.NameAndExclude // source identifier -> mapping
	byBare        map[string]string               // bare table name -> source identifier
	ambiguousBare map[string]struct{}             // bare names shared by >1 source table
}

func newTableResolver(mapping map[string]model.NameAndExclude) tableResolver {
	r := tableResolver{
		exact:         mapping,
		byBare:        make(map[string]string, len(mapping)),
		ambiguousBare: make(map[string]struct{}),
	}
	for src := range mapping {
		bare := src
		if qt, err := common.ParseTableIdentifier(src); err == nil {
			bare = qt.Table
		} else if idx := strings.LastIndexByte(src, '.'); idx >= 0 {
			bare = src[idx+1:]
		}
		if existing, ok := r.byBare[bare]; ok && existing != src {
			r.ambiguousBare[bare] = struct{}{}
		} else {
			r.byBare[bare] = src
		}
	}
	return r
}

// resolve returns the source identifier and mapping for a changefeed `table`
// value. It accepts either the exact source identifier or the bare table name,
// and also tolerates a database.schema.table form (e.g. from full_table_name)
// by matching on the final component.
func (r tableResolver) resolve(changefeedTable string) (string, model.NameAndExclude, bool) {
	if nae, ok := r.exact[changefeedTable]; ok {
		return changefeedTable, nae, true
	}
	bare := changefeedTable
	if idx := strings.LastIndexByte(changefeedTable, '.'); idx >= 0 {
		bare = changefeedTable[idx+1:]
	}
	if _, bad := r.ambiguousBare[bare]; bad {
		return "", model.NameAndExclude{}, false
	}
	if src, ok := r.byBare[bare]; ok {
		return src, r.exact[src], true
	}
	return "", model.NameAndExclude{}, false
}

// changefeedTables returns the quoted table list for CREATE CHANGEFEED FOR
// TABLE, sorted for statement determinism.
func changefeedTables(mapping map[string]model.NameAndExclude) ([]string, error) {
	tables := make([]string, 0, len(mapping))
	for src := range mapping {
		qt, err := common.ParseTableIdentifier(src)
		if err != nil {
			return nil, fmt.Errorf("invalid source table identifier %q: %w", src, err)
		}
		tables = append(tables, qt.String())
	}
	sort.Strings(tables)
	return tables, nil
}

// buildChangefeedStatement assembles the sinkless CREATE CHANGEFEED statement.
// The baseline options (wrapped envelope, diff, updated, mvcc_timestamp,
// resolved/min_checkpoint_frequency, initial_scan='no') are fixed; the cursor is
// added when non-empty (always, post-SetupReplication); and any
// ChangefeedExtraOptions are appended after validating they don't collide with a
// managed option and have a safe key.
func buildChangefeedStatement(
	quotedTables []string, cursor string, resolvedSeconds uint32, extra map[string]string,
) (string, error) {
	if resolvedSeconds == 0 {
		resolvedSeconds = defaultResolvedIntervalSeconds
	}

	opts := []string{
		"envelope = 'wrapped'",
		"diff",
		"updated",
		"mvcc_timestamp",
		fmt.Sprintf("resolved = '%ds'", resolvedSeconds),
		fmt.Sprintf("min_checkpoint_frequency = '%ds'", resolvedSeconds),
		"initial_scan = 'no'",
	}
	if cursor != "" {
		opts = append(opts, fmt.Sprintf("cursor = '%s'", quoteLiteral(cursor)))
	}

	extraKeys := make([]string, 0, len(extra))
	for k := range extra {
		extraKeys = append(extraKeys, k)
	}
	sort.Strings(extraKeys)
	for _, k := range extraKeys {
		if _, reserved := changefeedReservedOptions[strings.ToLower(k)]; reserved {
			return "", fmt.Errorf("changefeed option %q is managed by the connector and cannot be overridden", k)
		}
		if !changefeedOptionKeyPattern.MatchString(k) {
			return "", fmt.Errorf("invalid changefeed option key %q", k)
		}
		if v := extra[k]; v == "" {
			opts = append(opts, k)
		} else {
			opts = append(opts, fmt.Sprintf("%s = '%s'", k, quoteLiteral(v)))
		}
	}

	return fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s WITH %s",
		strings.Join(quotedTables, ", "), strings.Join(opts, ", ")), nil
}

func quoteLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// newReplConn opens a dedicated pgwire connection for the changefeed. It sets
// results_buffer_size=0 so CRDB flushes rows (including resolved checkpoints) to
// the client immediately instead of buffering them server-side, which is
// required for a sinkless changefeed to make timely progress.
func (c *CockroachConnector) newReplConn(ctx context.Context) (*pgx.Conn, error) {
	connConfig, err := ParseConfig(c.connStr, c.config)
	if err != nil {
		return nil, err
	}
	connConfig.Config.RuntimeParams["timezone"] = "UTC"
	connConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	connConfig.Config.RuntimeParams["statement_timeout"] = "0"
	connConfig.Config.RuntimeParams["results_buffer_size"] = "0"
	return NewCockroachConnFromConfig(ctx, connConfig, c.ssh)
}

// cdcReplState is the persistent sinkless-changefeed stream, held on the
// connector across PullRecords calls. The changefeed query blocks forever, so a
// pump goroutine owns the row iterator and feeds a buffered channel while
// PullRecords drains it and slices batches at resolved checkpoints.
//
// Keeping one changefeed alive across batches (rather than recreating it each
// PullRecords) is deliberate: a freshly created changefeed pays an initial
// rangefeed catch-up plus closed-timestamp latency before its first resolved
// message, which for a low-traffic mirror can exceed the batch idle timeout —
// recreating per batch would then repeatedly return records without ever
// advancing the checkpoint, re-scanning from a stale cursor forever. A single
// long-lived changefeed pays that cost once. The pump binds to a
// connector-scoped context (not the per-batch PullRecords context), so it is
// torn down only on Close, a table-set change, or an unrecoverable error.
type cdcReplState struct {
	conn     *pgx.Conn
	cancel   context.CancelFunc
	rowCh    chan changefeedRow
	errCh    chan error
	tableSig string // sorted quoted-table list the changefeed was created for
	cursor   string // resume cursor for reconnect; advances to the latest resolved HLC
}

// closeCDC tears down the persistent changefeed stream if one is running. Safe
// to call repeatedly and concurrently; invoked by Close() at connector shutdown.
func (c *CockroachConnector) closeCDC() {
	c.replLock.Lock()
	defer c.replLock.Unlock()
	if c.cdc != nil {
		c.cdc.cancel()
		_ = c.cdc.conn.Close(context.Background())
		c.cdc = nil
	}
}

// startChangefeed opens a dedicated connection, issues the sinkless changefeed
// from the given cursor, and starts the pump goroutine, storing the result as
// the connector's persistent stream. The pump runs on a connector-scoped context
// so it survives across PullRecords batches.
func (c *CockroachConnector) startChangefeed(ctx context.Context, quotedTables []string, tableSig, cursor string) error {
	sqlStmt, err := buildChangefeedStatement(
		quotedTables, cursor, c.config.GetResolvedIntervalSeconds(), c.config.GetChangefeedExtraOptions())
	if err != nil {
		return err
	}
	conn, err := c.newReplConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to open changefeed connection: %w", err)
	}
	// streamCtx is connector-scoped (NOT the per-batch PullRecords ctx) so the
	// changefeed query survives across batches; it is canceled only by closeCDC
	// (Close / reconnect / table-set change).
	streamCtx, cancel := context.WithCancel(context.Background())
	s := &cdcReplState{
		conn:     conn,
		cancel:   cancel,
		rowCh:    make(chan changefeedRow, rowChannelBuffer),
		errCh:    make(chan error, 1),
		tableSig: tableSig,
		cursor:   cursor,
	}
	go func() {
		defer close(s.errCh)
		rows, err := conn.Query(streamCtx, sqlStmt)
		if err != nil {
			s.errCh <- fmt.Errorf("failed to start changefeed: %w", err)
			return
		}
		defer rows.Close()
		var table pgtype.Text
		var key, value []byte
		for rows.Next() {
			if err := rows.Scan(&table, &key, &value); err != nil {
				s.errCh <- fmt.Errorf("failed to scan changefeed row: %w", err)
				return
			}
			row := changefeedRow{value: slices.Clone(value)}
			if table.Valid {
				row.table = table.String
			}
			select {
			case s.rowCh <- row:
			case <-streamCtx.Done():
				return
			}
		}
		if err := rows.Err(); err != nil {
			s.errCh <- err
		}
	}()

	c.replLock.Lock()
	c.cdc = s
	c.replLock.Unlock()
	c.logger.Info("[cockroach] started sinkless changefeed",
		slog.String("cursor", cursor), slog.Int("tables", len(quotedTables)))
	return nil
}

// PullRecords drains the persistent sinkless changefeed into the request's
// CDCStream, cutting a batch at the first resolved message once records have
// been queued (or at MaxBatchSize, or IdleTimeout). The checkpoint (CDCStream
// text) advances only on resolved messages, which are the sole safe restart
// points; row events queued before a batch ends on MaxBatchSize (i.e. before a
// resolved) are replayed after a restart (at-least-once, idempotent on the
// ReplacingMergeTree destination).
//
// On connection loss the loop reconnects with backoff, resuming the changefeed
// from the latest resolved HLC seen. The changefeed itself is long-lived (see
// cdcReplState) and is not torn down on normal batch exit.
func (c *CockroachConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	resolver := newTableResolver(req.TableNameMapping)
	quotedTables, err := changefeedTables(req.TableNameMapping)
	if err != nil {
		return err
	}
	if len(quotedTables) == 0 {
		req.RecordStream.SignalAsEmpty()
		return nil
	}
	tableSig := strings.Join(quotedTables, ",")

	// (Re)establish the persistent changefeed when there is none, or when the
	// mirror's table set changed (e.g. add/remove tables). It resumes from the
	// persisted checkpoint (t0 from SetupReplication, or the last synced resolved
	// HLC); once running it continues across batches from its own position.
	if c.cdc == nil || c.cdc.tableSig != tableSig {
		c.closeCDC()
		startCursor := req.LastOffset.Text
		if startCursor == "" {
			c.logger.Warn("[cockroach] starting changefeed with empty cursor; will begin at statement time")
		}
		if err := c.startChangefeed(ctx, quotedTables, tableSig, startCursor); err != nil {
			return err
		}
	}

	startOffset := req.LastOffset.Text
	var recordCount uint32
	var totalBytes int64
	var latestResolved string
	pullStart := time.Now()

	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int64(otel_metrics.RowsInBatchKey, int64(recordCount)),
			attribute.Int64(otel_metrics.BytesPulledKey, totalBytes),
		)
		c.logger.Info("[cockroach] PullRecords batch finished",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", totalBytes),
			slog.String("latestResolved", latestResolved),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	}()

	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		recordCount += 1
		if err := req.RecordStream.AddRecord(ctx, record); err != nil {
			return err
		}
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
		}
		return nil
	}

	// Idle timer: end the batch when no row event arrives within IdleTimeout.
	idleTimer := time.NewTimer(req.IdleTimeout)
	defer idleTimer.Stop()
	resetIdle := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(req.IdleTimeout)
	}

	reconnectAttempts := 0
	reconnect := func(cause error) error {
		reconnectAttempts++
		resumeCursor := c.cdc.cursor
		if latestResolved != "" {
			resumeCursor = latestResolved
		}
		if reconnectAttempts > cdcReconnectMaxAttempts {
			c.closeCDC() // force a fresh rebuild on the next PullRecords call
			return fmt.Errorf("changefeed connection lost, exhausted %d reconnect attempts: %w", cdcReconnectMaxAttempts, cause)
		}
		backoff := min(cdcReconnectBaseBackoff*time.Duration(1<<(reconnectAttempts-1)), cdcReconnectMaxBackoff)
		c.logger.Warn("[cockroach] changefeed connection error, reconnecting",
			slog.Any("error", cause),
			slog.Int("attempt", reconnectAttempts),
			slog.String("resumeCursor", resumeCursor),
			slog.Duration("backoff", backoff),
			slog.String("note", "rows since last resolved will be replayed (at-least-once)"))
		c.closeCDC()
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := c.startChangefeed(ctx, quotedTables, tableSig, resumeCursor); err != nil {
			return err
		}
		resetIdle()
		return nil
	}

	for recordCount < req.MaxBatchSize {
		s := c.cdc
		select {
		case <-ctx.Done():
			// Batch context canceled (activity shutdown/failure). Leave the
			// changefeed running; Close() tears it down.
			c.logger.Info("[cockroach] PullRecords context canceled", slog.Any("error", ctx.Err()))
			return ctx.Err()

		case <-idleTimer.C:
			// No row events within IdleTimeout: end the batch. If nothing was queued
			// but the changefeed advanced past idle tables, persist the resolved HLC
			// directly — the no-records workflow path does not call
			// UpdateReplStateLastOffset, so an all-idle mirror would otherwise never
			// checkpoint. Mirrors MySQL's inactive-offset advance.
			if recordCount == 0 && latestResolved != "" && latestResolved != startOffset {
				if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: latestResolved}); err != nil {
					c.logger.Error("[cockroach] failed to persist inactive checkpoint", slog.Any("error", err))
				}
			}
			return nil

		case err := <-s.errCh:
			if err == nil {
				// Iterator ended without error (unexpected for an endless changefeed).
				err = fmt.Errorf("changefeed stream ended unexpectedly")
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if rerr := reconnect(err); rerr != nil {
				return rerr
			}

		case row := <-s.rowCh:
			totalBytes += int64(len(row.value))
			otelManager.Metrics.FetchedBytesCounter.Add(ctx, int64(len(row.value)))
			otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, int64(len(row.value)))

			event, err := decode.ParseEnvelope(row.value)
			if err != nil {
				return fmt.Errorf("failed to parse changefeed envelope: %w", err)
			}

			if event.Resolved {
				resolvedText := event.ResolvedHLC.String()
				req.RecordStream.UpdateLatestCheckpointText(resolvedText)
				latestResolved = resolvedText
				s.cursor = resolvedText // reconnect resumes from here
				reconnectAttempts = 0   // progress made; reset backoff ladder
				otelManager.Metrics.LatestConsumedLogEventGauge.Record(ctx, event.ResolvedHLC.Time().Unix())
				otelManager.Metrics.CommitLagGauge.Record(ctx, time.Since(event.ResolvedHLC.Time()).Microseconds())

				// A resolved message is a consistency high-water: every row event with
				// HLC <= it has already been emitted (and, since the pump preserves
				// stream order, already drained ahead of this message). So once we hold
				// records, cutting the batch here yields a checkpoint that exactly
				// covers them — no duplicates, bounded latency.
				if recordCount > 0 {
					return nil
				}
				continue
			}

			if err := c.processRowEvent(ctx, req, resolver, row.table, event, addRecord); err != nil {
				return err
			}
			resetIdle()
		}
	}

	return nil
}

// processRowEvent converts one decoded changefeed row event into an
// Insert/Update/Delete record and hands it to addRecord. Unmapped tables are
// skipped with a warning.
func (c *CockroachConnector) processRowEvent(
	ctx context.Context,
	req *model.PullRecordsRequest[model.RecordItems],
	resolver tableResolver,
	changefeedTable string,
	event decode.Event,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) error {
	sourceTableName, nae, ok := resolver.resolve(changefeedTable)
	if !ok {
		c.logger.Warn("[cockroach] skipping event for unmapped table", slog.String("table", changefeedTable))
		return nil
	}
	destinationTableName := nae.Name
	// TableNameSchemaMapping is keyed by destination table identifier (see
	// internal.BuildProcessedSchemaMapping), matching the MySQL connector.
	schema := req.TableNameSchemaMapping[destinationTableName]
	if schema == nil {
		c.logger.Warn("[cockroach] no schema for destination table, skipping",
			slog.String("sourceTable", sourceTableName), slog.String("destTable", destinationTableName))
		return nil
	}

	beforeItems, afterItems, err := decode.ChangefeedJSONToRecordItems(schema, event.Before, event.After)
	if err != nil {
		return fmt.Errorf("failed to decode row for table %s: %w", sourceTableName, err)
	}

	commitNano := event.Updated.Time().UnixNano()

	switch event.Operation {
	case decode.OpInsert, decode.OpRead:
		return addRecord(ctx, &model.InsertRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: commitNano},
			Items:                afterItems,
			SourceTableName:      sourceTableName,
			DestinationTableName: destinationTableName,
		})
	case decode.OpUpdate:
		return addRecord(ctx, &model.UpdateRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: commitNano},
			OldItems:             beforeItems,
			NewItems:             afterItems,
			SourceTableName:      sourceTableName,
			DestinationTableName: destinationTableName,
		})
	case decode.OpDelete:
		return addRecord(ctx, &model.DeleteRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: commitNano},
			Items:                beforeItems,
			SourceTableName:      sourceTableName,
			DestinationTableName: destinationTableName,
		})
	default:
		c.logger.Warn("[cockroach] skipping event with unknown operation",
			slog.String("table", sourceTableName), slog.String("op", event.Operation.String()))
		return nil
	}
}
