package conncockroach

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Snapshot / QRep pull for the CockroachDB source connector.
//
// CockroachDB speaks the Postgres wire protocol, so the expensive part of a
// snapshot read — turning pgx binary/text values into qvalue.QValue — is
// delegated to the Postgres connector rather than duplicated here (see
// decode/reuse.go for the reasoning). Two things are CRDB-specific and handled
// locally:
//
//  1. Consistency. Every snapshot read must observe the cluster exactly at the
//     changefeed cursor t₀ (cluster_logical_timestamp() captured by the CDC
//     SetupReplication before the snapshot). CRDB pins this with
//     `SET TRANSACTION AS OF SYSTEM TIME '<hlc>'` as the first statement of the
//     read transaction — it has no Postgres exported snapshots. We inject that
//     via the Postgres connector's SnapshotStatement hook, which both partition
//     computation and row pulls run as their first transaction statement.
//
//  2. Partitioning. The Postgres partition path relies on system catalog
//     functions CRDB does not implement (pg_relation_size, current_setting
//     ('block_size')) and only produces int/timestamp/TID ranges — no
//     string/UUID ranges. So partitions are computed here with plain
//     MIN/MAX/COUNT reads (which CRDB supports at AS OF SYSTEM TIME) and the
//     shared PartitionHelper, mirroring the MySQL connector's PK-based scheme
//     (including its UUID string-range partitioning).

var (
	uuidLowerRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	uuidUpperRe = regexp.MustCompile(`^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$`)
)

// snapshotClause builds the `SET TRANSACTION AS OF SYSTEM TIME` statement for a
// validated HLC. The HLC is re-rendered from its parsed integer components, so
// the string spliced into SQL can only ever be `<int>.<int>` — never
// attacker-controlled text.
func snapshotClause(t0 decode.HLC) string {
	return "SET TRANSACTION AS OF SYSTEM TIME '" + t0.String() + "'"
}

// resolveSnapshotTime reads the flow's snapshot point t₀ from the metadata store.
// The CDC SetupReplication persists cluster_logical_timestamp() there as the
// initial checkpoint before the snapshot runs, so every partition of every table
// reads the same consistent point that the changefeed later resumes from.
//
// It returns (t₀, true) when a valid HLC checkpoint exists. For a qrep-only
// mirror with no CDC (no checkpoint captured), it returns ok=false and the caller
// reads at the latest timestamp without AS OF SYSTEM TIME — a documented v1
// limitation, since a single activity cannot durably share a captured timestamp
// across the other partition activities.
func (c *CockroachConnector) resolveSnapshotTime(ctx context.Context, flowJobName string) (decode.HLC, bool) {
	if flowJobName == "" {
		return decode.HLC{}, false
	}
	offset, err := c.GetLastOffset(ctx, flowJobName)
	if err != nil {
		c.logger.Warn("[cockroach] failed to read snapshot checkpoint, reading at latest timestamp",
			slog.String("flowJobName", flowJobName), slog.Any("error", err))
		return decode.HLC{}, false
	}
	if strings.TrimSpace(offset.Text) == "" {
		c.logger.Warn("[cockroach] no snapshot checkpoint captured, reading at latest timestamp (qrep-only mirror)",
			slog.String("flowJobName", flowJobName))
		return decode.HLC{}, false
	}
	hlc, err := decode.ParseHLC(offset.Text)
	if err != nil {
		c.logger.Warn("[cockroach] snapshot checkpoint is not a valid HLC, reading at latest timestamp",
			slog.String("flowJobName", flowJobName), slog.String("checkpoint", offset.Text), slog.Any("error", err))
		return decode.HLC{}, false
	}
	return hlc, true
}

// postgresConfigFromCockroach maps the CockroachDB connection fields onto a
// PostgresConfig so the pgwire-compatible Postgres connector can be reused for
// the snapshot read + pgx->QValue decode.
func postgresConfigFromCockroach(config *protos.CockroachConfig) *protos.PostgresConfig {
	return &protos.PostgresConfig{
		Host:                 config.Host,
		Port:                 config.Port,
		User:                 config.User,
		Password:             config.Password,
		Database:             config.Database,
		TlsHost:              config.TlsHost,
		MetadataSchema:       config.MetadataSchema,
		SshConfig:            config.SshConfig,
		RootCa:               config.RootCa,
		RequireTls:           config.RequireTls,
		DisableTls:           config.DisableTls,
		SkipCertVerification: config.SkipCertVerification,
	}
}

// newDelegatePostgresConnector builds a Postgres connector over the CockroachDB
// cluster for a snapshot read, pinned to t₀ via AS OF SYSTEM TIME when available.
// The caller owns Close().
func (c *CockroachConnector) newDelegatePostgresConnector(
	ctx context.Context, env map[string]string, dstType protos.DBType, t0 decode.HLC, hasT0 bool,
) (*connpostgres.PostgresConnector, error) {
	pgConn, err := connpostgres.NewPostgresConnectorWithCDCDestination(
		ctx, env, postgresConfigFromCockroach(c.config), dstType)
	if err != nil {
		return nil, fmt.Errorf("failed to create delegate postgres connector for CockroachDB: %w", err)
	}
	if hasT0 {
		pgConn.SnapshotStatement = snapshotClause(t0)
	}
	return pgConn, nil
}

func (c *CockroachConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	c.logger.Info("[cockroach] evaluating default partition keys for parallel load")

	output := &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: make(map[string]string, len(input.TableMappings)),
	}
	for _, tm := range input.TableMappings {
		source := tm.SourceTableIdentifier
		schema, ok := input.TableSchemaMapping[source]
		if !ok {
			c.logger.Warn("[cockroach] table schema not found, defaulting to full table snapshot",
				slog.String("table", source))
			continue
		}
		if len(schema.PrimaryKeyColumns) == 0 {
			c.logger.Info("[cockroach] table has no primary key, defaulting to full table snapshot",
				slog.String("table", source))
			continue
		}
		// A composite primary key can't drive a single-column range partition.
		if len(schema.PrimaryKeyColumns) > 1 {
			c.logger.Info("[cockroach] table has a composite primary key, defaulting to full table snapshot",
				slog.String("table", source))
			continue
		}
		pkColumn := schema.PrimaryKeyColumns[0]
		var pkQKind types.QValueKind
		for _, col := range schema.Columns {
			if col.Name == pkColumn {
				pkQKind = types.QValueKind(col.Type)
				break
			}
		}
		if !supportsRangePartition(pkQKind) {
			c.logger.Info("[cockroach] primary key type does not support range partitioning, defaulting to full table snapshot",
				slog.String("table", source), slog.String("column", pkColumn), slog.String("qkind", string(pkQKind)))
			continue
		}
		c.logger.Info("[cockroach] using primary key as default partition key",
			slog.String("table", source), slog.String("column", pkColumn), slog.String("qkind", string(pkQKind)))
		output.TableDefaultPartitionKeyMapping[source] = pkColumn
	}
	return output, nil
}

// supportsRangePartition reports whether a watermark column of the given kind can
// drive range partitioning. String/UUID columns are accepted here and resolved to
// either UUID string-range partitions or a full-table fallback at partition time.
func supportsRangePartition(qkind types.QValueKind) bool {
	switch qkind {
	case types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32, types.QValueKindUInt64:
		return true
	case types.QValueKindDate, types.QValueKindTimestamp, types.QValueKindTimestampTZ:
		return true
	case types.QValueKindString, types.QValueKindUUID:
		return true
	default:
		return false
	}
}

// isStringWatermark reports whether the watermark column is read as a string
// (and therefore partitioned via UUID string-range detection rather than numeric
// or temporal ranges).
func isStringWatermark(qkind types.QValueKind) bool {
	return qkind == types.QValueKindString || qkind == types.QValueKindUUID
}

func (c *CockroachConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
		return utils.FullTablePartition(), nil
	}
	if config.NumPartitionsOverride == 0 && config.NumRowsPerPartition == 0 {
		return nil, errors.New("num rows per partition must be greater than 0")
	}

	watermarkKind, err := c.watermarkColumnKind(ctx, config)
	if err != nil {
		return nil, err
	}

	t0, hasT0 := c.resolveSnapshotTime(ctx, config.FlowJobName)

	parsedWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}
	quotedTable := parsedWatermarkTable.String()
	quotedColumn := common.QuoteIdentifier(config.WatermarkColumn)

	// Resume bound (`WHERE wm > $1`) for incremental QRep. String/UUID resume is
	// unsupported (matches the MySQL connector); the CDC snapshot uses
	// InitialCopyOnly, which never resumes.
	var resumeBound any
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			resumeBound = lastRange.IntRange.End
		case *protos.PartitionRange_UintRange:
			resumeBound = lastRange.UintRange.End
		case *protos.PartitionRange_TimestampRange:
			resumeBound = lastRange.TimestampRange.End.AsTime()
		case *protos.PartitionRange_StringRange:
			return nil, errors.New("resuming QRep by a string partition range is not supported for CockroachDB")
		case *protos.PartitionRange_NullRange:
			return nil, errors.New("unexpected null range in last partition after resuming QRep")
		default:
			return nil, fmt.Errorf("unknown last partition range type %T", lastRange)
		}
	}

	var whereClause string
	var queryArgs []any
	if resumeBound != nil {
		whereClause = " WHERE " + quotedColumn + " > $1"
		queryArgs = []any{resumeBound}
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	if err := c.withSnapshotTx(ctx, t0, hasT0, func(tx pgx.Tx) error {
		numPartitions := int64(config.NumPartitionsOverride)
		if numPartitions == 0 {
			var totalRows int64
			countQuery := "SELECT COUNT(*) FROM " + quotedTable + whereClause
			if err := tx.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows); err != nil {
				return fmt.Errorf("failed to count rows for partitioning: %w", err)
			}
			if totalRows == 0 {
				c.logger.Warn("[cockroach] no records to replicate, returning no partitions")
				return nil
			}
			adjusted := shared.AdjustNumPartitions(totalRows, int64(config.NumRowsPerPartition))
			c.logger.Info("[cockroach] partition details",
				slog.Int64("totalRows", totalRows),
				slog.Int64("desiredNumRowsPerPartition", int64(config.NumRowsPerPartition)),
				slog.Int64("adjustedNumPartitions", adjusted.AdjustedNumPartitions))
			numPartitions = adjusted.AdjustedNumPartitions
		}
		if numPartitions <= 0 {
			return nil
		}

		if isStringWatermark(watermarkKind) {
			return c.addStringPartitions(ctx, tx, partitionHelper, quotedTable, quotedColumn, whereClause, queryArgs, numPartitions)
		}
		return c.addRangePartitions(ctx, tx, partitionHelper, quotedTable, quotedColumn, whereClause, queryArgs, numPartitions)
	}); err != nil {
		return nil, err
	}

	if config.AddNullPartition {
		partitionHelper.AddNullPartition()
	}
	return partitionHelper.GetPartitions(), nil
}

// addRangePartitions computes MIN/MAX for a numeric or temporal watermark and
// uniformly splits the range.
func (c *CockroachConnector) addRangePartitions(
	ctx context.Context, tx pgx.Tx, ph *utils.PartitionHelper,
	quotedTable, quotedColumn, whereClause string, queryArgs []any, numPartitions int64,
) error {
	minMaxQuery := fmt.Sprintf("SELECT MIN(%[1]s), MAX(%[1]s) FROM %[2]s%[3]s", quotedColumn, quotedTable, whereClause)
	var minVal, maxVal any
	if err := tx.QueryRow(ctx, minMaxQuery, queryArgs...).Scan(&minVal, &maxVal); err != nil {
		return fmt.Errorf("failed to query min/max for partitioning: %w", err)
	}
	if minVal == nil || maxVal == nil {
		c.logger.Warn("[cockroach] watermark min/max is null, no partitions")
		return nil
	}
	if err := ph.AddPartitionsWithRange(normalizeBound(minVal), normalizeBound(maxVal), numPartitions); err != nil {
		return fmt.Errorf("failed to add range partitions: %w", err)
	}
	return nil
}

// addStringPartitions computes MIN/MAX for a string/UUID watermark; UUID-shaped
// bounds are split into UUID string ranges, anything else falls back to a single
// full-table partition (mirrors the MySQL connector).
func (c *CockroachConnector) addStringPartitions(
	ctx context.Context, tx pgx.Tx, ph *utils.PartitionHelper,
	quotedTable, quotedColumn, whereClause string, queryArgs []any, numPartitions int64,
) error {
	// Cast to string so UUID columns come back in canonical text form regardless
	// of the pgx type map.
	minMaxQuery := fmt.Sprintf("SELECT MIN(%[1]s)::string, MAX(%[1]s)::string FROM %[2]s%[3]s",
		quotedColumn, quotedTable, whereClause)
	var minVal, maxVal *string
	if err := tx.QueryRow(ctx, minMaxQuery, queryArgs...).Scan(&minVal, &maxVal); err != nil {
		return fmt.Errorf("failed to query min/max for string partitioning: %w", err)
	}
	if minVal == nil || maxVal == nil {
		c.logger.Warn("[cockroach] watermark min/max is null, no partitions")
		return nil
	}

	if isUUID, casing := detectUUIDWithHexCasing(*minVal, *maxVal); isUUID {
		uuidPartitions, err := buildUUIDStringPartitions(*minVal, *maxVal, casing, numPartitions)
		if err != nil {
			return fmt.Errorf("failed to build uuid string partitions: %w", err)
		}
		ph.AddPartitions(uuidPartitions)
		return nil
	}
	c.logger.Info("[cockroach] string watermark column is not uuid, falling back to full table partition")
	ph.AddPartitions(utils.FullTablePartition())
	return nil
}

// watermarkColumnKind resolves the QValueKind of the watermark column by
// introspecting the watermark table schema (reusing the WP-A schema path).
func (c *CockroachConnector) watermarkColumnKind(ctx context.Context, config *protos.QRepConfig) (types.QValueKind, error) {
	schemas, err := c.GetTableSchema(ctx, config.Env, config.Version, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: config.WatermarkTable}})
	if err != nil {
		return "", fmt.Errorf("failed to get schema for watermark table %s: %w", config.WatermarkTable, err)
	}
	schema, ok := schemas[config.WatermarkTable]
	if !ok {
		return "", fmt.Errorf("schema for watermark table %s not returned", config.WatermarkTable)
	}
	for _, col := range schema.Columns {
		if col.Name == config.WatermarkColumn {
			return types.QValueKind(col.Type), nil
		}
	}
	return "", fmt.Errorf("watermark column %s not found in table %s", config.WatermarkColumn, config.WatermarkTable)
}

// withSnapshotTx runs fn inside a read-only transaction pinned to t₀ via
// AS OF SYSTEM TIME (when available). It commits on success so the pinned read is
// released promptly.
func (c *CockroachConnector) withSnapshotTx(
	ctx context.Context, t0 decode.HLC, hasT0 bool, fn func(tx pgx.Tx) error,
) error {
	tx, err := c.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("failed to begin snapshot transaction: %w", err)
	}
	defer shared.RollbackTx(tx, c.logger)

	if hasT0 {
		if _, err := tx.Exec(ctx, snapshotClause(t0)); err != nil {
			return fmt.Errorf("failed to pin snapshot with AS OF SYSTEM TIME (t0 may be older than gc.ttlseconds): %w", err)
		}
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit snapshot transaction: %w", err)
	}
	return nil
}

// normalizeBound widens the smaller integer types pgx may return to int64 so the
// shared PartitionHelper can build integer ranges.
func normalizeBound(v any) any {
	switch x := v.(type) {
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	default:
		return v
	}
}

func (c *CockroachConnector) PullQRepRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	t0, hasT0 := c.resolveSnapshotTime(ctx, config.FlowJobName)

	pgConn, err := c.newDelegatePostgresConnector(ctx, config.Env, dstType, t0, hasT0)
	if err != nil {
		return 0, 0, err
	}
	defer pgConn.Close()

	// String (UUID) range partitions are not understood by the Postgres pull path,
	// so build that query here and drive the reused query executor directly. The
	// AS OF SYSTEM TIME pinning still comes from the SnapshotStatement hook.
	if partition.Range != nil {
		if stringRange, ok := partition.Range.Range.(*protos.PartitionRange_StringRange); ok {
			return c.pullStringRangePartition(ctx, pgConn, config, dstType, partition, stringRange.StringRange, stream)
		}
	}

	return pgConn.PullQRepRecords(ctx, catalogPool, otelManager, config, dstType, partition, stream)
}

// pullStringRangePartition pulls one UUID string-range partition by building the
// SELECT explicitly and delegating execution (and pgx->QValue decode) to the
// reused Postgres query executor.
func (c *CockroachConnector) pullStringRangePartition(
	ctx context.Context,
	pgConn *connpostgres.PostgresConnector,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stringRange *protos.StringPartitionRange,
	stream *model.QRecordStream,
) (int64, int64, error) {
	if config.Query != "" {
		return 0, 0, errors.New("custom query is not supported with string range partitions for CockroachDB")
	}

	parsedTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse source table: %w", err)
	}
	selectedColumns, err := c.selectedColumns(ctx, config)
	if err != nil {
		return 0, 0, err
	}
	quotedColumn := common.QuoteIdentifier(config.WatermarkColumn)

	upperOp := "<"
	if stringRange.EndInclusive {
		upperOp = "<="
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= $1 AND %s %s $2",
		selectedColumns, parsedTable.String(), quotedColumn, quotedColumn, upperOp)

	executor, err := pgConn.NewQRepQueryExecutorSnapshot(
		ctx, config.Env, config.Version, config.SnapshotName, config.FlowJobName, partition.PartitionId)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create query executor: %w", err)
	}

	numRecords, numBytes, err := executor.ExecuteQueryIntoSink(
		ctx,
		connpostgres.RecordStreamSink{QRecordStream: stream, DestinationType: dstType},
		query, stringRange.Start, stringRange.End)
	if err != nil {
		return numRecords, numBytes, err
	}
	c.logger.Info("[cockroach] pulled string range partition",
		slog.String(string(shared.PartitionIDKey), partition.PartitionId),
		slog.Int64("records", numRecords), slog.Int64("bytes", numBytes))
	return numRecords, numBytes, nil
}

// selectedColumns builds the SELECT column list for the watermark table, honoring
// config.Exclude. Returns "*" when nothing is excluded.
func (c *CockroachConnector) selectedColumns(ctx context.Context, config *protos.QRepConfig) (string, error) {
	if len(config.Exclude) == 0 {
		return "*", nil
	}
	schemas, err := c.GetTableSchema(ctx, config.Env, config.Version, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: config.WatermarkTable}})
	if err != nil {
		return "", fmt.Errorf("failed to get schema for watermark table %s: %w", config.WatermarkTable, err)
	}
	schema := schemas[config.WatermarkTable]
	quotedColumns := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if !slices.Contains(config.Exclude, col.Name) {
			quotedColumns = append(quotedColumns, common.QuoteIdentifier(col.Name))
		}
	}
	return strings.Join(quotedColumns, ","), nil
}

// --- UUID string-range partitioning (mirrors the MySQL connector's scheme so
// CockroachDB's common gen_random_uuid() primary keys load in parallel) ---

type hexCasing int

const (
	hexCasingUnknown hexCasing = iota
	hexCasingLower
	hexCasingUpper
)

// detectUUIDWithHexCasing best-effort classifies a string watermark by inspecting
// only its min and max bounds: whether both are canonical UUIDs and, if so, their
// shared hex-letter casing. Because only the bounds are examined, misclassification
// is possible (e.g. non-UUID rows between UUID-shaped bounds) but only ever causes
// partition skew, never incorrect data.
func detectUUIDWithHexCasing(minVal string, maxVal string) (bool, hexCasing) {
	switch {
	case uuidLowerRe.MatchString(minVal) && uuidLowerRe.MatchString(maxVal):
		return true, hexCasingLower
	case uuidUpperRe.MatchString(minVal) && uuidUpperRe.MatchString(maxVal):
		return true, hexCasingUpper
	default:
		return false, hexCasingUnknown
	}
}

// buildUUIDStringPartitions splits a UUID string watermark into partitions. The
// original min/max are used for the first/last bounds to preserve correctness.
func buildUUIDStringPartitions(
	minVal string, maxVal string, casing hexCasing, numPartitions int64,
) ([]*protos.QRepPartition, error) {
	minInt, err := uuidToBigInt(minVal)
	if err != nil {
		return nil, fmt.Errorf("failed to convert min uuid to bigint: %w", err)
	}
	maxInt, err := uuidToBigInt(maxVal)
	if err != nil {
		return nil, fmt.Errorf("failed to convert max uuid to bigint: %w", err)
	}
	if minInt.Cmp(maxInt) > 0 {
		return nil, fmt.Errorf("min uuid (%s) greater than max uuid (%s)", minVal, maxVal)
	}

	var partitions []*protos.QRepPartition
	start := minVal
	step := shared.BigIntDivCeil(new(big.Int).Sub(maxInt, minInt), big.NewInt(numPartitions))
	if step.Sign() == 0 {
		step = big.NewInt(1)
	}
	for value := new(big.Int).Add(minInt, step); value.Cmp(maxInt) < 0; value.Add(value, step) {
		end, err := bigIntToUUID(value, casing)
		if err != nil {
			return nil, fmt.Errorf("failed to convert bigint to uuid: %w", err)
		}
		partitions = append(partitions, createStringPartition(start, end, false))
		start = end
	}
	partitions = append(partitions, createStringPartition(start, maxVal, true))
	return partitions, nil
}

func uuidToBigInt(s string) (*big.Int, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(u[:]), nil
}

func bigIntToUUID(n *big.Int, casing hexCasing) (string, error) {
	if n.BitLen() > 128 {
		return "", fmt.Errorf("value does not fit in a UUID (%d bits)", n.BitLen())
	}
	var b [16]byte
	n.FillBytes(b[:])
	s := uuid.UUID(b).String()
	if casing == hexCasingUpper {
		s = strings.ToUpper(s)
	}
	return s, nil
}

func createStringPartition(start string, end string, endInclusive bool) *protos.QRepPartition {
	return &protos.QRepPartition{
		PartitionId: uuid.NewString(),
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_StringRange{
				StringRange: &protos.StringPartitionRange{
					Start:        start,
					End:          end,
					EndInclusive: endInclusive,
				},
			},
		},
	}
}
