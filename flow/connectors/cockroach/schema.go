package conncockroach

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// systemSchemas are the schemas CockroachDB exposes over pgwire that are not
// user data: the pgwire compatibility catalogs plus CRDB's own internal schema.
// They are excluded from browse/introspection results.
const systemSchemaFilter = `('crdb_internal', 'pg_extension', 'information_schema', 'pg_catalog')`

// qkindForColumn maps a column's pgwire type OID (and, as a fallback, its
// human-readable type name) to a QValueKind. CockroachDB reports standard
// PostgreSQL OIDs for its built-in types, so the OID lookup succeeds for the
// common case; the type-name fallback covers types whose OID we don't recognise
// but whose name we do (e.g. GEOMETRY/GEOGRAPHY). Anything still unrecognised
// (user-defined enums, etc.) decodes as a string.
func qkindForColumn(oid uint32, formatType string) types.QValueKind {
	if kind, ok := decode.KindFromPostgresOID(oid); ok {
		return kind
	}
	if kind, ok := decode.KindFromTypeName(formatType); ok {
		return kind
	}
	return types.QValueKindString
}

func (c *CockroachConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rows, err := c.conn.Query(ctx,
		`SELECT table_schema || '.' || table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN `+systemSchemaFilter+`
			AND table_type = 'BASE TABLE'`)
	if err != nil {
		return nil, err
	}

	tables, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.AllTablesResponse{Tables: tables}, nil
}

func (c *CockroachConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	rows, err := c.conn.Query(ctx,
		`SELECT schema_name FROM information_schema.schemata
		WHERE schema_name NOT IN `+systemSchemaFilter+` ORDER BY schema_name`)
	if err != nil {
		return nil, err
	}

	schemas, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
}

func (c *CockroachConnector) GetTablesInSchema(
	ctx context.Context, schema string, cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	// can_mirror reflects whether the table has an EXPLICIT (non-hidden) primary
	// key. A CRDB table without a PK gets an implicit index on a hidden `rowid`
	// column, which changefeeds and QRep partitioning cannot key on, so such
	// tables are not mirrorable.
	rows, err := c.conn.Query(ctx, `SELECT c.relname,
		EXISTS (
			SELECT 1 FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = c.oid AND i.indisprimary AND NOT a.attishidden
		) AS has_pk
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind = 'r'
		ORDER BY c.relname`, schema)
	if err != nil {
		c.logger.Info("failed to fetch tables", slog.Any("error", err))
		return nil, err
	}

	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.TableResponse, error) {
		var table pgtype.Text
		var hasPk pgtype.Bool
		if err := row.Scan(&table, &hasPk); err != nil {
			return nil, err
		}
		// CockroachDB has no pg_size_pretty/pg_total_relation_size; leave the
		// size display empty rather than mislabel row counts as bytes.
		return &protos.TableResponse{
			TableName: table.String,
			CanMirror: !cdcEnabled || hasPk.Bool,
			TableSize: "",
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return &protos.SchemaTablesResponse{Tables: tables}, nil
}

func (c *CockroachConnector) GetColumns(
	ctx context.Context, version uint32, schema string, table string,
) (*protos.TableColumnsResponse, error) {
	rows, err := c.conn.Query(ctx, `SELECT
		a.attname AS column_name,
		a.atttypid AS oid,
		format_type(a.atttypid, a.atttypmod) AS data_type,
		EXISTS (
			SELECT 1 FROM pg_index i
			WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey)
		) AS is_primary_key
	FROM pg_attribute a
	JOIN pg_class c ON a.attrelid = c.oid
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE n.nspname = $1 AND c.relname = $2
		AND a.attnum > 0 AND NOT a.attisdropped AND NOT a.attishidden
	ORDER BY a.attnum`, schema, table)
	if err != nil {
		return nil, err
	}

	columns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.ColumnsItem, error) {
		var columnName pgtype.Text
		var oid uint32
		var datatype pgtype.Text
		var isPkey pgtype.Bool
		if err := row.Scan(&columnName, &oid, &datatype, &isPkey); err != nil {
			return nil, err
		}
		return &protos.ColumnsItem{
			Name:  columnName.String,
			Type:  datatype.String,
			IsKey: isPkey.Bool,
			Qkind: string(qkindForColumn(oid, datatype.String)),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}

func (c *CockroachConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for _, tm := range tableMappings {
		tableSchema, err := c.getTableSchemaForTable(ctx, env, tm, system)
		if err != nil {
			c.logger.Info("error fetching schema", slog.String("table", tm.SourceTableIdentifier), slog.Any("error", err))
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
		c.logger.Info("fetched schema", slog.String("table", tm.SourceTableIdentifier))
	}
	return res, nil
}

func (c *CockroachConnector) getTableSchemaForTable(
	ctx context.Context,
	env map[string]string,
	tm *protos.TableMapping,
	system protos.TypeSystem,
) (*protos.TableSchema, error) {
	schemaTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
	if err != nil {
		return nil, err
	}

	relID, err := c.getRelIDForTable(ctx, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] failed to get relation id for table %s: %w", schemaTable, err)
	}

	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	excluded := make(map[string]struct{}, len(tm.Exclude))
	for _, col := range tm.Exclude {
		excluded[col] = struct{}{}
	}

	// Column names/types/nullability. Hidden columns (e.g. the implicit `rowid`
	// backing a PK-less table) and dropped columns are excluded.
	rows, err := c.conn.Query(ctx, `SELECT
		a.attname,
		a.atttypid,
		format_type(a.atttypid, a.atttypmod) AS data_type,
		a.atttypmod,
		NOT a.attnotnull AS nullable
	FROM pg_attribute a
	JOIN pg_class c ON a.attrelid = c.oid
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE n.nspname = $1 AND c.relname = $2
		AND a.attnum > 0 AND NOT a.attisdropped AND NOT a.attishidden
	ORDER BY a.attnum`, schemaTable.Namespace, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("error getting columns for table %s: %w", schemaTable, err)
	}

	columns := make([]*protos.FieldDescription, 0)
	var attname, dataType string
	var oid uint32
	var typmod int32
	var nullable bool
	if _, err := pgx.ForEachRow(rows, []any{&attname, &oid, &dataType, &typmod, &nullable}, func() error {
		if _, ok := excluded[attname]; ok {
			return nil
		}
		var colType string
		switch system {
		case protos.TypeSystem_Q:
			colType = string(qkindForColumn(oid, dataType))
		case protos.TypeSystem_PG:
			colType = dataType
		default:
			colType = string(qkindForColumn(oid, dataType))
		}
		columns = append(columns, &protos.FieldDescription{
			Name:         attname,
			Type:         colType,
			TypeModifier: typmod,
			Nullable:     nullable,
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("error scanning columns for table %s: %w", schemaTable, err)
	}

	pKeyCols, err := c.getPrimaryKeyColumns(ctx, relID)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] error getting primary key columns for table %s: %w", schemaTable, err)
	}

	return &protos.TableSchema{
		TableIdentifier:       tm.SourceTableIdentifier,
		PrimaryKeyColumns:     pKeyCols,
		IsReplicaIdentityFull: false,
		Columns:               columns,
		NullableEnabled:       nullableEnabled,
		System:                system,
		TableOid:              relID,
	}, nil
}

// getRelIDForTable returns the pg_class OID for a table, mapping a missing
// table to shared.ErrTableDoesNotExist.
func (c *CockroachConnector) getRelIDForTable(ctx context.Context, schemaTable *common.QualifiedTable) (uint32, error) {
	var relID pgtype.Uint32
	if err := c.conn.QueryRow(ctx,
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`,
		schemaTable.Namespace, schemaTable.Table).Scan(&relID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, shared.ErrTableDoesNotExist
		}
		return 0, fmt.Errorf("error getting relation ID for table %s: %w", schemaTable, err)
	}
	return relID.Uint32, nil
}

// getPrimaryKeyColumns returns the EXPLICIT primary key columns for a table, in
// key order. Hidden columns are excluded, so a PK-less table (whose implicit PK
// is on the hidden `rowid`) returns an empty slice.
func (c *CockroachConnector) getPrimaryKeyColumns(ctx context.Context, relID uint32) ([]string, error) {
	rows, err := c.conn.Query(ctx,
		`SELECT a.attname FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1 AND i.indisprimary AND NOT a.attishidden
		ORDER BY array_position(i.indkey, a.attnum::int2)`, relID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows[string](rows, pgx.RowTo)
}

// EnsurePullability maps each requested source table to its relation OID (used
// by SetupFlow's SrcTableIdNameMapping) and, when constraints are checked,
// verifies the table has an explicit primary key. CockroachDB tables without an
// explicit PK are keyed on a hidden `rowid`, which cannot be replicated.
func (c *CockroachConnector) EnsurePullability(
	ctx context.Context, req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	tableIdentifierMapping := make(map[string]*protos.PostgresTableIdentifier, len(req.SourceTableIdentifiers))
	for _, tableName := range req.SourceTableIdentifiers {
		schemaTable, err := common.ParseTableIdentifier(tableName)
		if err != nil {
			return nil, fmt.Errorf("error parsing schema and table: %w", err)
		}

		relID, err := c.getRelIDForTable(ctx, schemaTable)
		if err != nil {
			return nil, err
		}
		tableIdentifierMapping[tableName] = &protos.PostgresTableIdentifier{RelId: relID}

		if !req.CheckConstraints {
			c.logger.Info("[no-constraints] ensured pullability table " + tableName)
			continue
		}

		pKeyCols, err := c.getPrimaryKeyColumns(ctx, relID)
		if err != nil {
			return nil, fmt.Errorf("error getting primary key columns for table %s: %w", schemaTable, err)
		}
		if len(pKeyCols) == 0 {
			return nil, exceptions.NewMissingPrimaryKeyError(schemaTable.String())
		}
	}

	return &protos.EnsurePullabilityBatchOutput{TableIdentifierMapping: tableIdentifierMapping}, nil
}
