package conncockroach

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// Minimum CockroachDB version PeerDB supports as a source. The sinkless
// changefeed envelope options the CDC path relies on (resolved timestamps,
// diff, updated) are stable from v24.1 onward.
const (
	minCRDBMajorVersion = 24
	minCRDBMinorVersion = 1
)

// crdbVersionRegex extracts the major.minor from the version() string, e.g.
// "CockroachDB CCL v25.4.12 (aarch64-...)" -> 25, 4.
var crdbVersionRegex = regexp.MustCompile(`CockroachDB\s+(?:\S+\s+)?v(\d+)\.(\d+)`)

func parseCRDBVersion(version string) (int, int, error) {
	m := crdbVersionRegex.FindStringSubmatch(version)
	if m == nil {
		return 0, 0, fmt.Errorf("unrecognized CockroachDB version string: %q", version)
	}
	major, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse major version from %q: %w", version, err)
	}
	minor, err := strconv.Atoi(m[2])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse minor version from %q: %w", version, err)
	}
	return major, minor, nil
}

func (c *CockroachConnector) ValidateCheck(ctx context.Context) error {
	if err := c.ConnectionActive(ctx); err != nil {
		return fmt.Errorf("failed to connect to CockroachDB: %w", err)
	}

	version, err := c.GetVersion(ctx)
	if err != nil {
		return err
	}
	major, minor, err := parseCRDBVersion(version)
	if err != nil {
		return err
	}
	// version() reports "CockroachDB" for the real product; a non-match usually
	// means the peer points at a plain PostgreSQL (or other pgwire) server.
	if !crdbVersionRegex.MatchString(version) {
		return fmt.Errorf("server does not appear to be CockroachDB: %q", version)
	}
	if major < minCRDBMajorVersion || (major == minCRDBMajorVersion && minor < minCRDBMinorVersion) {
		return fmt.Errorf("CockroachDB must be v%d.%d or above, got v%d.%d",
			minCRDBMajorVersion, minCRDBMinorVersion, major, minor)
	}

	// Sinkless (core) changefeeds require the rangefeed cluster setting. We do
	// NOT check for an enterprise license: a live v25.4 cluster runs sinkless
	// changefeeds without one.
	if err := c.checkRangefeedEnabled(ctx); err != nil {
		return err
	}

	return nil
}

// checkRangefeedEnabled verifies the kv.rangefeed.enabled cluster setting is on,
// which core changefeeds require.
func (c *CockroachConnector) checkRangefeedEnabled(ctx context.Context) error {
	var enabled pgtype.Bool
	if err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled); err != nil {
		return fmt.Errorf("failed to read kv.rangefeed.enabled cluster setting: %w", err)
	}
	if !enabled.Valid || !enabled.Bool {
		return errors.New("kv.rangefeed.enabled cluster setting is off; " +
			"enable it with: SET CLUSTER SETTING kv.rangefeed.enabled = true")
	}
	return nil
}

func (c *CockroachConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	sourceTables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		parsedTable, err := common.ParseTableIdentifier(tableMapping.SourceTableIdentifier)
		if err != nil {
			return fmt.Errorf("invalid source table identifier: %w", err)
		}
		sourceTables = append(sourceTables, parsedTable)
	}

	// All mapped tables must exist.
	if err := c.checkSourceTablesExist(ctx, sourceTables); err != nil {
		return err
	}

	// Every table must have an explicit primary key: changefeeds key by PK and
	// QRep partitions by it, and a CRDB PK-less table is keyed on a hidden
	// `rowid` that cannot be replicated.
	if err := c.checkSourceTablesHavePrimaryKey(ctx, sourceTables); err != nil {
		return err
	}

	// Initial-snapshot-only mirrors never create a changefeed, so the
	// changefeed-specific prerequisites don't apply.
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	if err := c.checkRangefeedEnabled(ctx); err != nil {
		return err
	}

	if err := c.checkChangefeedPrivilege(ctx, sourceTables); err != nil {
		return err
	}

	// Best-effort: warn (never fail) if MVCC history protection looks unavailable,
	// so a user learns before a long snapshot loses t0 to GC. Protection is a
	// safety net, not a hard requirement — raising gc.ttlseconds is an alternative.
	c.warnIfProtectionUnavailable(ctx)

	return nil
}

// warnIfProtectionUnavailable probes, without failing validation, whether the
// connecting user can create MVCC history protection. The builtins require the
// REPLICATION global privilege (admins have it), so has_system_privilege is a
// cheap proxy. Any uncertainty (missing function, probe error) is only logged.
func (c *CockroachConnector) warnIfProtectionUnavailable(ctx context.Context) {
	if _, enabled := c.historyProtectionWindow(); !enabled {
		return
	}
	var isAdmin pgtype.Bool
	if err := c.conn.QueryRow(ctx, "SELECT pg_has_role(current_user, 'admin', 'MEMBER')").Scan(&isAdmin); err == nil &&
		isAdmin.Valid && isAdmin.Bool {
		return
	}
	var hasRepl pgtype.Bool
	if err := c.conn.QueryRow(ctx, "SELECT has_system_privilege('REPLICATION')").Scan(&hasRepl); err != nil {
		c.logger.Warn("[cockroach] could not determine REPLICATION privilege; "+
			"MVCC history protection for long snapshots may be unavailable", slog.Any("error", err))
		return
	}
	if !hasRepl.Valid || !hasRepl.Bool {
		c.logger.Warn("[cockroach] connecting user lacks the REPLICATION privilege; " +
			"PeerDB cannot pin MVCC history, so an initial snapshot longer than the source gc.ttlseconds may fail. " +
			"Grant it with GRANT SYSTEM REPLICATION TO <user>, or raise gc.ttlseconds on the mirrored tables.")
	}
}

// checkSourceTablesExist selects from each table and collects the ones that do
// not exist into a SourceTablesMissingError, matching the postgres connector.
func (c *CockroachConnector) checkSourceTablesExist(ctx context.Context, tables []*common.QualifiedTable) error {
	var missingTables []common.QualifiedTable
	for _, parsedTable := range tables {
		var row pgx.Row
		if err := c.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT * FROM %s LIMIT 0", parsedTable),
		).Scan(&row); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == pgerrcode.UndefinedTable {
				missingTables = append(missingTables, *parsedTable)
				continue
			}
			return fmt.Errorf("failed to select from table %s: %w", parsedTable, err)
		}
	}
	if len(missingTables) > 0 {
		return common.NewSourceTablesMissingError(missingTables)
	}
	return nil
}

// checkSourceTablesHavePrimaryKey returns a clear error naming every mapped
// table that lacks an explicit primary key.
func (c *CockroachConnector) checkSourceTablesHavePrimaryKey(ctx context.Context, tables []*common.QualifiedTable) error {
	var missingPK []string
	for _, parsedTable := range tables {
		relID, err := c.getRelIDForTable(ctx, parsedTable)
		if err != nil {
			return fmt.Errorf("failed to get relation id for table %s: %w", parsedTable, err)
		}
		pKeyCols, err := c.getPrimaryKeyColumns(ctx, relID)
		if err != nil {
			return fmt.Errorf("failed to get primary key columns for table %s: %w", parsedTable, err)
		}
		if len(pKeyCols) == 0 {
			missingPK = append(missingPK, parsedTable.String())
		}
	}
	if len(missingPK) > 0 {
		return fmt.Errorf("the following tables have no explicit primary key and cannot be mirrored "+
			"(CockroachDB keys such tables on a hidden rowid column): %v", missingPK)
	}
	return nil
}

// checkChangefeedPrivilege verifies the connecting user can create changefeeds:
// either they are a member of the admin role, or they hold the table-level
// CHANGEFEED privilege on every mapped table.
func (c *CockroachConnector) checkChangefeedPrivilege(ctx context.Context, tables []*common.QualifiedTable) error {
	var isAdmin pgtype.Bool
	if err := c.conn.QueryRow(ctx, "SELECT pg_has_role(current_user, 'admin', 'MEMBER')").Scan(&isAdmin); err != nil {
		return fmt.Errorf("failed to check admin role membership: %w", err)
	}
	if isAdmin.Valid && isAdmin.Bool {
		return nil
	}

	var missing []string
	for _, parsedTable := range tables {
		var hasGrant pgtype.Bool
		// SHOW GRANTS surfaces the table-level CHANGEFEED privilege for the
		// current user; a user with an explicit grant can run changefeeds.
		if err := c.conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT EXISTS (
				SELECT 1 FROM [SHOW GRANTS ON TABLE %s FOR current_user]
				WHERE privilege_type = 'CHANGEFEED'
			)`, parsedTable)).Scan(&hasGrant); err != nil {
			return fmt.Errorf("failed to check CHANGEFEED privilege on table %s: %w", parsedTable, err)
		}
		if !hasGrant.Valid || !hasGrant.Bool {
			missing = append(missing, parsedTable.String())
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("the connecting user lacks the CHANGEFEED privilege on the following tables "+
			"(grant it with GRANT CHANGEFEED ON TABLE <table> TO <user>, or use an admin user): %v", missing)
	}
	return nil
}
