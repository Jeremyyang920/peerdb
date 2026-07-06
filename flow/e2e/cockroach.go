package e2e

import (
	"context"
	"fmt"
	"testing"

	conncockroach "github.com/PeerDB-io/peerdb/flow/connectors/cockroach"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// CockroachSource is the e2e SuiteSource for CockroachDB. It holds two things:
//
//   - conn: the CockroachDB connector under test. It is returned by Connector()
//     so the generic suite's source-type assertions see a CockroachConnector
//     (not a PostgresConnector), and creating it here exercises peer/connector
//     construction from a CockroachConfig.
//   - pg:   a Postgres connector over the SAME CockroachDB cluster, used to run
//     DDL/DML and read rows in tests. CockroachDB speaks the Postgres wire
//     protocol, and this is the very delegate the connector uses for its
//     snapshot reads (see cockroach/qrep.go), so source rows decode into exactly
//     the QValues the snapshot path produces — the fairest possible comparison
//     against the ClickHouse destination.
type CockroachSource struct {
	conn   *conncockroach.CockroachConnector
	pg     *connpostgres.PostgresConnector
	config *protos.CockroachConfig
}

// postgresConfigForCockroach projects the CockroachDB connection fields onto a
// PostgresConfig so the pgwire-compatible Postgres connector can drive reads and
// DDL against the CRDB cluster in tests.
func postgresConfigForCockroach(c *protos.CockroachConfig) *protos.PostgresConfig {
	return &protos.PostgresConfig{
		Host:                 c.Host,
		Port:                 c.Port,
		User:                 c.User,
		Password:             c.Password,
		Database:             c.Database,
		TlsHost:              c.TlsHost,
		MetadataSchema:       c.MetadataSchema,
		SshConfig:            c.SshConfig,
		RootCa:               c.RootCa,
		RequireTls:           c.RequireTls,
		DisableTls:           c.DisableTls,
		SkipCertVerification: c.SkipCertVerification,
	}
}

func SetupCockroach(t *testing.T, suffix string) (*CockroachSource, error) {
	t.Helper()

	config := internal.GetCockroachConfigFromEnv()

	conn, err := conncockroach.NewCockroachConnector(t.Context(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cockroach connector: %w", err)
	}

	pg, err := connpostgres.NewPostgresConnector(t.Context(), nil, postgresConfigForCockroach(config))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create pgwire reader for cockroach: %w", err)
	}

	src := &CockroachSource{conn: conn, pg: pg, config: config}
	if err := src.resetSchema(t.Context(), suffix); err != nil {
		conn.Close()
		pg.Close()
		return nil, err
	}
	return src, nil
}

// resetSchema drops and recreates the per-test schema. CockroachDB supports
// user-defined schemas within a database, so tests live in e2e_test_<suffix>
// inside `defaultdb`, mirroring the Postgres suite's schema-per-test layout.
func (s *CockroachSource) resetSchema(ctx context.Context, suffix string) error {
	if err := s.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS e2e_test_%s CASCADE`, suffix)); err != nil {
		return fmt.Errorf("failed to drop cockroach schema: %w", err)
	}
	if err := s.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA e2e_test_%s`, suffix)); err != nil {
		return fmt.Errorf("failed to create cockroach schema: %w", err)
	}
	return nil
}

func (s *CockroachSource) Connector() connectors.Connector {
	return s.conn
}

// CockroachConnector exposes the connector under test for CRDB-specific
// assertions in the dedicated suite.
func (s *CockroachSource) CockroachConnector() *conncockroach.CockroachConnector {
	return s.conn
}

func (s *CockroachSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name: "cockroach",
		Type: protos.DBType_COCKROACH,
		Config: &protos.Peer_CockroachConfig{
			CockroachConfig: s.config,
		},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *CockroachSource) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.pg.Conn().Exec(ctx, sql, args...)
	return err
}

// Query runs an arbitrary read against the CRDB cluster, decoding into QValues.
func (s *CockroachSource) Query(ctx context.Context, query string, args ...any) (*model.QRecordBatch, error) {
	exec, err := s.pg.NewQRepQueryExecutor(ctx, nil, shared.InternalVersion_Latest, "testflow", "testpart")
	if err != nil {
		return nil, err
	}
	return exec.ExecuteAndProcessQuery(ctx, query, args...)
}

func (s *CockroachSource) GetRows(ctx context.Context, suffix, table, cols string) (*model.QRecordBatch, error) {
	return s.Query(ctx,
		fmt.Sprintf(`SELECT %s FROM e2e_test_%s.%s ORDER BY id`, cols, suffix, common.QuoteIdentifier(table)))
}

func (s *CockroachSource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
	if err := s.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS e2e_test_%s CASCADE`, suffix)); err != nil {
		t.Log("failed to drop cockroach schema", err)
	}
	if s.conn != nil {
		s.conn.Close()
	}
	if s.pg != nil {
		s.pg.Close()
	}
}
