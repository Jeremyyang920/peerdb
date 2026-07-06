package conncockroach

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type CockroachConnector struct {
	*metadataStore.PostgresMetadata
	config         *protos.CockroachConfig
	ssh            *utils.SSHTunnel
	conn           *pgx.Conn
	replConn       *pgx.Conn
	typeMap        *pgtype.Map
	logger         log.Logger
	connStr        string
	metadataSchema string
	version        string
	replLock       sync.Mutex
}

func NewCockroachConnector(
	ctx context.Context, config *protos.CockroachConfig,
) (*CockroachConnector, error) {
	logger := internal.LoggerFromCtx(ctx)

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	flowNameInApplicationName, err := internal.PeerDBApplicationNamePerMirrorName(ctx, nil)
	if err != nil {
		logger.Error("Failed to get flow name from application name", slog.Any("error", err))
	}
	var flowName string
	if flowNameInApplicationName {
		flowName, _ = ctx.Value(shared.FlowNameKey).(string)
	}

	connectionString := GetConnectionString(config, flowName)
	connConfig, err := ParseConfig(connectionString, config)
	if err != nil {
		return nil, err
	}

	connConfig.Config.RuntimeParams["timezone"] = "UTC"
	connConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	connConfig.Config.RuntimeParams["statement_timeout"] = "0"
	connConfig.Config.RuntimeParams["DateStyle"] = "ISO, DMY"

	tunnel, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		logger.Error("failed to create ssh tunnel", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	conn, err := NewCockroachConnFromConfig(ctx, connConfig, tunnel)
	if err != nil {
		tunnel.Close()
		logger.Error("failed to create connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	metadataSchema := "_peerdb_internal"
	if config.MetadataSchema != nil {
		metadataSchema = *config.MetadataSchema
	}

	return &CockroachConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		ssh:              tunnel,
		conn:             conn,
		typeMap:          pgtype.NewMap(),
		logger:           logger,
		connStr:          connectionString,
		metadataSchema:   metadataSchema,
	}, nil
}

func (c *CockroachConnector) Close() error {
	var errs []error
	if c.replConn != nil {
		if err := c.replConn.Close(context.Background()); err != nil {
			c.logger.Error("failed to close replication connection", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close replication connection: %w", err))
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(context.Background()); err != nil {
			c.logger.Error("failed to close connection", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}
	if c.ssh != nil {
		if err := c.ssh.Close(); err != nil {
			c.logger.Error("failed to close SSH tunnel", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close SSH tunnel: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing CockroachDB connector: %v", errs)
	}
	return nil
}

func (c *CockroachConnector) ConnectionActive(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.conn.Ping(ctx)
}

func (c *CockroachConnector) GetVersion(ctx context.Context) (string, error) {
	if c.version != "" {
		return c.version, nil
	}
	var version string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return "", fmt.Errorf("failed to get CockroachDB version: %w", err)
	}
	c.version = version
	return version, nil
}
