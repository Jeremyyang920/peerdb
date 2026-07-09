package conncockroach

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// mustUseTLS mirrors internal.PGMustUseTlsConnection: TLS is used when explicitly
// required, or unless it has been explicitly disabled.
func mustUseTLS(config *protos.CockroachConfig) bool {
	return config.RequireTls || (config.DisableTls != nil && !*config.DisableTls)
}

// GetConnectionString builds a pgwire connection string for CockroachDB.
// CockroachDB speaks the Postgres wire protocol, so this mirrors
// internal.GetPGConnectionString.
func GetConnectionString(config *protos.CockroachConfig, flowName string) string {
	// strip path and query params that may be present in the host
	host, _, _ := strings.Cut(config.Host, "/")
	host, _, _ = strings.Cut(host, "?")

	u := &url.URL{
		Scheme: "postgres",
		Host:   shared.JoinHostPort(host, config.Port),
		Path:   "/" + config.Database,
	}

	if config.Password != "" {
		u.User = url.UserPassword(config.User, config.Password)
	} else {
		u.User = url.User(config.User)
	}

	applicationName := "peerdb"
	if flowName != "" {
		applicationName = "peerdb_" + flowName
	}

	q := u.Query()
	q.Set("application_name", applicationName)
	q.Set("client_encoding", "UTF8")
	if mustUseTLS(config) {
		q.Set("sslmode", "require")
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func ParseConfig(connectionString string, config *protos.CockroachConfig) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	connConfig.Config.MaxProtocolVersion = "3.0"

	if mustUseTLS(config) || config.RootCa != nil {
		// CockroachConfig has no client-certificate (mTLS) fields yet; pass nil.
		tlsConfig, err := common.CreateTlsConfig(
			tls.VersionTLS12, config.RootCa, connConfig.Host, config.TlsHost, config.SkipCertVerification, nil)
		if err != nil {
			return nil, err
		}
		connConfig.TLSConfig = tlsConfig
	}
	return connConfig, nil
}

func NewCockroachConnFromConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	tunnel *utils.SSHTunnel,
) (*pgx.Conn, error) {
	if tunnel != nil && tunnel.Client != nil {
		connConfig.DialFunc = tunnel.DialContext
		// DNS lookup happens before the connection is established, which is a problem
		// when the host is only resolvable on the SSH host.
		// https://github.com/jackc/pgx/issues/1724
		connConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			return []string{host}, nil
		}
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CockroachDB: %w", err)
	}
	return conn, nil
}
