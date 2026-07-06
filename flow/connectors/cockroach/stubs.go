package conncockroach

import (
	"context"
	"errors"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// This file holds placeholder implementations for the source-connector interface
// methods that land in later work packages (WP-A validation/introspection,
// WP-C snapshot/QRep, WP-D CDC). They exist so the compile-time interface
// assertions in flow/connectors/core.go hold during Phase 0.

// --- GetTableSchemaConnector (WP-A) ---

func (c *CockroachConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	return nil, errors.ErrUnsupported
}

// --- GetSchemaConnector (WP-A) ---

func (c *CockroachConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	return nil, errors.ErrUnsupported
}

func (c *CockroachConnector) GetColumns(
	ctx context.Context, version uint32, schema string, table string,
) (*protos.TableColumnsResponse, error) {
	return nil, errors.ErrUnsupported
}

func (c *CockroachConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	return nil, errors.ErrUnsupported
}

func (c *CockroachConnector) GetTablesInSchema(
	ctx context.Context, schema string, cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	return nil, errors.ErrUnsupported
}

// --- ValidationConnector / MirrorSourceValidationConnector (WP-A) ---

func (c *CockroachConnector) ValidateCheck(ctx context.Context) error {
	return errors.ErrUnsupported
}

func (c *CockroachConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	return errors.ErrUnsupported
}

// --- CDCPullConnectorCore / CDCPullConnector (WP-D) ---

func (c *CockroachConnector) EnsurePullability(
	ctx context.Context, req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, errors.ErrUnsupported
}

func (c *CockroachConnector) ExportTxSnapshot(
	ctx context.Context, flowName string, env map[string]string,
) (*protos.ExportTxSnapshotOutput, any, error) {
	return nil, nil, errors.ErrUnsupported
}

func (c *CockroachConnector) FinishExport(any) error {
	return errors.ErrUnsupported
}

func (c *CockroachConnector) SetupReplication(
	ctx context.Context, catalogPool shared.CatalogPool, req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	return model.SetupReplicationResult{}, errors.ErrUnsupported
}

func (c *CockroachConnector) SetupReplConn(ctx context.Context, env map[string]string) error {
	return errors.ErrUnsupported
}

func (c *CockroachConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	return errors.ErrUnsupported
}

func (c *CockroachConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return errors.ErrUnsupported
}

func (c *CockroachConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	return errors.ErrUnsupported
}

// --- QRepPullConnectorCore / QRepPullConnector (WP-C) ---

func (c *CockroachConnector) GetQRepPartitions(
	ctx context.Context, config *protos.QRepConfig, last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	return nil, errors.ErrUnsupported
}

func (c *CockroachConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context, input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	return nil, errors.ErrUnsupported
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
	return 0, 0, errors.ErrUnsupported
}
