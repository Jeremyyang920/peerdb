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

// --- CDCPullConnectorCore / CDCPullConnector (WP-D) ---

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
