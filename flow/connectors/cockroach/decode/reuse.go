package decode

// Pgwire snapshot value decoding — reuse guidance for WP-C (QRep snapshot).
//
// WP-C needs to turn pgx rows (read from CockroachDB over pgwire with
// AS OF SYSTEM TIME) into qvalue.QValue. Do NOT reimplement that conversion in
// this package or in the cockroach connector — the postgres connector already
// has a complete, battle-tested pgx-value -> QValue path, and CRDB speaks
// pgwire with standard PG OIDs, so it applies directly.
//
// Findings (flow/connectors/postgres):
//
//   - The value conversion is NOT exposed as a standalone function. It lives in
//     two unexported methods on *connpostgres.PostgresConnector:
//       * (*PostgresConnector).mapRowToQRecord        (qrep_query_executor.go)
//       * (*PostgresConnector).parseFieldFromPostgresOID (qvalue_convert.go)
//     Both need connector state (typeMap *pgtype.Map, customTypeMapping,
//     version, logger), so they cannot be called in isolation.
//   - The QRepQueryExecutor type and its constructor are unexported too; the
//     public entry points are the methods
//     ExecuteAndProcessQuery / ExecuteAndProcessQueryStream on that executor,
//     reachable only via a *PostgresConnector.
//   - Only the OID->QValueKind classifier is exported:
//     connpostgres.PostgresOIDToQValueKind(oid, customTypeMapping, *pgtype.Map, version).
//     This package's KindFromPostgresOID mirrors its standard-OID arm without
//     requiring a *pgtype.Map, for schema introspection (WP-A).
//
// Recommended approach for WP-C (no duplicate decoder, no cross-file edits to
// the postgres package):
//
//   1. Build a *protos.PostgresConfig from the CockroachConfig connection fields
//      (host/port/user/password/database/TLS are identical) and construct a
//      *connpostgres.PostgresConnector via connpostgres.NewPostgresConnector.
//      CRDB is pgwire-compatible, so this connects and introspects fine.
//   2. Delegate the QRep snapshot to that connector: reuse GetQRepPartitions /
//      PullQRepRecords / ExecuteAndProcessQueryStream, appending
//      `AS OF SYSTEM TIME '<t0>'` to the generated SELECT (t0 is the HLC
//      captured in SetupReplication, see the plan's snapshot-consistency
//      contract). The pgx -> QValue decode then comes for free and stays in
//      lockstep with the postgres connector.
//
// If a thin wrapper is preferred over embedding the whole PostgresConnector,
// the minimal change to the postgres package would be to export a helper that
// wraps mapRowToQRecord for a given *pgx.Conn + fds; but embedding the existing
// connector is less code and avoids maintaining a parallel path.
//
// This package intentionally owns ONLY the changefeed-JSON decode path
// (JSONToQValue / ChangefeedJSONToRecordItems) plus HLC, envelope, and type-kind
// mapping. Snapshot (pgx binary/text) values are a different wire format and are
// handled by the reused postgres path above.
