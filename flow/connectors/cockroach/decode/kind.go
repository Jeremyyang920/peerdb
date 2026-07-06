package decode

import (
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// KindFromPostgresOID maps a pgwire type OID to a QValueKind. CockroachDB speaks
// the pgwire protocol and reports standard PostgreSQL OIDs for its built-in
// types, so this mirrors the standard-OID arm of the postgres connector's
// PostgresOIDToQValueKind (flow/connectors/postgres/type_conversion.go). The ok
// return is false for OIDs we don't recognise (user-defined enums, spatial,
// etc.) so the caller can fall back to a string decode and/or log a gap.
//
// It intentionally does NOT take a *pgtype.Map / custom-type map like the
// postgres helper: this package is connection-free, and CRDB custom types are
// resolved by name via KindFromTypeName during schema introspection (WP-A).
func KindFromPostgresOID(oid uint32) (types.QValueKind, bool) {
	switch oid {
	case pgtype.BoolOID:
		return types.QValueKindBoolean, true
	case pgtype.Int2OID:
		return types.QValueKindInt16, true
	case pgtype.Int4OID:
		return types.QValueKindInt32, true
	case pgtype.Int8OID:
		return types.QValueKindInt64, true
	case pgtype.Float4OID:
		return types.QValueKindFloat32, true
	case pgtype.Float8OID:
		return types.QValueKindFloat64, true
	case pgtype.QCharOID:
		return types.QValueKindQChar, true
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID:
		return types.QValueKindString, true
	case pgtype.ByteaOID:
		return types.QValueKindBytes, true
	case pgtype.JSONOID:
		return types.QValueKindJSON, true
	case pgtype.JSONBOID:
		return types.QValueKindJSONB, true
	case pgtype.UUIDOID:
		return types.QValueKindUUID, true
	case pgtype.TimeOID:
		return types.QValueKindTime, true
	case pgtype.TimetzOID:
		return types.QValueKindTimeTZ, true
	case pgtype.DateOID:
		return types.QValueKindDate, true
	case pgtype.TimestampOID:
		return types.QValueKindTimestamp, true
	case pgtype.TimestamptzOID:
		return types.QValueKindTimestampTZ, true
	case pgtype.NumericOID:
		return types.QValueKindNumeric, true
	case pgtype.IntervalOID:
		return types.QValueKindInterval, true
	case pgtype.InetOID:
		return types.QValueKindINET, true
	case pgtype.OIDOID:
		// CRDB OID-family columns come back as pg OID; treat as an int64 id.
		return types.QValueKindInt64, true
	// array OIDs
	case pgtype.BoolArrayOID:
		return types.QValueKindArrayBoolean, true
	case pgtype.Int2ArrayOID:
		return types.QValueKindArrayInt16, true
	case pgtype.Int4ArrayOID:
		return types.QValueKindArrayInt32, true
	case pgtype.Int8ArrayOID:
		return types.QValueKindArrayInt64, true
	case pgtype.Float4ArrayOID:
		return types.QValueKindArrayFloat32, true
	case pgtype.Float8ArrayOID:
		return types.QValueKindArrayFloat64, true
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return types.QValueKindArrayString, true
	case pgtype.UUIDArrayOID:
		return types.QValueKindArrayUUID, true
	case pgtype.DateArrayOID:
		return types.QValueKindArrayDate, true
	case pgtype.TimestampArrayOID:
		return types.QValueKindArrayTimestamp, true
	case pgtype.TimestamptzArrayOID:
		return types.QValueKindArrayTimestampTZ, true
	case pgtype.NumericArrayOID:
		return types.QValueKindArrayNumeric, true
	case pgtype.JSONBArrayOID:
		return types.QValueKindArrayJSONB, true
	case pgtype.JSONArrayOID:
		return types.QValueKindArrayJSON, true
	case pgtype.IntervalArrayOID:
		return types.QValueKindArrayInterval, true
	default:
		return types.QValueKindString, false
	}
}

// KindFromTypeName maps a CockroachDB type name (as reported by
// information_schema / SHOW COLUMNS crdb_sql_type, e.g. "INT8", "STRING",
// "DECIMAL(10,2)", "STRING[]", "TIMESTAMPTZ") to a QValueKind. The ok return is
// false for types we map only best-effort (see the CRDB gaps noted below), so
// WP-A can surface an "unsupported type" warning while still getting a usable
// string fallback.
//
// CRDB-specific gaps (return ok=false, fall back to String unless noted):
//   - BIT / VARBIT              -> String (no bit QValueKind)
//   - user-defined ENUM types   -> Enum, but the type name is the enum's own
//     name so it can't be recognised here; resolve via OID/udt during
//     introspection. Bare "ENUM" maps to Enum.
//   - GEOMETRY / GEOGRAPHY      -> mapped (ok=true) to the spatial kinds
//   - BOX2D, spatial index types-> String
//   - CRDB has no CIDR, MACADDR, HSTORE, ranges, or domains; INET is supported.
//   - OID-family (OID, REGCLASS, ...) -> Int64
func KindFromTypeName(name string) (types.QValueKind, bool) {
	base := normalizeTypeName(name)

	if elem, isArray := strings.CutSuffix(base, "[]"); isArray {
		elemKind, ok := scalarKindFromTypeName(strings.TrimSpace(elem))
		return arrayKindFor(elemKind), ok
	}
	// CRDB also spells arrays via the pg internal "_int8" style; handle it.
	if elem, isArray := strings.CutPrefix(base, "_"); isArray {
		elemKind, ok := scalarKindFromTypeName(strings.TrimSpace(elem))
		return arrayKindFor(elemKind), ok
	}

	return scalarKindFromTypeName(base)
}

// normalizeTypeName uppercases and strips length/precision modifiers and
// collation clauses, preserving a trailing "[]" array marker.
// "varchar(255)" -> "VARCHAR", "DECIMAL(10,2)" -> "DECIMAL",
// "STRING COLLATE en" -> "STRING", "int8[]" -> "INT8[]".
func normalizeTypeName(name string) string {
	s := strings.ToUpper(strings.TrimSpace(name))

	isArray := strings.HasSuffix(s, "[]")
	if isArray {
		s = strings.TrimSuffix(s, "[]")
	}
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = s[:i]
	}
	if i := strings.Index(s, " COLLATE "); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	if isArray {
		s += "[]"
	}
	return s
}

func scalarKindFromTypeName(base string) (types.QValueKind, bool) {
	switch base {
	case "BOOL", "BOOLEAN":
		return types.QValueKindBoolean, true
	case "INT2", "SMALLINT":
		return types.QValueKindInt16, true
	case "INT4":
		return types.QValueKindInt32, true
	// CRDB's INT / INTEGER / BIGINT / INT8 are all 64-bit.
	case "INT", "INT8", "INTEGER", "BIGINT", "INT64", "SERIAL", "SERIAL8", "BIGSERIAL":
		return types.QValueKindInt64, true
	case "SERIAL2", "SMALLSERIAL":
		return types.QValueKindInt16, true
	case "SERIAL4":
		return types.QValueKindInt32, true
	case "FLOAT4", "REAL":
		return types.QValueKindFloat32, true
	case "FLOAT", "FLOAT8", "DOUBLE PRECISION":
		return types.QValueKindFloat64, true
	case "DECIMAL", "NUMERIC", "DEC":
		return types.QValueKindNumeric, true
	case "STRING", "TEXT", "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING", "BPCHAR", "NAME":
		return types.QValueKindString, true
	case "\"CHAR\"":
		return types.QValueKindQChar, true
	case "BYTES", "BYTEA", "BLOB":
		return types.QValueKindBytes, true
	case "UUID":
		return types.QValueKindUUID, true
	case "DATE":
		return types.QValueKindDate, true
	case "TIME":
		return types.QValueKindTime, true
	case "TIMETZ", "TIME WITH TIME ZONE":
		return types.QValueKindTimeTZ, true
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return types.QValueKindTimestamp, true
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return types.QValueKindTimestampTZ, true
	case "INTERVAL":
		return types.QValueKindInterval, true
	case "JSONB", "JSON":
		return types.QValueKindJSONB, true
	case "INET":
		return types.QValueKindINET, true
	case "GEOMETRY":
		return types.QValueKindGeometry, true
	case "GEOGRAPHY":
		return types.QValueKindGeography, true
	case "ENUM":
		return types.QValueKindEnum, true
	case "OID", "REGCLASS", "REGPROC", "REGTYPE", "REGNAMESPACE":
		return types.QValueKindInt64, true
	case "BIT", "VARBIT", "BIT VARYING":
		// No bit QValueKind; CRDB emits these as bit strings.
		return types.QValueKindString, false
	default:
		return types.QValueKindString, false
	}
}

// arrayKindFor returns the array QValueKind whose elements are elemKind, falling
// back to a string array for element kinds without a dedicated array variant.
func arrayKindFor(elemKind types.QValueKind) types.QValueKind {
	switch elemKind {
	case types.QValueKindBoolean:
		return types.QValueKindArrayBoolean
	case types.QValueKindInt16:
		return types.QValueKindArrayInt16
	case types.QValueKindInt32:
		return types.QValueKindArrayInt32
	case types.QValueKindInt64:
		return types.QValueKindArrayInt64
	case types.QValueKindFloat32:
		return types.QValueKindArrayFloat32
	case types.QValueKindFloat64:
		return types.QValueKindArrayFloat64
	case types.QValueKindNumeric:
		return types.QValueKindArrayNumeric
	case types.QValueKindString:
		return types.QValueKindArrayString
	case types.QValueKindEnum:
		return types.QValueKindArrayEnum
	case types.QValueKindUUID:
		return types.QValueKindArrayUUID
	case types.QValueKindDate:
		return types.QValueKindArrayDate
	case types.QValueKindTimestamp:
		return types.QValueKindArrayTimestamp
	case types.QValueKindTimestampTZ:
		return types.QValueKindArrayTimestampTZ
	case types.QValueKindInterval:
		return types.QValueKindArrayInterval
	case types.QValueKindJSON:
		return types.QValueKindArrayJSON
	case types.QValueKindJSONB:
		return types.QValueKindArrayJSONB
	default:
		return types.QValueKindArrayString
	}
}
