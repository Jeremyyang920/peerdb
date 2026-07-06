package decode

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestKindFromPostgresOID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		oid  uint32
		want types.QValueKind
		ok   bool
	}{
		{pgtype.BoolOID, types.QValueKindBoolean, true},
		{pgtype.Int2OID, types.QValueKindInt16, true},
		{pgtype.Int4OID, types.QValueKindInt32, true},
		{pgtype.Int8OID, types.QValueKindInt64, true},
		{pgtype.Float4OID, types.QValueKindFloat32, true},
		{pgtype.Float8OID, types.QValueKindFloat64, true},
		{pgtype.NumericOID, types.QValueKindNumeric, true},
		{pgtype.TextOID, types.QValueKindString, true},
		{pgtype.VarcharOID, types.QValueKindString, true},
		{pgtype.ByteaOID, types.QValueKindBytes, true},
		{pgtype.UUIDOID, types.QValueKindUUID, true},
		{pgtype.JSONBOID, types.QValueKindJSONB, true},
		{pgtype.JSONOID, types.QValueKindJSON, true},
		{pgtype.DateOID, types.QValueKindDate, true},
		{pgtype.TimeOID, types.QValueKindTime, true},
		{pgtype.TimetzOID, types.QValueKindTimeTZ, true},
		{pgtype.TimestampOID, types.QValueKindTimestamp, true},
		{pgtype.TimestamptzOID, types.QValueKindTimestampTZ, true},
		{pgtype.IntervalOID, types.QValueKindInterval, true},
		{pgtype.InetOID, types.QValueKindINET, true},
		{pgtype.Int8ArrayOID, types.QValueKindArrayInt64, true},
		{pgtype.TextArrayOID, types.QValueKindArrayString, true},
		{pgtype.UUIDArrayOID, types.QValueKindArrayUUID, true},
		{pgtype.NumericArrayOID, types.QValueKindArrayNumeric, true},
		{0, types.QValueKindString, false},
		{999999, types.QValueKindString, false},
	}
	for _, tc := range tests {
		got, ok := KindFromPostgresOID(tc.oid)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("KindFromPostgresOID(%d) = (%s,%v), want (%s,%v)", tc.oid, got, ok, tc.want, tc.ok)
		}
	}
}

func TestKindFromTypeName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want types.QValueKind
		ok   bool
	}{
		{"BOOL", types.QValueKindBoolean, true},
		{"INT8", types.QValueKindInt64, true},
		{"INT", types.QValueKindInt64, true},
		{"INTEGER", types.QValueKindInt64, true},
		{"INT4", types.QValueKindInt32, true},
		{"INT2", types.QValueKindInt16, true},
		{"SMALLINT", types.QValueKindInt16, true},
		{"BIGINT", types.QValueKindInt64, true},
		{"FLOAT8", types.QValueKindFloat64, true},
		{"FLOAT4", types.QValueKindFloat32, true},
		{"DECIMAL", types.QValueKindNumeric, true},
		{"DECIMAL(10,2)", types.QValueKindNumeric, true},
		{"NUMERIC (12, 4)", types.QValueKindNumeric, true},
		{"STRING", types.QValueKindString, true},
		{"VARCHAR(255)", types.QValueKindString, true},
		{"STRING COLLATE en_US", types.QValueKindString, true},
		{"BYTES", types.QValueKindBytes, true},
		{"UUID", types.QValueKindUUID, true},
		{"TIMESTAMPTZ", types.QValueKindTimestampTZ, true},
		{"TIMESTAMP", types.QValueKindTimestamp, true},
		{"TIMESTAMP WITH TIME ZONE", types.QValueKindTimestampTZ, true},
		{"DATE", types.QValueKindDate, true},
		{"TIME", types.QValueKindTime, true},
		{"TIMETZ", types.QValueKindTimeTZ, true},
		{"INTERVAL", types.QValueKindInterval, true},
		{"JSONB", types.QValueKindJSONB, true},
		{"JSON", types.QValueKindJSONB, true},
		{"INET", types.QValueKindINET, true},
		{"GEOMETRY", types.QValueKindGeometry, true},
		{"GEOGRAPHY", types.QValueKindGeography, true},
		{"lowercase int8", types.QValueKindString, false},

		// arrays
		{"INT8[]", types.QValueKindArrayInt64, true},
		{"STRING[]", types.QValueKindArrayString, true},
		{"UUID[]", types.QValueKindArrayUUID, true},
		{"DECIMAL(10,2)[]", types.QValueKindArrayNumeric, true},
		{"_int8", types.QValueKindArrayInt64, true},
		{"TIMESTAMPTZ[]", types.QValueKindArrayTimestampTZ, true},

		// gaps
		{"BIT", types.QValueKindString, false},
		{"VARBIT", types.QValueKindString, false},
		{"CIDR", types.QValueKindString, false},
	}
	for _, tc := range tests {
		got, ok := KindFromTypeName(tc.name)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("KindFromTypeName(%q) = (%s,%v), want (%s,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}
