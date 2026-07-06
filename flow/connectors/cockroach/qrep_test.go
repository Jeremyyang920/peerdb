package conncockroach

import (
	"math/big"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestPostgresConfigFromCockroach(t *testing.T) {
	disable := true
	meta := "custom_schema"
	rootCa := "ca-pem"
	crdb := &protos.CockroachConfig{
		Host:                 "crdb.example",
		Port:                 26257,
		User:                 "root",
		Password:             "secret",
		Database:             "app",
		TlsHost:              "sni.example",
		MetadataSchema:       &meta,
		RootCa:               &rootCa,
		RequireTls:           true,
		DisableTls:           &disable,
		SkipCertVerification: true,
	}
	pg := postgresConfigFromCockroach(crdb)

	if pg.Host != crdb.Host || pg.Port != crdb.Port || pg.User != crdb.User ||
		pg.Password != crdb.Password || pg.Database != crdb.Database || pg.TlsHost != crdb.TlsHost {
		t.Fatalf("connection fields not mapped: %+v", pg)
	}
	if pg.MetadataSchema == nil || *pg.MetadataSchema != meta {
		t.Fatalf("metadata schema not mapped: %v", pg.MetadataSchema)
	}
	if pg.RootCa == nil || *pg.RootCa != rootCa {
		t.Fatalf("root ca not mapped: %v", pg.RootCa)
	}
	if !pg.RequireTls || pg.DisableTls == nil || !*pg.DisableTls || !pg.SkipCertVerification {
		t.Fatalf("tls fields not mapped: %+v", pg)
	}
	// Password auth is the default; CRDB has no RDS IAM equivalent.
	if pg.AuthType != protos.PostgresAuthType_POSTGRES_PASSWORD {
		t.Fatalf("unexpected auth type: %v", pg.AuthType)
	}
}

func TestSupportsRangePartition(t *testing.T) {
	supported := []types.QValueKind{
		types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32, types.QValueKindUInt64,
		types.QValueKindDate, types.QValueKindTimestamp, types.QValueKindTimestampTZ,
		types.QValueKindString, types.QValueKindUUID,
	}
	for _, k := range supported {
		if !supportsRangePartition(k) {
			t.Errorf("expected %s to support range partitioning", k)
		}
	}
	unsupported := []types.QValueKind{
		types.QValueKindBytes, types.QValueKindJSON, types.QValueKindNumeric,
		types.QValueKindBoolean, types.QValueKindFloat64,
	}
	for _, k := range unsupported {
		if supportsRangePartition(k) {
			t.Errorf("expected %s not to support range partitioning", k)
		}
	}
}

func TestIsStringWatermark(t *testing.T) {
	if !isStringWatermark(types.QValueKindUUID) || !isStringWatermark(types.QValueKindString) {
		t.Fatal("uuid and string must be treated as string watermarks")
	}
	if isStringWatermark(types.QValueKindInt64) || isStringWatermark(types.QValueKindTimestamp) {
		t.Fatal("numeric/temporal must not be treated as string watermarks")
	}
}

func TestNormalizeBound(t *testing.T) {
	if got := normalizeBound(int16(5)); got != int64(5) {
		t.Errorf("int16 not widened to int64: %#v", got)
	}
	if got := normalizeBound(int32(7)); got != int64(7) {
		t.Errorf("int32 not widened to int64: %#v", got)
	}
	if got := normalizeBound(int64(9)); got != int64(9) {
		t.Errorf("int64 changed: %#v", got)
	}
}

func TestDetectUUIDWithHexCasing(t *testing.T) {
	lowerMin := "004ef3e4-97ca-4ec7-a96d-cba73f0cebe2"
	lowerMax := "ffad725b-4fa8-482e-9785-1ddf82ceda77"
	if ok, casing := detectUUIDWithHexCasing(lowerMin, lowerMax); !ok || casing != hexCasingLower {
		t.Errorf("expected lowercase uuid, got ok=%v casing=%v", ok, casing)
	}
	upperMin := "004EF3E4-97CA-4EC7-A96D-CBA73F0CEBE2"
	upperMax := "FFAD725B-4FA8-482E-9785-1DDF82CEDA77"
	if ok, casing := detectUUIDWithHexCasing(upperMin, upperMax); !ok || casing != hexCasingUpper {
		t.Errorf("expected uppercase uuid, got ok=%v casing=%v", ok, casing)
	}
	if ok, _ := detectUUIDWithHexCasing("k00001", "k00500"); ok {
		t.Error("non-uuid strings must not be detected as uuid")
	}
	// Mixed casing is not a canonical single-casing UUID pair.
	if ok, _ := detectUUIDWithHexCasing(lowerMin, upperMax); ok {
		t.Error("mixed-casing bounds must not be detected as a single-casing uuid pair")
	}
}

func TestBuildUUIDStringPartitions(t *testing.T) {
	minVal := "00000000-0000-0000-0000-000000000000"
	maxVal := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	const numPartitions = 8

	parts, err := buildUUIDStringPartitions(minVal, maxVal, hexCasingLower, numPartitions)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) == 0 {
		t.Fatal("expected at least one partition")
	}

	// First partition starts at the true min; last ends at the true max inclusive.
	first := parts[0].Range.GetStringRange()
	if first.Start != minVal {
		t.Errorf("first partition start = %s, want %s", first.Start, minVal)
	}
	last := parts[len(parts)-1].Range.GetStringRange()
	if last.End != maxVal {
		t.Errorf("last partition end = %s, want %s", last.End, maxVal)
	}
	if !last.EndInclusive {
		t.Error("last partition must be end-inclusive to cover the max row")
	}

	// Contiguity: each partition's end equals the next partition's start, so the
	// half-open ranges [start,end) tile the space with no gaps or overlaps.
	for i := 1; i < len(parts); i++ {
		prev := parts[i-1].Range.GetStringRange()
		cur := parts[i].Range.GetStringRange()
		if prev.End != cur.Start {
			t.Errorf("partition %d end %s != partition %d start %s", i-1, prev.End, i, cur.Start)
		}
		if prev.EndInclusive {
			t.Errorf("interior partition %d must be end-exclusive", i-1)
		}
	}
}

func TestBuildUUIDStringPartitionsUpperCasing(t *testing.T) {
	minVal := "004EF3E4-97CA-4EC7-A96D-CBA73F0CEBE2"
	maxVal := "FFAD725B-4FA8-482E-9785-1DDF82CEDA77"
	parts, err := buildUUIDStringPartitions(minVal, maxVal, hexCasingUpper, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Interior boundaries must be rendered in the detected uppercase casing.
	for i := 1; i < len(parts); i++ {
		start := parts[i].Range.GetStringRange().Start
		if start != minVal && start != maxVal {
			for _, r := range start {
				if r >= 'a' && r <= 'f' {
					t.Fatalf("interior boundary %q is lowercase, expected uppercase", start)
				}
			}
		}
	}
}

func TestBuildUUIDStringPartitionsRejectsInverted(t *testing.T) {
	_, err := buildUUIDStringPartitions(
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"00000000-0000-0000-0000-000000000000",
		hexCasingLower, 4)
	if err == nil {
		t.Fatal("expected error when min uuid > max uuid")
	}
}

func TestUUIDBigIntRoundTrip(t *testing.T) {
	s := "004ef3e4-97ca-4ec7-a96d-cba73f0cebe2"
	n, err := uuidToBigInt(s)
	if err != nil {
		t.Fatalf("uuidToBigInt: %v", err)
	}
	got, err := bigIntToUUID(n, hexCasingLower)
	if err != nil {
		t.Fatalf("bigIntToUUID: %v", err)
	}
	if got != s {
		t.Errorf("round trip = %s, want %s", got, s)
	}
	if _, err := bigIntToUUID(new(big.Int).Lsh(big.NewInt(1), 200), hexCasingLower); err == nil {
		t.Error("expected error for value exceeding 128 bits")
	}
}

func TestSnapshotClauseInjectionSafe(t *testing.T) {
	// snapshotClause must render only from parsed integer HLC components, so no
	// caller-controlled text can reach the SQL string.
	hlc, err := decode.ParseHLC("1783373642721470097.0000000000")
	if err != nil {
		t.Fatalf("parse hlc: %v", err)
	}
	got := snapshotClause(hlc)
	want := "SET TRANSACTION AS OF SYSTEM TIME '1783373642721470097.0000000000'"
	if got != want {
		t.Errorf("snapshotClause = %q, want %q", got, want)
	}
}
