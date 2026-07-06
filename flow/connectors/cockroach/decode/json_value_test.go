package decode

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func mustDecode(t *testing.T, kind types.QValueKind, typmod int32, raw string) types.QValue {
	t.Helper()
	qv, err := JSONToQValue(kind, typmod, json.RawMessage(raw))
	if err != nil {
		t.Fatalf("JSONToQValue(%s, %q): %v", kind, raw, err)
	}
	return qv
}

func TestJSONToQValueScalars(t *testing.T) {
	t.Parallel()

	if qv := mustDecode(t, types.QValueKindBoolean, -1, `true`); qv.Value() != true {
		t.Fatalf("bool = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindInt64, -1, `42`); qv.Value() != int64(42) {
		t.Fatalf("int64 = %v", qv.Value())
	}
	// INT8 beyond 2^53 must survive: parsed via json.Number, not float64.
	const big = int64(9007199254740993) // 2^53 + 1
	if qv := mustDecode(t, types.QValueKindInt64, -1, `9007199254740993`); qv.Value() != big {
		t.Fatalf("large int64 = %v, want %d", qv.Value(), big)
	}
	if qv := mustDecode(t, types.QValueKindInt32, -1, `-7`); qv.Value() != int32(-7) {
		t.Fatalf("int32 = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindInt16, -1, `100`); qv.Value() != int16(100) {
		t.Fatalf("int16 = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindFloat64, -1, `1.5`); qv.Value() != 1.5 {
		t.Fatalf("float64 = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindString, -1, `"hello"`); qv.Value() != "hello" {
		t.Fatalf("string = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindEnum, -1, `"active"`); qv.Value() != "active" {
		t.Fatalf("enum = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindINET, -1, `"192.168.0.1/32"`); qv.Value() != "192.168.0.1/32" {
		t.Fatalf("inet = %v", qv.Value())
	}
}

func TestJSONToQValueDecimal(t *testing.T) {
	t.Parallel()
	// DECIMAL is emitted as an unquoted JSON number; precision must be exact.
	qv := mustDecode(t, types.QValueKindNumeric, datatypes.MakeNumericTypmod(10, 2), `22.30`)
	num, ok := qv.(types.QValueNumeric)
	if !ok {
		t.Fatalf("not numeric: %T", qv)
	}
	// shopspring/decimal normalises trailing zeros (22.30 -> "22.3"); the DECIMAL
	// scale is preserved separately in Precision/Scale, matching the postgres
	// connector's QValueNumeric behaviour.
	if num.Val.String() != "22.3" {
		t.Fatalf("decimal = %s, want 22.3", num.Val.String())
	}
	if num.Precision != 10 || num.Scale != 2 {
		t.Fatalf("precision/scale = %d/%d, want 10/2", num.Precision, num.Scale)
	}

	// Very high precision decimal beyond float64 range of exactness.
	qv2 := mustDecode(t, types.QValueKindNumeric, -1, `123456789012345678901234567890.123456789`)
	if got := qv2.(types.QValueNumeric).Val.String(); got != "123456789012345678901234567890.123456789" {
		t.Fatalf("hi-precision decimal = %s", got)
	}

	// Live v25.4.12 ground truth: DECIMAL columns arrive as unquoted JSON
	// numbers (10.50, 99.99), not strings.
	for in, want := range map[string]string{`10.50`: "10.5", `99.99`: "99.99"} {
		if got := mustDecode(t, types.QValueKindNumeric, -1, in).(types.QValueNumeric).Val.String(); got != want {
			t.Fatalf("live decimal %s = %s, want %s", in, got, want)
		}
	}
}

func TestJSONToQValueUUID(t *testing.T) {
	t.Parallel()
	const s = "58390d92-2472-43e1-86bc-1642395e8dad"
	qv := mustDecode(t, types.QValueKindUUID, -1, `"`+s+`"`)
	if qv.(types.QValueUUID).Val != uuid.MustParse(s) {
		t.Fatalf("uuid = %v", qv.Value())
	}
}

func TestJSONToQValueBytes(t *testing.T) {
	t.Parallel()
	// hex form with \x prefix (SQL default).
	qv := mustDecode(t, types.QValueKindBytes, -1, `"\\x48aa"`)
	if !bytes.Equal(qv.(types.QValueBytes).Val, []byte{0x48, 0xaa}) {
		t.Fatalf("hex bytes = %x", qv.(types.QValueBytes).Val)
	}
	// base64 fallback.
	qv2 := mustDecode(t, types.QValueKindBytes, -1, `"aGVsbG8="`)
	if string(qv2.(types.QValueBytes).Val) != "hello" {
		t.Fatalf("base64 bytes = %q", qv2.(types.QValueBytes).Val)
	}
}

func TestJSONToQValueTemporal(t *testing.T) {
	t.Parallel()

	// TIMESTAMP: no zone.
	ts := mustDecode(t, types.QValueKindTimestamp, -1, `"2025-04-30T20:02:35.40316"`)
	wantTs := time.Date(2025, 4, 30, 20, 2, 35, 403160000, time.UTC)
	if !ts.(types.QValueTimestamp).Val.Equal(wantTs) {
		t.Fatalf("timestamp = %v, want %v", ts.Value(), wantTs)
	}

	// TIMESTAMPTZ: Z suffix, normalised to UTC.
	tstz := mustDecode(t, types.QValueKindTimestampTZ, -1, `"2025-04-30T20:02:35.40316Z"`)
	if !tstz.(types.QValueTimestampTZ).Val.Equal(wantTs) {
		t.Fatalf("timestamptz = %v, want %v", tstz.Value(), wantTs)
	}

	// TIMESTAMPTZ with numeric offset.
	tstz2 := mustDecode(t, types.QValueKindTimestampTZ, -1, `"2025-04-30T22:02:35+02:00"`)
	want2 := time.Date(2025, 4, 30, 20, 2, 35, 0, time.UTC)
	if !tstz2.(types.QValueTimestampTZ).Val.Equal(want2) {
		t.Fatalf("timestamptz offset = %v, want %v", tstz2.Value(), want2)
	}

	// DATE.
	d := mustDecode(t, types.QValueKindDate, -1, `"2025-04-30"`)
	wantD := time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC)
	if !d.(types.QValueDate).Val.Equal(wantD) {
		t.Fatalf("date = %v, want %v", d.Value(), wantD)
	}

	// TIME -> duration since midnight.
	tm := mustDecode(t, types.QValueKindTime, -1, `"13:45:30.5"`)
	wantDur := 13*time.Hour + 45*time.Minute + 30*time.Second + 500*time.Millisecond
	if tm.(types.QValueTime).Val != wantDur {
		t.Fatalf("time = %v, want %v", tm.Value(), wantDur)
	}

	// infinity -> NULL.
	inf := mustDecode(t, types.QValueKindTimestamp, -1, `"infinity"`)
	if _, ok := inf.(types.QValueNull); !ok {
		t.Fatalf("infinity timestamp = %T, want QValueNull", inf)
	}
}

func TestJSONToQValueInterval(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want datatypes.PeerDBInterval
	}{
		{
			name: "iso full",
			in:   `"P1Y2M3DT4H5M6.5S"`,
			want: datatypes.PeerDBInterval{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6.5, Valid: true},
		},
		{
			name: "iso time only",
			in:   `"PT1H30M"`,
			want: datatypes.PeerDBInterval{Hours: 1, Minutes: 30, Valid: true},
		},
		{
			name: "iso weeks",
			in:   `"P2W"`,
			want: datatypes.PeerDBInterval{Days: 14, Valid: true},
		},
		{
			name: "postgres verbose",
			in:   `"1 year 2 mons 3 days 04:05:06"`,
			want: datatypes.PeerDBInterval{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6, Valid: true},
		},
		{
			name: "postgres clock only",
			in:   `"04:05:06.5"`,
			want: datatypes.PeerDBInterval{Hours: 4, Minutes: 5, Seconds: 6.5, Valid: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			qv := mustDecode(t, types.QValueKindInterval, -1, tc.in)
			wantJSON, _ := json.Marshal(tc.want)
			if qv.Value() != string(wantJSON) {
				t.Fatalf("interval %s = %v, want %s", tc.in, qv.Value(), wantJSON)
			}
		})
	}
}

func TestJSONToQValueJSONB(t *testing.T) {
	t.Parallel()

	// object passthrough.
	qv := mustDecode(t, types.QValueKindJSONB, -1, `{"a":1,"b":[2,3]}`)
	j := qv.(types.QValueJSON)
	if j.Val != `{"a":1,"b":[2,3]}` || j.IsArray {
		t.Fatalf("jsonb object = %+v", j)
	}

	// array passthrough.
	qv2 := mustDecode(t, types.QValueKindJSONB, -1, `[1,2,3]`)
	if !qv2.(types.QValueJSON).IsArray {
		t.Fatalf("jsonb array IsArray = false")
	}

	// JSON null *value* (not SQL NULL) survives as "null".
	qv3 := mustDecode(t, types.QValueKindJSONB, -1, `null`)
	if qv3.(types.QValueJSON).Val != "null" {
		t.Fatalf("json null value = %q, want null", qv3.(types.QValueJSON).Val)
	}

	// encode_json_value_null_as_object sentinel -> "null".
	qv4 := mustDecode(t, types.QValueKindJSONB, -1, jsonNullSentinel)
	if qv4.(types.QValueJSON).Val != "null" {
		t.Fatalf("sentinel = %q, want null", qv4.(types.QValueJSON).Val)
	}
}

func TestJSONToQValueNull(t *testing.T) {
	t.Parallel()
	// SQL NULL for a non-JSON kind.
	qv, err := JSONToQValue(types.QValueKindInt64, -1, json.RawMessage(`null`))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, ok := qv.(types.QValueNull); !ok {
		t.Fatalf("null int = %T, want QValueNull", qv)
	}
	if qv.Kind() != types.QValueKindInt64 {
		t.Fatalf("null kind = %s", qv.Kind())
	}
	// absent value.
	qv2, err := JSONToQValue(types.QValueKindString, -1, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, ok := qv2.(types.QValueNull); !ok {
		t.Fatalf("absent = %T, want QValueNull", qv2)
	}
}

func TestJSONToQValueArrays(t *testing.T) {
	t.Parallel()

	qv := mustDecode(t, types.QValueKindArrayInt64, -1, `[1,2,9007199254740993]`)
	got := qv.(types.QValueArrayInt64).Val
	want := []int64{1, 2, 9007199254740993}
	if len(got) != 3 || got[2] != want[2] {
		t.Fatalf("int64 array = %v, want %v", got, want)
	}

	qs := mustDecode(t, types.QValueKindArrayString, -1, `["a","b","c"]`)
	if len(qs.(types.QValueArrayString).Val) != 3 {
		t.Fatalf("string array = %v", qs.Value())
	}

	qb := mustDecode(t, types.QValueKindArrayBoolean, -1, `[true,false]`)
	if len(qb.(types.QValueArrayBoolean).Val) != 2 {
		t.Fatalf("bool array = %v", qb.Value())
	}

	qu := mustDecode(t, types.QValueKindArrayUUID, -1,
		`["58390d92-2472-43e1-86bc-1642395e8dad","32856ed8-34d3-45a3-a449-412bdeaa277c"]`)
	if len(qu.(types.QValueArrayUUID).Val) != 2 {
		t.Fatalf("uuid array = %v", qu.Value())
	}

	qn := mustDecode(t, types.QValueKindArrayNumeric, -1, `[1.5,2.25]`)
	if len(qn.(types.QValueArrayNumeric).Val) != 2 || qn.(types.QValueArrayNumeric).Val[1].String() != "2.25" {
		t.Fatalf("numeric array = %v", qn.Value())
	}
}

func TestJSONToQValueMiscScalars(t *testing.T) {
	t.Parallel()

	if qv := mustDecode(t, types.QValueKindInt8, -1, `12`); qv.Value() != int8(12) {
		t.Fatalf("int8 = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindFloat32, -1, `2.5`); qv.Value() != float32(2.5) {
		t.Fatalf("float32 = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindQChar, -1, `"A"`); qv.Value() != uint8('A') {
		t.Fatalf("qchar = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindGeometry, -1, `"POINT(1 2)"`); qv.Value() != "POINT(1 2)" {
		t.Fatalf("geometry = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindGeography, -1, `"POINT(3 4)"`); qv.Value() != "POINT(3 4)" {
		t.Fatalf("geography = %v", qv.Value())
	}

	// large INT8 delivered as a quoted string still decodes losslessly.
	if qv := mustDecode(t, types.QValueKindInt64, -1, `"9007199254740993"`); qv.Value() != int64(9007199254740993) {
		t.Fatalf("quoted int64 = %v", qv.Value())
	}
}

func TestJSONToQValueTimeTZ(t *testing.T) {
	t.Parallel()
	// TIMETZ mirrors the postgres connector: time-of-day as a duration offset
	// from year 0000 in UTC. We assert round-trip stability of the underlying
	// duration rather than a magic constant.
	qv := mustDecode(t, types.QValueKindTimeTZ, -1, `"13:45:30+00:00"`)
	if _, ok := qv.(types.QValueTimeTZ); !ok {
		t.Fatalf("timetz type = %T", qv)
	}
	qvZ := mustDecode(t, types.QValueKindTimeTZ, -1, `"13:45:30Z"`)
	if qv.Value() != qvZ.Value() {
		t.Fatalf("timetz +00:00 (%v) != Z (%v)", qv.Value(), qvZ.Value())
	}
}

func TestJSONToQValueNegativeInterval(t *testing.T) {
	t.Parallel()
	qv := mustDecode(t, types.QValueKindInterval, -1, `"-P1Y2DT3H"`)
	want := datatypes.PeerDBInterval{Years: -1, Days: -2, Hours: -3, Valid: true}
	wantJSON, _ := json.Marshal(want)
	if qv.Value() != string(wantJSON) {
		t.Fatalf("negative interval = %v, want %s", qv.Value(), wantJSON)
	}
}

func TestJSONToQValueMoreArrays(t *testing.T) {
	t.Parallel()

	if qv := mustDecode(t, types.QValueKindArrayFloat64, -1, `[1.5,2.5]`); len(qv.(types.QValueArrayFloat64).Val) != 2 {
		t.Fatalf("float64 array = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindArrayInt16, -1, `[1,2,3]`); len(qv.(types.QValueArrayInt16).Val) != 3 {
		t.Fatalf("int16 array = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindArrayInt32, -1, `[10,20]`); len(qv.(types.QValueArrayInt32).Val) != 2 {
		t.Fatalf("int32 array = %v", qv.Value())
	}
	if qv := mustDecode(t, types.QValueKindArrayFloat32, -1, `[1.5]`); qv.(types.QValueArrayFloat32).Val[0] != 1.5 {
		t.Fatalf("float32 array = %v", qv.Value())
	}

	dates := mustDecode(t, types.QValueKindArrayDate, -1, `["2025-01-01","2025-12-31"]`)
	if len(dates.(types.QValueArrayDate).Val) != 2 {
		t.Fatalf("date array = %v", dates.Value())
	}

	tss := mustDecode(t, types.QValueKindArrayTimestampTZ, -1, `["2025-01-01T00:00:00Z"]`)
	if !tss.(types.QValueArrayTimestampTZ).Val[0].Equal(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("timestamptz array = %v", tss.Value())
	}

	ivs := mustDecode(t, types.QValueKindArrayInterval, -1, `["PT1H","PT30M"]`)
	if len(ivs.(types.QValueArrayInterval).Val) != 2 {
		t.Fatalf("interval array = %v", ivs.Value())
	}
}

func TestJSONToQValueErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		kind types.QValueKind
		raw  string
	}{
		{types.QValueKindInt64, `"not-a-number"`},
		{types.QValueKindUUID, `"not-a-uuid"`},
		{types.QValueKindNumeric, `"abc"`},
		{types.QValueKindTimestamp, `"totally-not-a-date"`},
		{types.QValueKindArrayInt64, `"not-an-array"`},
	}
	for _, tc := range cases {
		if _, err := JSONToQValue(tc.kind, -1, json.RawMessage(tc.raw)); err == nil {
			t.Fatalf("expected error for kind=%s raw=%s", tc.kind, tc.raw)
		}
	}
}

func TestChangefeedJSONToRecordItems(t *testing.T) {
	t.Parallel()
	schema := &protos.TableSchema{
		TableIdentifier:   "public.products",
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "name", Type: string(types.QValueKindString)},
			{Name: "price", Type: string(types.QValueKindNumeric), TypeModifier: datatypes.MakeNumericTypmod(10, 2)},
			{Name: "active", Type: string(types.QValueKindBoolean)},
		},
	}

	value := `{
		"after": {"id": 7, "name": "Lamp", "price": 22.30, "active": true},
		"before": {"id": 7, "name": "Lamp", "price": 19.99, "active": false},
		"updated": "100.0000000000"
	}`
	ev, err := ParseEnvelope([]byte(value))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	before, after, err := ChangefeedJSONToRecordItems(schema, ev.Before, ev.After)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if after.Len() != 4 {
		t.Fatalf("after len = %d, want 4", after.Len())
	}
	if v := after.GetColumnValue("id"); v.Value() != int64(7) {
		t.Fatalf("after id = %v", v.Value())
	}
	if v := after.GetColumnValue("price"); v.(types.QValueNumeric).Val.String() != "22.3" {
		t.Fatalf("after price = %v", v.Value())
	}
	if v := before.GetColumnValue("price"); v.(types.QValueNumeric).Val.String() != "19.99" {
		t.Fatalf("before price = %v", v.Value())
	}
	// scale carried from typmod.
	if after.GetColumnValue("price").(types.QValueNumeric).Scale != 2 {
		t.Fatalf("price scale not carried")
	}
}

func TestChangefeedJSONToRecordItemsDelete(t *testing.T) {
	t.Parallel()
	schema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
		},
	}
	// delete: after is nil -> empty after items, before populated.
	before, after, err := ChangefeedJSONToRecordItems(schema, map[string]json.RawMessage{"id": json.RawMessage(`5`)}, nil)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if after.Len() != 0 {
		t.Fatalf("after len = %d, want 0", after.Len())
	}
	if before.GetColumnValue("id").Value() != int64(5) {
		t.Fatalf("before id = %v", before.GetColumnValue("id").Value())
	}
}
