package decode

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// jsonNullSentinel is the object CRDB emits for a JSON `null` *value* (as
// opposed to a SQL NULL) when the changefeed runs WITH encode_json_value_null_as_object.
// It lets us disambiguate the two: a SQL NULL yields QValueNull, a JSON null
// value yields QValueJSON{Val:"null"}.
const jsonNullSentinel = `{"__crdb_json_null__": true}`

// timestampLayouts / timestampTZLayouts are the formats CRDB uses when encoding
// temporal types into changefeed JSON. TIMESTAMP is emitted without a zone
// (e.g. "2025-04-30T20:02:35.40316"); TIMESTAMPTZ carries a Z or numeric offset
// (RFC3339). These are best-effort and flagged for e2e verification.
var timestampLayouts = []string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
}

var timestampTZLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05Z07:00",
}

// JSONToQValue decodes a single changefeed-JSON column value into a QValue,
// keyed by the column's QValueKind (from FieldDescription.Type) and its
// TypeModifier (needed for numeric precision/scale). A nil/absent raw value or
// an explicit JSON `null` decodes to QValueNull(kind); the JSON-null-vs-SQL-null
// distinction for JSONB columns is handled via jsonNullSentinel.
func JSONToQValue(kind types.QValueKind, typmod int32, raw json.RawMessage) (types.QValue, error) {
	// SQL NULL: field absent or literal JSON null. For JSONB we must not treat
	// a literal null as SQL NULL here — it is handled inside the JSONB arm.
	if len(raw) == 0 {
		return types.QValueNull(kind), nil
	}
	if string(raw) == "null" && kind != types.QValueKindJSON && kind != types.QValueKindJSONB {
		return types.QValueNull(kind), nil
	}

	if kind.IsArray() {
		return decodeArray(kind, typmod, raw)
	}

	switch kind {
	case types.QValueKindBoolean:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return nil, fmt.Errorf("bool: %w", err)
		}
		return types.QValueBoolean{Val: b}, nil
	case types.QValueKindInt8:
		i, err := decodeInt(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueInt8{Val: int8(i)}, nil
	case types.QValueKindInt16:
		i, err := decodeInt(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueInt16{Val: int16(i)}, nil
	case types.QValueKindInt32:
		i, err := decodeInt(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueInt32{Val: int32(i)}, nil
	case types.QValueKindInt64:
		i, err := decodeInt(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueInt64{Val: i}, nil
	case types.QValueKindFloat32:
		f, err := decodeFloat(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueFloat32{Val: float32(f)}, nil
	case types.QValueKindFloat64:
		f, err := decodeFloat(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueFloat64{Val: f}, nil
	case types.QValueKindNumeric:
		d, err := decodeDecimal(raw)
		if err != nil {
			return nil, err
		}
		precision, scale := common.ParseNumericTypmod(typmod)
		return types.QValueNumeric{Val: d, Precision: precision, Scale: scale}, nil
	case types.QValueKindString:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueString{Val: s}, nil
	case types.QValueKindEnum:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueEnum{Val: s}, nil
	case types.QValueKindQChar:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		if len(s) == 0 {
			return types.QValueQChar{Val: 0}, nil
		}
		return types.QValueQChar{Val: s[0]}, nil
	case types.QValueKindBytes:
		b, err := decodeBytes(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueBytes{Val: b}, nil
	case types.QValueKindUUID:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("uuid: %w", err)
		}
		return types.QValueUUID{Val: id}, nil
	case types.QValueKindINET:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueINET{Val: s}, nil
	case types.QValueKindGeometry:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueGeometry{Val: s}, nil
	case types.QValueKindGeography:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueGeography{Val: s}, nil
	case types.QValueKindDate:
		t, ok, err := decodeTemporal(raw, []string{"2006-01-02"})
		if err != nil || !ok {
			return nullOr(kind, err)
		}
		return types.QValueDate{Val: t}, nil
	case types.QValueKindTimestamp:
		t, ok, err := decodeTemporal(raw, timestampLayouts)
		if err != nil || !ok {
			return nullOr(kind, err)
		}
		return types.QValueTimestamp{Val: t}, nil
	case types.QValueKindTimestampTZ:
		t, ok, err := decodeTemporal(raw, timestampTZLayouts)
		if err != nil || !ok {
			return nullOr(kind, err)
		}
		return types.QValueTimestampTZ{Val: t.UTC()}, nil
	case types.QValueKindTime:
		d, err := decodeTimeOfDay(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueTime{Val: d}, nil
	case types.QValueKindTimeTZ:
		d, err := decodeTimeTZ(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueTimeTZ{Val: d}, nil
	case types.QValueKindInterval:
		s, err := decodeString(raw)
		if err != nil {
			return nil, err
		}
		iv, err := parseInterval(s)
		if err != nil {
			return nil, err
		}
		return types.QValueInterval{Val: iv}, nil
	case types.QValueKindJSON, types.QValueKindJSONB:
		return decodeJSONValue(raw)
	default:
		// Unknown/unsupported kind: preserve the raw text so nothing is lost.
		s, err := decodeString(raw)
		if err != nil {
			return types.QValueString{Val: string(raw)}, nil
		}
		return types.QValueString{Val: s}, nil
	}
}

// ChangefeedJSONToRecordItems converts the before/after column maps of a parsed
// changefeed row into RecordItems, using the QValueKind and TypeModifier carried
// on each schema column (FieldDescription.Type holds the QValueKind string, as
// populated by the schema-introspection path). Either map may be nil (e.g.
// before is nil for inserts, after is nil for deletes), yielding an empty
// RecordItems for that side.
func ChangefeedJSONToRecordItems(
	schema *protos.TableSchema,
	before, after map[string]json.RawMessage,
) (beforeItems, afterItems model.RecordItems, err error) {
	beforeItems, err = changefeedRowToRecordItems(schema, before)
	if err != nil {
		return model.RecordItems{}, model.RecordItems{}, fmt.Errorf("before: %w", err)
	}
	afterItems, err = changefeedRowToRecordItems(schema, after)
	if err != nil {
		return model.RecordItems{}, model.RecordItems{}, fmt.Errorf("after: %w", err)
	}
	return beforeItems, afterItems, nil
}

// changefeedRowToRecordItems decodes one column map against the table schema.
// Only columns present in the schema are decoded; columns missing from the row
// map are skipped (CRDB emits every column, so this only trims extras).
func changefeedRowToRecordItems(
	schema *protos.TableSchema,
	row map[string]json.RawMessage,
) (model.RecordItems, error) {
	items := model.NewRecordItems(len(schema.GetColumns()))
	if row == nil {
		return items, nil
	}
	for _, col := range schema.GetColumns() {
		raw, ok := row[col.Name]
		if !ok {
			continue
		}
		qv, err := JSONToQValue(types.QValueKind(col.Type), col.TypeModifier, raw)
		if err != nil {
			return model.RecordItems{}, fmt.Errorf("column %q (%s): %w", col.Name, col.Type, err)
		}
		items.AddColumn(col.Name, qv)
	}
	return items, nil
}

// nullOr returns QValueNull for the kind when there was no decode error (e.g.
// an infinity sentinel we deliberately drop), otherwise the error.
func nullOr(kind types.QValueKind, err error) (types.QValue, error) {
	if err != nil {
		return nil, err
	}
	return types.QValueNull(kind), nil
}

// --- scalar helpers ---

// rawToNumber extracts a json.Number from raw, tolerating a quoted-string form
// (some drivers/versions emit large INT8 as strings). Using json.Number rather
// than float64 preserves full precision for values beyond 2^53.
func rawToNumber(raw json.RawMessage) (json.Number, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return json.Number(strings.TrimSpace(s)), nil
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return "", err
	}
	return n, nil
}

func decodeInt(raw json.RawMessage) (int64, error) {
	n, err := rawToNumber(raw)
	if err != nil {
		return 0, fmt.Errorf("int: %w", err)
	}
	i, err := n.Int64()
	if err != nil {
		return 0, fmt.Errorf("int %q: %w", n.String(), err)
	}
	return i, nil
}

func decodeFloat(raw json.RawMessage) (float64, error) {
	n, err := rawToNumber(raw)
	if err != nil {
		return 0, fmt.Errorf("float: %w", err)
	}
	f, err := n.Float64()
	if err != nil {
		return 0, fmt.Errorf("float %q: %w", n.String(), err)
	}
	return f, nil
}

func decodeDecimal(raw json.RawMessage) (decimal.Decimal, error) {
	// CRDB emits DECIMAL as an unquoted JSON number (e.g. 22.30). json.Number
	// keeps the exact textual form so decimal parsing is lossless.
	n, err := rawToNumber(raw)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("decimal: %w", err)
	}
	d, err := decimal.NewFromString(n.String())
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("decimal %q: %w", n.String(), err)
	}
	return d, nil
}

func decodeString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("string: %w", err)
	}
	return s, nil
}

// decodeBytes decodes CRDB's BYTES encoding. The SQL default is hex with a
// literal "\x" prefix inside the JSON string (e.g. "\\x48aa"); we also accept
// standard base64 as a fallback. Which one CRDB emits in changefeed JSON is
// flagged for e2e verification.
func decodeBytes(raw json.RawMessage) ([]byte, error) {
	s, err := decodeString(raw)
	if err != nil {
		return nil, err
	}
	if rest, ok := strings.CutPrefix(s, `\x`); ok {
		b, err := hex.DecodeString(rest)
		if err != nil {
			return nil, fmt.Errorf("bytes hex: %w", err)
		}
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	// Not hex-prefixed and not valid base64: treat the literal string as bytes.
	return []byte(s), nil
}

// decodeTemporal parses a JSON string against the given layouts. It returns
// ok=false (with nil error) for the CRDB infinity/-infinity sentinels, which we
// deliberately map to NULL since this connection-free layer has no destination
// default-time context.
func decodeTemporal(raw json.RawMessage, layouts []string) (time.Time, bool, error) {
	s, err := decodeString(raw)
	if err != nil {
		return time.Time{}, false, err
	}
	switch strings.ToLower(s) {
	case "infinity", "-infinity":
		return time.Time{}, false, nil
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("failed to parse temporal value %q", s)
}

// decodeTimeOfDay parses a "HH:MM:SS[.ffffff]" string to a duration since
// midnight, matching QValueTime semantics.
func decodeTimeOfDay(raw json.RawMessage) (time.Duration, error) {
	s, err := decodeString(raw)
	if err != nil {
		return 0, err
	}
	// CRDB allows the extreme value 24:00:00; clamp like the postgres connector.
	s = strings.Replace(s, "24:00:00", "23:59:59.999999", 1)
	t, err := time.Parse("15:04:05.999999999", s)
	if err != nil {
		return 0, fmt.Errorf("time: %w", err)
	}
	return time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second +
		time.Duration(t.Nanosecond()), nil
}

// decodeTimeTZ parses "HH:MM:SS[.ffffff]±TZ" to a duration, mirroring the
// postgres connector's TIMETZ handling (time-of-day in UTC as an offset from
// year 0000).
func decodeTimeTZ(raw json.RawMessage) (time.Duration, error) {
	s, err := decodeString(raw)
	if err != nil {
		return 0, err
	}
	s = strings.Replace(s, "24:00:00", "23:59:59.999999", 1)
	layouts := []string{"15:04:05.999999999Z07:00", "15:04:05Z07:00", "15:04:05.999999999-07", "15:04:05-07"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Sub(shared.Year0000), nil
		}
	}
	return 0, fmt.Errorf("failed to parse timetz value %q", s)
}

// decodeJSONValue handles JSON/JSONB passthrough. It distinguishes a SQL NULL
// (raw is empty) from a JSON null value, and recognises the
// encode_json_value_null_as_object sentinel that CRDB uses to make that
// distinction explicit on the wire.
func decodeJSONValue(raw json.RawMessage) (types.QValue, error) {
	compact := strings.Join(strings.Fields(string(raw)), "")
	if compact == strings.Join(strings.Fields(jsonNullSentinel), "") {
		return types.QValueJSON{Val: "null"}, nil
	}
	isArray := len(compact) > 0 && compact[0] == '['
	return types.QValueJSON{Val: string(raw), IsArray: isArray}, nil
}

// --- interval parsing ---

// parseInterval converts a CRDB interval string into the PeerDBInterval JSON
// representation used by QValueInterval (matching the postgres connector's
// intervalToString output). It accepts both the ISO-8601 duration form
// ("P1Y2M3DT4H5M6S") and the postgres verbose form ("1 year 2 mons 3 days
// 04:05:06"). Which form CRDB emits in changefeed JSON is flagged for e2e
// verification; both are handled.
func parseInterval(s string) (string, error) {
	s = strings.TrimSpace(s)
	var iv datatypes.PeerDBInterval
	iv.Valid = true

	var err error
	if strings.HasPrefix(s, "P") || strings.HasPrefix(s, "-P") || strings.HasPrefix(s, "+P") {
		err = parseISO8601Interval(s, &iv)
	} else {
		err = parsePostgresInterval(s, &iv)
	}
	if err != nil {
		return "", err
	}

	out, err := json.Marshal(iv)
	if err != nil {
		return "", fmt.Errorf("interval marshal: %w", err)
	}
	return string(out), nil
}

func parseISO8601Interval(s string, iv *datatypes.PeerDBInterval) error {
	neg := false
	if rest, ok := strings.CutPrefix(s, "-"); ok {
		neg, s = true, rest
	} else {
		s = strings.TrimPrefix(s, "+")
	}
	body, ok := strings.CutPrefix(s, "P")
	if !ok {
		return fmt.Errorf("invalid ISO-8601 interval %q", s)
	}

	datePart, timePart, _ := strings.Cut(body, "T")

	parseSection := func(section string, isTime bool) error {
		var num strings.Builder
		for _, r := range section {
			if (r >= '0' && r <= '9') || r == '.' || r == '-' {
				num.WriteRune(r)
				continue
			}
			valStr := num.String()
			num.Reset()
			if valStr == "" {
				return fmt.Errorf("interval unit %q without value in %q", string(r), s)
			}
			if isTime && r == 'S' {
				f, err := parseFloat(valStr)
				if err != nil {
					return err
				}
				iv.Seconds = f
				continue
			}
			n, err := parseIntStr(valStr)
			if err != nil {
				return err
			}
			switch {
			case !isTime && r == 'Y':
				iv.Years = n
			case !isTime && r == 'M':
				iv.Months = n
			case !isTime && r == 'W':
				iv.Days += n * 7
			case !isTime && r == 'D':
				iv.Days += n
			case isTime && r == 'H':
				iv.Hours = n
			case isTime && r == 'M':
				iv.Minutes = n
			default:
				return fmt.Errorf("unexpected interval unit %q in %q", string(r), s)
			}
		}
		if num.Len() != 0 {
			return fmt.Errorf("trailing interval value %q in %q", num.String(), s)
		}
		return nil
	}

	if err := parseSection(datePart, false); err != nil {
		return err
	}
	if err := parseSection(timePart, true); err != nil {
		return err
	}

	if neg {
		iv.Years, iv.Months, iv.Days = -iv.Years, -iv.Months, -iv.Days
		iv.Hours, iv.Minutes, iv.Seconds = -iv.Hours, -iv.Minutes, -iv.Seconds
	}
	return nil
}

func parsePostgresInterval(s string, iv *datatypes.PeerDBInterval) error {
	fields := strings.Fields(s)
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		// A clock component "HH:MM:SS[.ffffff]", optionally signed.
		if strings.Contains(f, ":") {
			if err := parseClock(f, iv); err != nil {
				return err
			}
			continue
		}
		// Otherwise expect "<number> <unit>".
		n, err := parseIntStr(f)
		if err != nil {
			return fmt.Errorf("interval %q: %w", s, err)
		}
		if i+1 >= len(fields) {
			return fmt.Errorf("interval %q: value %d without unit", s, n)
		}
		unit := strings.ToLower(strings.TrimSuffix(fields[i+1], "s"))
		i++
		switch unit {
		case "year", "yr":
			iv.Years = n
		case "mon", "month":
			iv.Months = n
		case "week", "wk":
			iv.Days += n * 7
		case "day":
			iv.Days += n
		case "hour", "hr":
			iv.Hours = n
		case "min", "minute":
			iv.Minutes = n
		case "sec", "second":
			iv.Seconds = float64(n)
		default:
			return fmt.Errorf("interval %q: unknown unit %q", s, unit)
		}
	}
	return nil
}

func parseClock(f string, iv *datatypes.PeerDBInterval) error {
	neg := false
	if rest, ok := strings.CutPrefix(f, "-"); ok {
		neg, f = true, rest
	}
	parts := strings.SplitN(f, ":", 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid clock component %q", f)
	}
	h, err := parseIntStr(parts[0])
	if err != nil {
		return err
	}
	m, err := parseIntStr(parts[1])
	if err != nil {
		return err
	}
	sec, err := parseFloat(parts[2])
	if err != nil {
		return err
	}
	if neg {
		h, m, sec = -h, -m, -sec
	}
	iv.Hours, iv.Minutes, iv.Seconds = h, m, sec
	return nil
}

func parseIntStr(s string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q: %w", s, err)
	}
	return n, nil
}

func parseFloat(s string) (float64, error) {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float %q: %w", s, err)
	}
	return f, nil
}

// --- array decoding ---

func decodeArray(kind types.QValueKind, typmod int32, raw json.RawMessage) (types.QValue, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, fmt.Errorf("array %s: %w", kind, err)
	}

	switch kind {
	case types.QValueKindArrayBoolean:
		out := make([]bool, len(elems))
		for i, e := range elems {
			var b bool
			if err := json.Unmarshal(e, &b); err != nil {
				return nil, fmt.Errorf("array bool[%d]: %w", i, err)
			}
			out[i] = b
		}
		return types.QValueArrayBoolean{Val: out}, nil
	case types.QValueKindArrayInt16:
		out := make([]int16, len(elems))
		for i, e := range elems {
			v, err := decodeInt(e)
			if err != nil {
				return nil, err
			}
			out[i] = int16(v)
		}
		return types.QValueArrayInt16{Val: out}, nil
	case types.QValueKindArrayInt32:
		out := make([]int32, len(elems))
		for i, e := range elems {
			v, err := decodeInt(e)
			if err != nil {
				return nil, err
			}
			out[i] = int32(v)
		}
		return types.QValueArrayInt32{Val: out}, nil
	case types.QValueKindArrayInt64:
		out := make([]int64, len(elems))
		for i, e := range elems {
			v, err := decodeInt(e)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return types.QValueArrayInt64{Val: out}, nil
	case types.QValueKindArrayFloat32:
		out := make([]float32, len(elems))
		for i, e := range elems {
			v, err := decodeFloat(e)
			if err != nil {
				return nil, err
			}
			out[i] = float32(v)
		}
		return types.QValueArrayFloat32{Val: out}, nil
	case types.QValueKindArrayFloat64:
		out := make([]float64, len(elems))
		for i, e := range elems {
			v, err := decodeFloat(e)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return types.QValueArrayFloat64{Val: out}, nil
	case types.QValueKindArrayNumeric:
		out := make([]decimal.Decimal, len(elems))
		for i, e := range elems {
			v, err := decodeDecimal(e)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		precision, scale := common.ParseNumericTypmod(typmod)
		return types.QValueArrayNumeric{Val: out, Precision: precision, Scale: scale}, nil
	case types.QValueKindArrayString:
		out, err := decodeStringArray(elems)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayString{Val: out}, nil
	case types.QValueKindArrayEnum:
		out, err := decodeStringArray(elems)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayEnum{Val: out}, nil
	case types.QValueKindArrayUUID:
		out := make([]uuid.UUID, len(elems))
		for i, e := range elems {
			s, err := decodeString(e)
			if err != nil {
				return nil, err
			}
			id, err := uuid.Parse(s)
			if err != nil {
				return nil, fmt.Errorf("array uuid[%d]: %w", i, err)
			}
			out[i] = id
		}
		return types.QValueArrayUUID{Val: out}, nil
	case types.QValueKindArrayDate, types.QValueKindArrayTimestamp, types.QValueKindArrayTimestampTZ:
		layouts := timestampLayouts
		switch kind {
		case types.QValueKindArrayDate:
			layouts = []string{"2006-01-02"}
		case types.QValueKindArrayTimestampTZ:
			layouts = timestampTZLayouts
		}
		out := make([]time.Time, len(elems))
		for i, e := range elems {
			t, ok, err := decodeTemporal(e, layouts)
			if err != nil {
				return nil, err
			}
			if !ok {
				out[i] = time.Time{}
				continue
			}
			if kind == types.QValueKindArrayTimestampTZ {
				t = t.UTC()
			}
			out[i] = t
		}
		switch kind {
		case types.QValueKindArrayDate:
			return types.QValueArrayDate{Val: out}, nil
		case types.QValueKindArrayTimestamp:
			return types.QValueArrayTimestamp{Val: out}, nil
		default:
			return types.QValueArrayTimestampTZ{Val: out}, nil
		}
	case types.QValueKindArrayInterval:
		out := make([]string, len(elems))
		for i, e := range elems {
			s, err := decodeString(e)
			if err != nil {
				return nil, err
			}
			iv, err := parseInterval(s)
			if err != nil {
				return nil, err
			}
			out[i] = iv
		}
		return types.QValueArrayInterval{Val: out}, nil
	default:
		// ArrayJSON/JSONB and any unhandled array kind: keep element text.
		out := make([]string, len(elems))
		for i, e := range elems {
			out[i] = string(e)
		}
		return types.QValueArrayString{Val: out}, nil
	}
}

func decodeStringArray(elems []json.RawMessage) ([]string, error) {
	out := make([]string, len(elems))
	for i, e := range elems {
		s, err := decodeString(e)
		if err != nil {
			return nil, fmt.Errorf("array string[%d]: %w", i, err)
		}
		out[i] = s
	}
	return out, nil
}
