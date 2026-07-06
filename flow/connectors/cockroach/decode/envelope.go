package decode

import (
	"encoding/json"
	"fmt"
)

// Operation classifies a changefeed row event.
type Operation uint8

const (
	// OpUnknown is a not-yet-classified/invalid operation.
	OpUnknown Operation = iota
	// OpInsert is a row insert (enriched "c").
	OpInsert
	// OpUpdate is a row update (enriched "u").
	OpUpdate
	// OpDelete is a row delete (enriched "d"; wrapped after==null).
	OpDelete
	// OpRead is an initial-scan/backfill row (enriched "r"). Consumers should
	// generally treat this like an insert.
	OpRead
)

func (o Operation) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	case OpRead:
		return "read"
	default:
		return "unknown"
	}
}

// EnvelopeKind identifies which changefeed envelope a message used.
type EnvelopeKind uint8

const (
	// EnvelopeWrapped is the default `WITH diff, updated` envelope:
	// {"after": {...}|null, "before": {...}|null, "updated": "<hlc>"}.
	EnvelopeWrapped EnvelopeKind = iota
	// EnvelopeEnriched is the CRDB >=25.2 Debezium-style envelope:
	// {"after": {...}, "op": "c"|"u"|"d"|"r", "source": {...}, "ts_ns": N}.
	EnvelopeEnriched
)

// Event is a parsed changefeed message. Exactly one of {Resolved, a row event}
// is meaningful: if Resolved is true the message is a resolved-timestamp
// checkpoint and Operation/Before/After are unset.
type Event struct {
	// Resolved is true for resolved-timestamp (high-water) checkpoint messages.
	Resolved bool
	// ResolvedHLC is the checkpoint timestamp, valid only when Resolved.
	ResolvedHLC HLC

	// Envelope records which envelope the message used (row events only).
	Envelope EnvelopeKind
	// Operation is the row change kind (row events only).
	Operation Operation
	// Before holds the pre-image column map (nil unless present & non-null).
	// Populated for updates/deletes when the changefeed ran WITH diff.
	Before map[string]json.RawMessage
	// After holds the post-image column map (nil for deletes).
	After map[string]json.RawMessage
	// Updated is the row commit HLC: from the wrapped `updated` field, or from
	// enriched `source.ts_hlc`. Zero if the changefeed omitted it.
	Updated HLC
	// TsNs is the enriched top-level ts_ns (changefeed processing time, not
	// commit time). Zero if absent. Do NOT use for ordering — use Updated.
	TsNs int64
}

// rawEnvelope is the union of fields across both envelopes. Using pointers /
// json.RawMessage lets us tell "field absent" from "field present and null",
// which is how we classify insert vs delete in the wrapped envelope.
type rawEnvelope struct {
	Resolved *string         `json:"resolved"`
	After    json.RawMessage `json:"after"`
	Before   json.RawMessage `json:"before"`
	Updated  *string         `json:"updated"`
	Op       *string         `json:"op"`
	Source   json.RawMessage `json:"source"`
	TsNs     *json.Number    `json:"ts_ns"`
	// Payload/Schema are the Debezium-style wrapper the enriched envelope adds
	// when the changefeed runs WITH enriched_properties='schema': the real
	// op/before/after/source/ts_ns live nested under "payload" alongside a
	// "schema" block. Both are present together only in that wrapper.
	Payload json.RawMessage `json:"payload"`
	Schema  json.RawMessage `json:"schema"`
}

type enrichedSource struct {
	TsHLC *string `json:"ts_hlc"`
}

// ParseEnvelope parses the raw changefeed `value` JSON bytes into an Event,
// auto-detecting the wrapped vs enriched envelope. For sinkless changefeeds the
// value column carries exactly this JSON; the key column (null for resolved
// messages) is redundant with the `resolved` field detected here.
func ParseEnvelope(value []byte) (Event, error) {
	if len(value) == 0 {
		return Event{}, fmt.Errorf("empty changefeed value")
	}

	var raw rawEnvelope
	if err := json.Unmarshal(value, &raw); err != nil {
		return Event{}, fmt.Errorf("failed to unmarshal changefeed envelope: %w", err)
	}

	// Resolved-timestamp checkpoint: {"resolved": "<hlc>"}. Resolved messages are
	// never Debezium-wrapped, so this stays flat even with enriched schema.
	if raw.Resolved != nil {
		hlc, err := ParseHLC(*raw.Resolved)
		if err != nil {
			return Event{}, fmt.Errorf("failed to parse resolved HLC: %w", err)
		}
		return Event{Resolved: true, ResolvedHLC: hlc}, nil
	}

	// Debezium schema-wrapped enriched envelope: unwrap "payload" and parse the
	// nested enriched message. Gated on the "schema" sibling so a wrapped-row
	// column named "payload" can't be mistaken for the wrapper.
	if len(raw.Payload) > 0 && len(raw.Schema) > 0 {
		var inner rawEnvelope
		if err := json.Unmarshal(raw.Payload, &inner); err != nil {
			return Event{}, fmt.Errorf("failed to unmarshal enriched payload: %w", err)
		}
		if inner.Op == nil {
			return Event{}, fmt.Errorf("enriched payload missing op")
		}
		return parseEnriched(inner)
	}

	if raw.Op != nil {
		return parseEnriched(raw)
	}
	return parseWrapped(raw)
}

// ParseKey decodes a sinkless changefeed's key column into a column-name ->
// raw-JSON-value map. It handles both key shapes CRDB emits:
//   - wrapped envelope: a JSON array of PK values ordered by pkColumns, e.g. [1]
//   - enriched envelope: a JSON object keyed by column name, e.g. {"id":2},
//     optionally Debezium-wrapped as {"schema":{...},"payload":{"id":2}}
//
// A null/empty key (as on resolved rows) yields a nil map. pkColumns is only
// consulted for the array form; for the array form its length must match.
func ParseKey(pkColumns []string, key []byte) (map[string]json.RawMessage, error) {
	trimmed := bytesTrimSpace(key)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return nil, nil
	}

	switch trimmed[0] {
	case '[':
		var vals []json.RawMessage
		if err := json.Unmarshal(trimmed, &vals); err != nil {
			return nil, fmt.Errorf("failed to unmarshal array key: %w", err)
		}
		if len(vals) != len(pkColumns) {
			return nil, fmt.Errorf("array key has %d values but %d PK columns", len(vals), len(pkColumns))
		}
		out := make(map[string]json.RawMessage, len(vals))
		for i, col := range pkColumns {
			out[col] = vals[i]
		}
		return out, nil
	case '{':
		var obj struct {
			Payload json.RawMessage `json:"payload"`
			Schema  json.RawMessage `json:"schema"`
		}
		// Peek for a Debezium wrapper (payload+schema) without losing the flat form.
		if err := json.Unmarshal(trimmed, &obj); err == nil && len(obj.Payload) > 0 && len(obj.Schema) > 0 {
			trimmed = obj.Payload
		}
		var out map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &out); err != nil {
			return nil, fmt.Errorf("failed to unmarshal object key: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected key JSON: %s", string(trimmed))
	}
}

func bytesTrimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && (b[start] == ' ' || b[start] == '\t' || b[start] == '\n' || b[start] == '\r') {
		start++
	}
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t' || b[end-1] == '\n' || b[end-1] == '\r') {
		end--
	}
	return b[start:end]
}

func parseEnriched(raw rawEnvelope) (Event, error) {
	ev := Event{Envelope: EnvelopeEnriched}

	switch *raw.Op {
	case "c":
		ev.Operation = OpInsert
	case "u":
		ev.Operation = OpUpdate
	case "d":
		ev.Operation = OpDelete
	case "r":
		ev.Operation = OpRead
	default:
		return Event{}, fmt.Errorf("unknown enriched op %q", *raw.Op)
	}

	after, _, err := decodeObject(raw.After)
	if err != nil {
		return Event{}, fmt.Errorf("failed to decode enriched after: %w", err)
	}
	before, _, err := decodeObject(raw.Before)
	if err != nil {
		return Event{}, fmt.Errorf("failed to decode enriched before: %w", err)
	}
	ev.After = after
	ev.Before = before

	// Commit timestamp lives in source.ts_hlc when enriched_properties=source.
	if len(raw.Source) > 0 {
		var src enrichedSource
		if err := json.Unmarshal(raw.Source, &src); err != nil {
			return Event{}, fmt.Errorf("failed to decode enriched source: %w", err)
		}
		if src.TsHLC != nil {
			hlc, err := ParseHLC(*src.TsHLC)
			if err != nil {
				return Event{}, fmt.Errorf("failed to parse source.ts_hlc: %w", err)
			}
			ev.Updated = hlc
		}
	}

	if raw.TsNs != nil {
		if n, err := raw.TsNs.Int64(); err == nil {
			ev.TsNs = n
		}
	}

	return ev, nil
}

func parseWrapped(raw rawEnvelope) (Event, error) {
	ev := Event{Envelope: EnvelopeWrapped}

	after, afterNull, err := decodeObject(raw.After)
	if err != nil {
		return Event{}, fmt.Errorf("failed to decode wrapped after: %w", err)
	}
	before, _, err := decodeObject(raw.Before)
	if err != nil {
		return Event{}, fmt.Errorf("failed to decode wrapped before: %w", err)
	}
	ev.After = after
	ev.Before = before

	switch {
	case afterNull:
		// after present and null => delete.
		ev.Operation = OpDelete
	case before == nil:
		// after non-null, before null or absent => insert. Note: without the
		// `diff` option before is always absent, so genuine updates also land
		// here and are indistinguishable from inserts (documented limitation;
		// the connector always requests `diff`).
		ev.Operation = OpInsert
	default:
		ev.Operation = OpUpdate
	}

	if raw.Updated != nil {
		hlc, err := ParseHLC(*raw.Updated)
		if err != nil {
			return Event{}, fmt.Errorf("failed to parse updated HLC: %w", err)
		}
		ev.Updated = hlc
	}

	return ev, nil
}

// decodeObject unmarshals a changefeed before/after value. It distinguishes an
// absent field (raw==nil) from an explicit JSON null: both yield a nil map, but
// only an explicit null sets isNull=true, which drives insert/delete
// classification in the wrapped envelope.
func decodeObject(raw json.RawMessage) (m map[string]json.RawMessage, isNull bool, err error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	if string(raw) == "null" {
		return nil, true, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, false, err
	}
	return m, false, nil
}
