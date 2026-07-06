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

	// Resolved-timestamp checkpoint: {"resolved": "<hlc>"}.
	if raw.Resolved != nil {
		hlc, err := ParseHLC(*raw.Resolved)
		if err != nil {
			return Event{}, fmt.Errorf("failed to parse resolved HLC: %w", err)
		}
		return Event{Resolved: true, ResolvedHLC: hlc}, nil
	}

	if raw.Op != nil {
		return parseEnriched(raw)
	}
	return parseWrapped(raw)
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
