package decode

import (
	"testing"
)

func TestParseEnvelopeResolved(t *testing.T) {
	t.Parallel()
	ev, err := ParseEnvelope([]byte(`{"resolved":"1746045115619002000.0000000000"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ev.Resolved {
		t.Fatalf("expected resolved event")
	}
	if ev.ResolvedHLC.Wall != 1746045115619002000 {
		t.Fatalf("resolved HLC = %v", ev.ResolvedHLC)
	}
}

func TestParseEnvelopeWrapped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		value   string
		wantOp  Operation
		hasAft  bool
		hasBef  bool
		updated int64
	}{
		{
			name:   "insert (before absent)",
			value:  `{"after":{"id":1,"name":"a"},"updated":"100.0000000000"}`,
			wantOp: OpInsert, hasAft: true, updated: 100,
		},
		{
			name:   "insert (before null)",
			value:  `{"after":{"id":1},"before":null,"updated":"100.0000000000"}`,
			wantOp: OpInsert, hasAft: true,
		},
		{
			name:   "update (diff)",
			value:  `{"after":{"id":1,"name":"b"},"before":{"id":1,"name":"a"},"updated":"200.0000000000"}`,
			wantOp: OpUpdate, hasAft: true, hasBef: true, updated: 200,
		},
		{
			name:   "delete",
			value:  `{"after":null,"before":{"id":1,"name":"a"},"updated":"300.0000000000"}`,
			wantOp: OpDelete, hasBef: true, updated: 300,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ev, err := ParseEnvelope([]byte(tc.value))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ev.Resolved {
				t.Fatalf("did not expect resolved")
			}
			if ev.Envelope != EnvelopeWrapped {
				t.Fatalf("envelope = %v, want wrapped", ev.Envelope)
			}
			if ev.Operation != tc.wantOp {
				t.Fatalf("op = %v, want %v", ev.Operation, tc.wantOp)
			}
			if (ev.After != nil) != tc.hasAft {
				t.Fatalf("after present = %v, want %v", ev.After != nil, tc.hasAft)
			}
			if (ev.Before != nil) != tc.hasBef {
				t.Fatalf("before present = %v, want %v", ev.Before != nil, tc.hasBef)
			}
			if tc.updated != 0 && ev.Updated.Wall != tc.updated {
				t.Fatalf("updated = %v, want wall %d", ev.Updated, tc.updated)
			}
		})
	}
}

func TestParseEnvelopeEnriched(t *testing.T) {
	t.Parallel()
	value := `{
		"after": {"id": "58390d92-2472-43e1-86bc-1642395e8dad", "name": "Bluetooth Speaker", "price": 45.00},
		"op": "c",
		"source": {
			"cluster_id": "585e6512-ea54-490a-8f1d-50c8d182a2e6",
			"table_name": "products",
			"ts_hlc": "1746045115619002000.0000000000",
			"ts_ns": 1746045115619002000
		},
		"ts_ns": 1746045115679811000
	}`
	ev, err := ParseEnvelope([]byte(value))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Envelope != EnvelopeEnriched {
		t.Fatalf("envelope = %v, want enriched", ev.Envelope)
	}
	if ev.Operation != OpInsert {
		t.Fatalf("op = %v, want insert", ev.Operation)
	}
	if ev.After == nil || len(ev.After) != 3 {
		t.Fatalf("after = %v", ev.After)
	}
	if ev.Updated.Wall != 1746045115619002000 {
		t.Fatalf("updated (from source.ts_hlc) = %v", ev.Updated)
	}
	if ev.TsNs != 1746045115679811000 {
		t.Fatalf("ts_ns = %d", ev.TsNs)
	}
}

func TestParseEnvelopeEnrichedOps(t *testing.T) {
	t.Parallel()
	cases := map[string]Operation{
		`{"after":{"id":1},"op":"c"}`:                   OpInsert,
		`{"after":{"id":1},"before":{"id":1},"op":"u"}`: OpUpdate,
		`{"after":null,"before":{"id":1},"op":"d"}`:     OpDelete,
		`{"after":{"id":1},"op":"r"}`:                   OpRead,
	}
	for value, wantOp := range cases {
		ev, err := ParseEnvelope([]byte(value))
		if err != nil {
			t.Fatalf("value %s: %v", value, err)
		}
		if ev.Operation != wantOp {
			t.Fatalf("value %s: op = %v, want %v", value, ev.Operation, wantOp)
		}
	}
}

func TestParseEnvelopeErrors(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		``,
		`not json`,
		`{"op":"x","after":{}}`,
		`{"resolved":"not-a-number"}`,
	} {
		if _, err := ParseEnvelope([]byte(value)); err == nil {
			t.Fatalf("expected error for %q", value)
		}
	}
}
