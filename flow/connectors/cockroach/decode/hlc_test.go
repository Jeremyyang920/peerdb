package decode

import (
	"testing"
	"time"
)

func TestParseHLC(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		in          string
		wantWall    int64
		wantLogical int32
		wantErr     bool
	}{
		{name: "changefeed updated", in: "1746045115619002000.0000000000", wantWall: 1746045115619002000, wantLogical: 0},
		{name: "cluster_logical_timestamp", in: "1701102296662969433.0000000000", wantWall: 1701102296662969433, wantLogical: 0},
		{name: "nonzero logical", in: "1746045115619002000.0000000005", wantWall: 1746045115619002000, wantLogical: 5},
		{name: "bare integer", in: "1746045115619002000", wantWall: 1746045115619002000, wantLogical: 0},
		{name: "trailing dot", in: "123.", wantWall: 123, wantLogical: 0},
		{name: "whitespace", in: "  123.0000000001 ", wantWall: 123, wantLogical: 1},
		{name: "empty", in: "", wantErr: true},
		{name: "not a number", in: "abc.def", wantErr: true},
		{name: "bad logical", in: "123.xyz", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseHLC(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %+v", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Wall != tc.wantWall || got.Logical != tc.wantLogical {
				t.Fatalf("ParseHLC(%q) = {%d,%d}, want {%d,%d}", tc.in, got.Wall, got.Logical, tc.wantWall, tc.wantLogical)
			}
		})
	}
}

func TestHLCStringRoundTrip(t *testing.T) {
	t.Parallel()
	// Canonical CRDB forms must round-trip exactly through parse->String.
	for _, s := range []string{
		"1746045115619002000.0000000000",
		"1701102296662969433.0000000000",
		"1746045115619002000.0000000005",
		"0.0000000000",
	} {
		hlc, err := ParseHLC(s)
		if err != nil {
			t.Fatalf("ParseHLC(%q): %v", s, err)
		}
		if got := hlc.String(); got != s {
			t.Fatalf("round-trip %q -> %q", s, got)
		}
	}
}

func TestHLCCompare(t *testing.T) {
	t.Parallel()
	mk := func(w int64, l int32) HLC { return HLC{Wall: w, Logical: l} }
	tests := []struct {
		a, b HLC
		want int
	}{
		{mk(100, 0), mk(200, 0), -1},
		{mk(200, 0), mk(100, 0), 1},
		{mk(100, 0), mk(100, 0), 0},
		{mk(100, 1), mk(100, 2), -1},
		{mk(100, 5), mk(100, 3), 1},
		{mk(100, 5), mk(101, 0), -1},
	}
	for _, tc := range tests {
		if got := tc.a.Compare(tc.b); got != tc.want {
			t.Fatalf("Compare(%v,%v) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
		if got := tc.a.Less(tc.b); got != (tc.want < 0) {
			t.Fatalf("Less(%v,%v) = %v, want %v", tc.a, tc.b, got, tc.want < 0)
		}
	}
}

func TestHLCTime(t *testing.T) {
	t.Parallel()
	hlc := HLC{Wall: 1746045115619002000, Logical: 7}
	got := hlc.Time()
	want := time.Unix(0, 1746045115619002000).UTC()
	if !got.Equal(want) {
		t.Fatalf("Time() = %v, want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("Time() location = %v, want UTC", got.Location())
	}
}
