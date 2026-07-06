// Package decode contains the pure (connection-free) decoding library for the
// CockroachDB source connector: HLC timestamps, changefeed envelope parsing,
// CRDB type -> QValueKind mapping, and changefeed-JSON -> QValue conversion.
//
// Everything here depends only on the standard library plus the shared qvalue
// type packages, so it can be unit tested in isolation and reused by both the
// sinkless (v1) and webhook-sink (v2) pull paths.
package decode

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// HLC is a CockroachDB hybrid-logical-clock timestamp as emitted by changefeeds
// and by cluster_logical_timestamp(). On the wire it is a decimal string of the
// form "<wall_nanos>.<logical>", e.g. "1746045115619002000.0000000000", where
// the integer part is nanoseconds since the Unix epoch (wall clock) and the
// fractional part is a zero-padded logical counter (CRDB pads it to 10 digits).
//
// The fractional part is NOT a decimal fraction: "…0000000001" means logical=1,
// not 1e-10. We therefore parse/format via integers only and never round-trip
// through float64, which would silently lose precision on the 19-digit wall
// component.
type HLC struct {
	// Wall is nanoseconds since the Unix epoch.
	Wall int64
	// Logical is the logical (tiebreak) counter within a wall nanosecond.
	Logical int32
}

// ParseHLC parses a CRDB HLC decimal string. It accepts an optional fractional
// (logical) component; a bare integer (no dot) is treated as logical 0.
func ParseHLC(s string) (HLC, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return HLC{}, fmt.Errorf("empty HLC string")
	}

	wallStr, logicalStr, hasDot := strings.Cut(s, ".")
	wall, err := strconv.ParseInt(wallStr, 10, 64)
	if err != nil {
		return HLC{}, fmt.Errorf("invalid HLC wall component %q: %w", wallStr, err)
	}

	var logical int64
	if hasDot && logicalStr != "" {
		// Leading zeros are significant only as padding; ParseInt handles them.
		logical, err = strconv.ParseInt(logicalStr, 10, 32)
		if err != nil {
			return HLC{}, fmt.Errorf("invalid HLC logical component %q: %w", logicalStr, err)
		}
	}

	return HLC{Wall: wall, Logical: int32(logical)}, nil
}

// String renders the HLC back to the canonical CRDB decimal form with a
// 10-digit zero-padded logical component. For any HLC that originated from CRDB
// this is an exact round-trip of ParseHLC.
func (h HLC) String() string {
	return fmt.Sprintf("%d.%010d", h.Wall, h.Logical)
}

// IsZero reports whether the HLC is the zero value.
func (h HLC) IsZero() bool {
	return h.Wall == 0 && h.Logical == 0
}

// Compare orders two HLCs: wall first, then logical. Returns -1, 0, or 1.
func (h HLC) Compare(o HLC) int {
	if h.Wall != o.Wall {
		if h.Wall < o.Wall {
			return -1
		}
		return 1
	}
	if h.Logical != o.Logical {
		if h.Logical < o.Logical {
			return -1
		}
		return 1
	}
	return 0
}

// Less reports whether h is strictly before o.
func (h HLC) Less(o HLC) bool {
	return h.Compare(o) < 0
}

// Time converts the wall component to a time.Time in UTC. The logical counter
// is intentionally dropped; this is only meaningful for coarse lag metrics
// (e.g. resolved-HLC age), never for equality or ordering — use Compare for
// those.
func (h HLC) Time() time.Time {
	return time.Unix(0, h.Wall).UTC()
}
