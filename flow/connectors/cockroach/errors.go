package conncockroach

import (
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// changefeedIrrecoverableSubstrings maps a stable classification code to the
// substrings CRDB uses in the changefeed error message for that failure mode.
// All of these arrive as SQLSTATE XXUUU (uncategorized internal error), so the
// message is the only discriminator; the captured live forms are documented on
// the exceptions.CockroachChangefeed* codes.
var changefeedIrrecoverableSubstrings = []struct {
	code       string
	substrings []string
}{
	{
		code: exceptions.CockroachChangefeedGCThreshold,
		// v25.x: "...supplied backups do not cover requested time"; older CRDB:
		// "batch timestamp ... must be after replica GC threshold".
		substrings: []string{"supplied backups do not cover requested time", "must be after replica GC threshold"},
	},
	{
		code:       exceptions.CockroachChangefeedTableTruncated,
		substrings: []string{"was truncated"},
	},
	{
		code: exceptions.CockroachChangefeedTableDropped,
		// v25.x: `"<table>" was dropped`; v26.x: `descriptor is being dropped`. The
		// exact phrasing (and which of the two a DROP produces) depends on the CRDB
		// version and on DDL/feed timing, so match both — both mean the same
		// needs-resync condition.
		substrings: []string{"was dropped", "descriptor is being dropped"},
	},
}

// classifyChangefeedError inspects a changefeed stream error and, when it is a
// known irrecoverable condition (cursor past GC threshold, watched table
// truncated/dropped), returns the stable classification code and true. Callers
// use this to stop the in-loop reconnect ladder — retrying these against the
// same cursor is pointless, so PullRecords should surface them terminally so the
// mirror can be resynced.
func classifyChangefeedError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	for _, c := range changefeedIrrecoverableSubstrings {
		for _, sub := range c.substrings {
			if strings.Contains(msg, sub) {
				return c.code, true
			}
		}
	}
	return "", false
}
