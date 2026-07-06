package conncockroach

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// TestClassifyChangefeedError pins the substring matching to the exact error
// strings captured live from CockroachDB v25.4 (all arrive as SQLSTATE XXUUU).
func TestClassifyChangefeedError(t *testing.T) {
	cases := []struct {
		name      string
		msg       string
		wantCode  string
		wantMatch bool
	}{
		{
			name: "gc threshold v25 phrasing",
			msg: `failed to resolve targets in the CHANGEFEED stmt: table "defaultdb.public.p_gc" ` +
				`does not exist: supplied backups do not cover requested time`,
			wantCode:  exceptions.CockroachChangefeedGCThreshold,
			wantMatch: true,
		},
		{
			name:      "gc threshold classic phrasing",
			msg:       "batch timestamp 123.0 must be after replica GC threshold 456.0",
			wantCode:  exceptions.CockroachChangefeedGCThreshold,
			wantMatch: true,
		},
		{
			name:      "table truncated",
			msg:       `"trunc_probe" was truncated`,
			wantCode:  exceptions.CockroachChangefeedTableTruncated,
			wantMatch: true,
		},
		{
			name:      "table dropped v25 phrasing",
			msg:       `"drop_probe" was dropped`,
			wantCode:  exceptions.CockroachChangefeedTableDropped,
			wantMatch: true,
		},
		{
			name:      "table dropped v26 phrasing",
			msg:       "descriptor is being dropped",
			wantCode:  exceptions.CockroachChangefeedTableDropped,
			wantMatch: true,
		},
		{
			name:      "transient connection error is not irrecoverable",
			msg:       "failed to receive message: unexpected EOF",
			wantMatch: false,
		},
		{
			name:      "conn closed is not irrecoverable",
			msg:       "conn closed",
			wantMatch: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, ok := classifyChangefeedError(errors.New(tc.msg))
			require.Equal(t, tc.wantMatch, ok)
			if tc.wantMatch {
				require.Equal(t, tc.wantCode, code)
			}
		})
	}

	code, ok := classifyChangefeedError(nil)
	require.False(t, ok)
	require.Empty(t, code)
}
