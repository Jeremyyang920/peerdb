package conncockroach

import "testing"

func TestParseCRDBVersion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		major   int
		minor   int
		wantErr bool
	}{
		{
			name:  "ccl build string",
			input: "CockroachDB CCL v25.4.12 (aarch64-unknown-linux-gnu, built 2026/06/24 12:28:17, go1.23.12)",
			major: 25, minor: 4,
		},
		{
			name:  "oss build string",
			input: "CockroachDB OSS v24.1.0 (x86_64-pc-linux-gnu, built 2024/05/20)",
			major: 24, minor: 1,
		},
		{
			name:  "no build variant token",
			input: "CockroachDB v23.2.5 (x86_64-apple-darwin)",
			major: 23, minor: 2,
		},
		{
			name:    "postgres version string is rejected",
			input:   "PostgreSQL 16.2 on x86_64-pc-linux-gnu",
			wantErr: true,
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			major, minor, err := parseCRDBVersion(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got major=%d minor=%d", tc.input, major, minor)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if major != tc.major || minor != tc.minor {
				t.Fatalf("parseCRDBVersion(%q) = %d.%d, want %d.%d", tc.input, major, minor, tc.major, tc.minor)
			}
		})
	}
}
