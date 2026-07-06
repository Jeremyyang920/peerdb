package exceptions

// CockroachChangefeedError wraps a CockroachDB sinkless-changefeed failure that
// the connector has determined is irrecoverable from the persisted cursor: the
// changefeed cannot be resumed and the mirror needs a resync. This mirrors
// MySQLBinlogIncidentError (a resync-required signal that stays temporally
// retryable but alerts the user via classification).
//
// The distinct failure modes all surface from CRDB as SQLSTATE XXUUU with only
// the message differing, so Code carries the connector's classification for
// stable alerting instead of re-matching the message at the alerting layer.
type CockroachChangefeedError struct {
	error
	Code string
}

const (
	// CockroachChangefeedGCThreshold: the resume cursor precedes the earliest
	// MVCC history CRDB still retains (GC threshold / protected-timestamp
	// expiry). CRDB v25.x phrases this as "supplied backups do not cover
	// requested time"; older versions as "must be after replica GC threshold".
	CockroachChangefeedGCThreshold = "GC_THRESHOLD_EXCEEDED"
	// CockroachChangefeedTableTruncated: a watched table was TRUNCATEd, which
	// fails the changefeed ("<table>" was truncated).
	CockroachChangefeedTableTruncated = "TABLE_TRUNCATED"
	// CockroachChangefeedTableDropped: a watched table was DROPped, which fails
	// the changefeed. CRDB v25.x phrases this as `"<table>" was dropped`; v26.x as
	// `descriptor is being dropped`.
	CockroachChangefeedTableDropped = "TABLE_DROPPED"
)

func NewCockroachChangefeedError(err error, code string) *CockroachChangefeedError {
	return &CockroachChangefeedError{err, code}
}

func (e *CockroachChangefeedError) Error() string {
	return "CockroachDB changefeed irrecoverable error (" + e.Code + "): " + e.error.Error()
}

func (e *CockroachChangefeedError) Unwrap() error {
	return e.error
}
