package conncockroach

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PeerDB-io/peerdb/flow/connectors/cockroach/decode"
)

// MVCC history protection for the CockroachDB source connector.
//
// The initial snapshot reads AS OF SYSTEM TIME t₀ (the changefeed cursor
// captured in SetupReplication) and the changefeed later resumes from
// cursor=t₀. If the snapshot outlives the source's gc.ttlseconds (default 4h),
// GC advances past t₀: the AOST reads start failing and the changefeed cursor
// becomes permanently unresumable. To let a long snapshot outlive gc.ttlseconds
// we pin MVCC history at t₀ with CockroachDB's protected-timestamp builtins:
//
//   - crdb_internal.protect_mvcc_history(<hlc>, <window>::interval, <desc>)
//     creates a cluster-wide protected-timestamp record fixed at <hlc> plus a
//     HISTORY RETENTION job that AUTO-EXPIRES after <window> unless extended.
//     Returns the job id.
//   - crdb_internal.extend_mvcc_history_protection(<job_id>) prolongs expiry.
//   - CANCEL JOB <job_id> removes it (async: canceled → reverting → the PTS
//     record disappears).
//
// Scope: the record's cluster target translates to a span config over the
// ENTIRE keyspace (the tenant keyspace on Serverless), so system tables are
// pinned too. That is required — AOST reads and the changefeed catch-up scan
// resolve historical descriptors from system.descriptor at t₀ — but it means
// MVCC garbage accrues cluster-wide (including churny system tables like
// system.jobs) while the protection is live, same blast radius as a
// full-cluster BACKUP's protected timestamp. The release-on-first-resolved
// and auto-expiry logic below keeps that window tight.
//
// The job's description ("History Retention for peerdb <flow_job_name>") makes
// the SOURCE cluster the durable registry of our job id — no PeerDB catalog
// schema change is needed; we rediscover the job by description.
//
// Version split (verified empirically on live containers): on v25.4.x these
// builtins work WITHOUT any session gate, but v26.1+ restricts crdb_internal /
// system access via the new `unsafesql` package — a bare call there fails with
// SQLSTATE 42501 "Access to crdb_internal and system is restricted" unless the
// session first runs `SET allow_unsafe_internals = true`. Older versions do not
// have that session var at all. So we always attempt the SET and ignore its
// error, which is correct for both: harmless where the var is absent, and
// required on v26.1+. On v26+ each such call is additionally logged to
// CockroachDB's SENSITIVE_ACCESS audit channel as an UnsafeInternalsAccessed
// event; that is expected and attributable to PeerDB via our job description.
//
// The builtins themselves may be absent on older CockroachDB (they are what
// MOLT Fetch uses; roughly v23.2+) — we treat "unknown function" (42883) as a
// graceful-degrade signal so a mirror never fails merely because protection is
// unavailable. A 42501 from the builtin call (after the SET has been attempted)
// likewise degrades, but is remediated distinctly (privilege vs. session gate;
// see protectionRemediation).

const (
	// historyRetentionJobType is the SHOW JOBS job_type CockroachDB uses for the
	// protected-timestamp history-retention job that protect_mvcc_history spawns.
	historyRetentionJobType = "HISTORY RETENTION"
	// historyRetentionDescPrefix is the prefix CockroachDB prepends to the
	// description we pass protect_mvcc_history; we reconstruct the full stored
	// description to rediscover the job by description.
	historyRetentionDescPrefix = "History Retention for "
	// protectionDescNamespace namespaces our descriptions so a PeerDB job is
	// unambiguous among any other history-retention jobs on the cluster.
	protectionDescNamespace = "peerdb "
	// defaultHistoryProtectionWindowSeconds is the protection window used when the
	// peer config leaves history_protection_window_seconds unset.
	defaultHistoryProtectionWindowSeconds = 86400 // 24h
	// protectionExtendThrottle bounds how often the QRep path re-extends the
	// protection job; extends are cheap but there is no reason to issue one on
	// every partition.
	protectionExtendThrottle = 5 * time.Minute
)

// errProtectionUnsupported is the sentinel wrapped by protection helpers when
// MVCC history protection is unavailable on this cluster/user (the builtins do
// not exist, or the user lacks the REPLICATION privilege). Callers log a loud
// one-time warning and continue WITHOUT protection rather than failing the
// mirror. Test with errors.Is(err, errProtectionUnsupported).
var errProtectionUnsupported = errors.New("cockroachdb mvcc history protection unsupported")

// protectionDescription builds the description passed to protect_mvcc_history
// for a flow. CockroachDB stores it prefixed (see fullProtectionDescription).
// The flow name is only ever passed as a bound query parameter, never spliced
// into SQL, so no quoting is required here; we validate it is non-empty so we
// never create an unfindable job.
func protectionDescription(flowJobName string) (string, error) {
	if flowJobName == "" {
		return "", errors.New("cannot protect mvcc history for an empty flow job name")
	}
	return protectionDescNamespace + flowJobName, nil
}

// fullProtectionDescription is the description as CockroachDB stores it in
// SHOW JOBS / system.jobs, i.e. what we match on to rediscover the job.
func fullProtectionDescription(flowJobName string) (string, error) {
	desc, err := protectionDescription(flowJobName)
	if err != nil {
		return "", err
	}
	return historyRetentionDescPrefix + desc, nil
}

// validateProtectionWindow rejects a non-positive window so we never ask
// CockroachDB to create an already-expired (or absurd) protection record.
func validateProtectionWindow(window time.Duration) error {
	if window <= 0 {
		return fmt.Errorf("history protection window must be positive, got %s", window)
	}
	return nil
}

// windowInterval renders a duration as a CockroachDB interval literal (seconds),
// e.g. 24h -> "86400s". Passed as a bound parameter cast to ::interval.
func windowInterval(window time.Duration) string {
	return fmt.Sprintf("%ds", int64(window.Seconds()))
}

// enableUnsafeInternals runs the v26.1+ session gate required before touching
// crdb_internal/system interfaces (see the package doc's version split). On
// v25.4.x and earlier the session var does not exist, so the error is
// intentionally ignored — the subsequent builtin call is the real capability
// probe. When the SET does apply (v26.1+) it lifts the unsafesql restriction
// for the rest of the session; a later 42501 then means a genuine privilege
// gap, not the restriction.
func enableUnsafeInternals(ctx context.Context, conn *pgx.Conn) {
	_, _ = conn.Exec(ctx, "SET allow_unsafe_internals = true")
}

// classifyProtectionError maps a builtin failure that means "protection is not
// available here" (missing builtin, or insufficient privilege) onto
// errProtectionUnsupported so callers can degrade gracefully. Any other error
// is returned unchanged.
func classifyProtectionError(err error) error {
	if err == nil {
		return nil
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case pgerrcode.UndefinedFunction, pgerrcode.InsufficientPrivilege:
			return fmt.Errorf("%w: %w", errProtectionUnsupported, err)
		}
	}
	return err
}

// protectHistory pins MVCC history at t₀ with a protected-timestamp record and
// its auto-expiring history-retention job, returning the job id. An
// errProtectionUnsupported error (see classifyProtectionError) means the caller
// should warn and continue without protection.
func protectHistory(
	ctx context.Context, conn *pgx.Conn, t0 decode.HLC, window time.Duration, flowJobName string,
) (int64, error) {
	if err := validateProtectionWindow(window); err != nil {
		return 0, err
	}
	desc, err := protectionDescription(flowJobName)
	if err != nil {
		return 0, err
	}

	enableUnsafeInternals(ctx, conn)

	var jobID int64
	// t₀ is rendered from parsed HLC integer components (decode.HLC.String) so the
	// ::decimal literal can only be <int>.<int>; window and description are bound
	// parameters. Nothing here is attacker-controlled SQL text.
	if err := conn.QueryRow(ctx,
		"SELECT crdb_internal.protect_mvcc_history($1::decimal, $2::interval, $3)",
		t0.String(), windowInterval(window), desc,
	).Scan(&jobID); err != nil {
		return 0, classifyProtectionError(err)
	}
	return jobID, nil
}

// findProtectionJob rediscovers a flow's active (running/paused) history
// retention job by its stored description. Returns found=false when there is
// none (including when the job has already been canceled/expired). An
// errProtectionUnsupported error means the builtins/tables are unavailable.
func findProtectionJob(ctx context.Context, conn *pgx.Conn, flowJobName string) (jobID int64, found bool, err error) {
	fullDesc, err := fullProtectionDescription(flowJobName)
	if err != nil {
		return 0, false, err
	}

	enableUnsafeInternals(ctx, conn)

	// SHOW JOBS surfaces a job for a while after it finishes; restrict to states
	// where the protection is still live so we never try to extend a dead job.
	row := conn.QueryRow(ctx,
		`SELECT job_id FROM [SHOW JOBS]
		 WHERE job_type = $1 AND description = $2 AND status IN ('running', 'paused', 'pause-requested')
		 ORDER BY created DESC LIMIT 1`,
		historyRetentionJobType, fullDesc)
	if err := row.Scan(&jobID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, classifyProtectionError(err)
	}
	return jobID, true, nil
}

// extendProtection prolongs the expiration of an existing history-retention job.
func extendProtection(ctx context.Context, conn *pgx.Conn, jobID int64) error {
	enableUnsafeInternals(ctx, conn)
	if _, err := conn.Exec(ctx,
		"SELECT crdb_internal.extend_mvcc_history_protection($1)", jobID); err != nil {
		return classifyProtectionError(err)
	}
	return nil
}

// cancelProtectionByFlow removes a flow's protection job if one is active. It is
// a no-op when no active job is found (already canceled/expired, or never
// created), and safe to call repeatedly. Cancellation is asynchronous on the
// CockroachDB side (the job goes canceled → reverting before the PTS record
// disappears); this returns once CANCEL JOB is accepted.
func cancelProtectionByFlow(ctx context.Context, conn *pgx.Conn, flowJobName string) error {
	jobID, found, err := findProtectionJob(ctx, conn, flowJobName)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	// CANCEL JOB does not accept a bound parameter for the job id, so interpolate
	// the int64 discovered from SHOW JOBS (never attacker-controlled).
	if _, err := conn.Exec(ctx, fmt.Sprintf("CANCEL JOB %d", jobID)); err != nil {
		return classifyProtectionError(err)
	}
	return nil
}

// --- connector-level protection lifecycle ---

// historyProtectionWindow resolves the configured protection window. An unset
// config field defaults to 24h; an explicit 0 disables protection (enabled=false).
func (c *CockroachConnector) historyProtectionWindow() (window time.Duration, enabled bool) {
	seconds := uint32(defaultHistoryProtectionWindowSeconds)
	if c.config.HistoryProtectionWindowSeconds != nil {
		seconds = *c.config.HistoryProtectionWindowSeconds
	}
	if seconds == 0 {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

// protectionRemediation returns operator guidance tailored to WHY protection is
// unavailable, so the one-time warning points at the right fix. The two 42501
// forms are deliberately distinguished: the unsafesql "restricted" message means
// the v26.1+ session gate did not take effect (a session/version problem, not a
// grant), whereas any other 42501 means the connecting user simply lacks the
// REPLICATION privilege.
func protectionRemediation(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case pgerrcode.UndefinedFunction:
			return "Upgrade the source to CockroachDB v23.2+ (the protect_mvcc_history builtins do not exist here), " +
				"or raise gc.ttlseconds on the mirrored tables to cover the expected snapshot duration."
		case pgerrcode.InsufficientPrivilege:
			if strings.Contains(pgErr.Message, "restricted") {
				return "The session could not enable allow_unsafe_internals (unexpected on CockroachDB v26.1+), " +
					"so crdb_internal access stayed restricted. Raise gc.ttlseconds on the mirrored tables instead."
			}
			return "Grant the connecting user the REPLICATION privilege (GRANT SYSTEM REPLICATION TO <user>), " +
				"or raise gc.ttlseconds on the mirrored tables to cover the expected snapshot duration."
		}
	}
	return "Ensure the source is CockroachDB v23.2+ and the connecting user holds the REPLICATION privilege, " +
		"or raise gc.ttlseconds on the mirrored tables to cover the expected snapshot duration."
}

// warnProtectionUnsupported emits a single loud warning (per connector) that the
// mirror is proceeding WITHOUT MVCC history protection, with remediation tailored
// to the cause (see protectionRemediation).
func (c *CockroachConnector) warnProtectionUnsupported(context string, err error) {
	if c.protectionUnsupportedWarned.Swap(true) {
		return
	}
	c.logger.Warn("[cockroach] MVCC history protection is UNAVAILABLE; proceeding without it. "+
		"A snapshot or CDC catch-up longer than the source gc.ttlseconds may fail and require a resync. "+
		protectionRemediation(err),
		slog.String("context", context), slog.Any("error", err))
}

// protectFlowHistory creates the protection job for a flow after t₀ is captured.
// Protection failures never fail the mirror: an unsupported condition warns
// loudly once, any other error warns and continues.
func (c *CockroachConnector) protectFlowHistory(ctx context.Context, t0 decode.HLC, flowJobName string) {
	window, enabled := c.historyProtectionWindow()
	if !enabled {
		c.logger.Info("[cockroach] MVCC history protection disabled by config (window=0)",
			slog.String("flowJobName", flowJobName))
		return
	}
	jobID, err := protectHistory(ctx, c.conn, t0, window, flowJobName)
	if err != nil {
		if errors.Is(err, errProtectionUnsupported) {
			c.warnProtectionUnsupported("SetupReplication", err)
		} else {
			c.logger.Warn("[cockroach] failed to protect MVCC history at t0; continuing without protection",
				slog.String("flowJobName", flowJobName), slog.Any("error", err))
		}
		return
	}
	c.logger.Info("[cockroach] protected MVCC history at t0",
		slog.String("flowJobName", flowJobName), slog.String("t0", t0.String()),
		slog.Int64("jobID", jobID), slog.Duration("window", window))
}

// maybeExtendProtection prolongs the flow's protection job while the snapshot
// runs, throttled to at most once per protectionExtendThrottle. Extend failures
// never fail the snapshot; they warn and continue.
func (c *CockroachConnector) maybeExtendProtection(ctx context.Context, flowJobName string) {
	if _, enabled := c.historyProtectionWindow(); !enabled || flowJobName == "" {
		return
	}

	c.protectionMu.Lock()
	if !c.lastProtectionExtend.IsZero() && time.Since(c.lastProtectionExtend) < protectionExtendThrottle {
		c.protectionMu.Unlock()
		return
	}
	c.lastProtectionExtend = time.Now()
	c.protectionMu.Unlock()

	jobID, found, err := findProtectionJob(ctx, c.conn, flowJobName)
	if err != nil {
		if errors.Is(err, errProtectionUnsupported) {
			c.warnProtectionUnsupported("GetQRepPartitions/PullQRepRecords", err)
		} else {
			c.logger.Warn("[cockroach] failed to look up MVCC history protection job to extend",
				slog.String("flowJobName", flowJobName), slog.Any("error", err))
		}
		return
	}
	if !found {
		// No live job: SetupReplication may have degraded, or this is a qrep-only
		// mirror with no CDC-captured t₀. Nothing to extend.
		return
	}
	if err := extendProtection(ctx, c.conn, jobID); err != nil {
		c.logger.Warn("[cockroach] failed to extend MVCC history protection",
			slog.String("flowJobName", flowJobName), slog.Int64("jobID", jobID), slog.Any("error", err))
		return
	}
	c.logger.Info("[cockroach] extended MVCC history protection",
		slog.String("flowJobName", flowJobName), slog.Int64("jobID", jobID))
}

// maybeReleaseProtection cancels the flow's protection job once CDC has
// persisted a resolved checkpoint strictly greater than the cursor the feed
// started from: at that point the snapshot is done and the changefeed resumes
// from the newer resolved, so the pre-t₀ history it pinned is no longer needed.
// Fires at most once per connector lifetime.
//
// Steady-state CDC intentionally remains gc.ttlseconds-bounded (like MySQL
// binlog retention); ratcheting protection forward with the changefeed cursor
// is possible future work.
func (c *CockroachConnector) maybeReleaseProtection(ctx context.Context, flowJobName, startCursor, resolvedText string) {
	if _, enabled := c.historyProtectionWindow(); !enabled || flowJobName == "" || startCursor == "" {
		return
	}
	startHLC, err := decode.ParseHLC(startCursor)
	if err != nil {
		return
	}
	resolvedHLC, err := decode.ParseHLC(resolvedText)
	if err != nil {
		return
	}
	if resolvedHLC.Compare(startHLC) <= 0 {
		return
	}
	c.protectionReleaseOnce.Do(func() {
		if err := cancelProtectionByFlow(ctx, c.conn, flowJobName); err != nil {
			if errors.Is(err, errProtectionUnsupported) {
				c.warnProtectionUnsupported("PullRecords release", err)
			} else {
				c.logger.Warn("[cockroach] failed to release MVCC history protection after CDC caught up",
					slog.String("flowJobName", flowJobName), slog.Any("error", err))
			}
			return
		}
		c.logger.Info("[cockroach] released MVCC history protection; CDC advanced past the snapshot start",
			slog.String("flowJobName", flowJobName), slog.String("startCursor", startCursor),
			slog.String("resolved", resolvedText))
	})
}
