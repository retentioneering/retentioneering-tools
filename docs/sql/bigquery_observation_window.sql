-- Run after validating the observation coverage contract in the guide.
-- Input: user_id STRING, event STRING, timestamp UTC (microsecond precision),
-- event_id STRING unique within user. Resolve ingestion duplicates upstream.
-- BigQuery: GoogleSQL. ClickHouse: timestamp DateTime64(6, 'UTC').
WITH params AS (
    SELECT UNIX_MICROS(TIMESTAMP '2026-09-01 00:00:00+00') AS analysis_start_us,
        UNIX_MICROS(TIMESTAMP '2026-10-01 00:00:00+00') AS analysis_end_us,
        UNIX_MICROS(TIMESTAMP '2026-08-01 00:00:00+00') AS observed_start_us,
        UNIX_MICROS(TIMESTAMP '2026-10-16 00:00:00+00') AS observed_end_us,
        1800000000 AS timeout_us,
        1209600000000 AS horizon_us
),
source AS (
    SELECT user_id, event, event_id, UNIX_MICROS(timestamp) AS ts_us
    FROM `project.analytics.events_normalized`
),
extended AS (
    SELECT source.*, p.*
    FROM source CROSS JOIN params p
    WHERE ts_us >= observed_start_us AND ts_us < observed_end_us
),
cohort AS (
    SELECT DISTINCT user_id FROM extended
    WHERE ts_us >= analysis_start_us AND ts_us < analysis_end_us
),
ordered AS (
    SELECT e.*,
        LAG(ts_us) OVER (
            PARTITION BY e.user_id ORDER BY ts_us, event_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS previous_us
    FROM extended e INNER JOIN cohort c ON e.user_id = c.user_id
),
numbered AS (
    SELECT *, SUM(CASE WHEN previous_us IS NULL OR ts_us - previous_us > timeout_us
                      THEN 1 ELSE 0 END) OVER (
        PARTITION BY user_id ORDER BY ts_us, event_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS observed_session_index
    FROM ordered
),
sessions AS (
    SELECT user_id, observed_session_index,
        MIN(ts_us) AS session_start_us, MAX(ts_us) AS session_end_us,
        COUNT(*) AS session_event_count
    FROM numbered GROUP BY user_id, observed_session_index
),
neighbors AS (
    SELECT *,
        LAG(session_end_us) OVER w AS previous_session_end_us,
        LEAD(session_start_us) OVER w AS next_session_start_us
    FROM sessions
    WINDOW w AS (PARTITION BY user_id ORDER BY observed_session_index
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
user_context AS (
    SELECT user_id,
        MAX(CASE WHEN ts_us < analysis_start_us THEN ts_us END) AS previous_event_us,
        MIN(CASE WHEN ts_us >= analysis_end_us THEN ts_us END) AS next_event_us,
        MAX(CASE WHEN ts_us >= analysis_start_us AND ts_us < analysis_end_us
                 THEN ts_us END) AS anchor_us
    FROM numbered GROUP BY user_id
),
enriched AS (
    SELECT e.user_id, e.event, e.event_id, TIMESTAMP_MICROS(e.ts_us) AS timestamp,
        e.ts_us, e.observed_session_index,
        e.analysis_start_us, e.analysis_end_us,
        e.observed_start_us, e.observed_end_us, e.timeout_us, e.horizon_us,
        e.ts_us >= e.analysis_start_us AND e.ts_us < e.analysis_end_us AS is_core,
        s.session_start_us, s.session_end_us, s.session_event_count,
        s.previous_session_end_us, s.next_session_start_us,
        s.previous_session_end_us IS NULL
            AND s.session_start_us - e.observed_start_us < e.timeout_us
            AS session_left_censored,
        s.next_session_start_us IS NULL
            AND e.observed_end_us <= s.session_end_us + e.timeout_us
            AS session_right_censored,
        u.previous_event_us, u.next_event_us, u.anchor_us,
        CASE WHEN u.next_event_us <= u.anchor_us + e.horizon_us THEN 'returned'
             WHEN e.observed_end_us > u.anchor_us + e.horizon_us THEN 'inactive_14d'
             ELSE 'right_censored' END AS outcome
    FROM numbered e
    INNER JOIN neighbors s ON e.user_id = s.user_id
        AND e.observed_session_index = s.observed_session_index
    INNER JOIN user_context u ON e.user_id = u.user_id
)
-- Compact export: only real core events, with external history as columns.
-- For the full context export, remove WHERE is_core (keep the same CTEs).
SELECT * FROM enriched WHERE is_core
ORDER BY user_id, ts_us, event_id;
