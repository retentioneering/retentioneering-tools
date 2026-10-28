-- GoogleSQL. Replace project/dataset and property timezone before running.
-- Daily tables only; freeze a complete snapshot after the late-arrival window.
CREATE OR REPLACE TABLE `project.analytics.events_normalized` AS
WITH raw AS (
    SELECT user_pseudo_id AS user_id, event_name AS event,
        TIMESTAMP_MICROS(event_timestamp) AS timestamp,
        -- Reproducible tie order, NOT a GA4 globally unique event identifier.
        TO_JSON_STRING(t) AS source_row
    FROM `project.analytics_123.events_*` t
    WHERE _TABLE_SUFFIX BETWEEN '20260801' AND '20261016'
        AND REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\d{8}$')
        AND user_pseudo_id IS NOT NULL
)
SELECT user_id, event, timestamp,
    CAST(ROW_NUMBER() OVER (
        PARTITION BY user_id ORDER BY timestamp, source_row
    ) AS STRING) AS event_id
FROM raw;
-- Identical source rows are retained. Deduplicate by a verified ingestion key,
-- never blindly by (user_id, timestamp, event). Audit excluded NULL identities.
-- Widen suffix dates if the property's local dates differ from the UTC scan.
