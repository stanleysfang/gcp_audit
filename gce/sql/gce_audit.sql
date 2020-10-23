
WITH
log_raw AS (
    SELECT
        resource.labels.project_id AS project_id,
        resource.labels.instance_id AS instance_id,
        REGEXP_EXTRACT(protopayload_auditlog.resourceName, r'instances/(.+)$') AS instance_name,
        protopayload_auditlog.authenticationInfo.principalEmail AS username,
        TIMESTAMP(REGEXP_REPLACE(STRING(timestamp, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')) AS ts, -- TIMESTAMP_SUB(timestamp, INTERVAL 8 HOUR)
        REGEXP_EXTRACT(protopayload_auditlog.methodName, r'instances\.(.+)$') AS event
    FROM `stanleysfang.monitoring_logging.cloudaudit_googleapis_com_activity`
    WHERE resource.type = 'gce_instance' AND operation.last = TRUE
    
    UNION ALL
    
    SELECT
        resource.labels.project_id AS project_id,
        resource.labels.instance_id AS instance_id,
        jsonPayload.resource.name AS instance_name,
        jsonPayload.actor.user AS username,
        TIMESTAMP(REGEXP_REPLACE(STRING(timestamp, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')) AS ts, -- TIMESTAMP_SUB(timestamp, INTERVAL 8 HOUR),
        REGEXP_EXTRACT(jsonPayload.event_subtype, r'instances\.(.+)$') AS event
    FROM `stanleysfang.monitoring_logging.compute_googleapis_com_activity_log_*`
    WHERE jsonPayload.event_type = 'GCE_OPERATION_DONE'
),
log_categorize_event_2_action AS (
    SELECT *
    FROM (
        SELECT
            *,
            CASE
                WHEN event IN('start', 'insert') THEN 'start'
                WHEN event IN('stop', 'delete') THEN 'stop'
                WHEN event IN('setMetadata') THEN 'filter_out'
                ELSE 'OTHER'
            END AS action
        FROM log_raw
    )
    WHERE action != 'filter_out'
)

SELECT
    project_id,
    instance_id, instance_name, CONCAT(instance_name, ':', SUBSTR(CAST(instance_id AS STRING), 1, 6)) AS instance,
    last_action_ts,
    CONCAT(day, ' day ', hr, ' hr ', min, ' min ', sec, ' sec') AS uptime, uptime_sec,
    start_by, start_ts,
    stop_by, stop_ts,
    insert_by, insert_ts,
    delete_by, delete_ts
FROM (
    SELECT
        * EXCEPT(insert_by, insert_ts, delete_by, delete_ts),
        MAX(insert_by) OVER(PARTITION BY project_id, instance_id, instance_name) AS insert_by,
        MAX(insert_ts) OVER(PARTITION BY project_id, instance_id, instance_name) AS insert_ts,
        MAX(delete_by) OVER(PARTITION BY project_id, instance_id, instance_name) AS delete_by,
        MAX(delete_ts) OVER(PARTITION BY project_id, instance_id, instance_name) AS delete_ts,
        CAST(MOD(uptime_sec, 60) AS STRING) AS sec,
        CAST(MOD(CAST(FLOOR(uptime_sec/60) AS INT64), 60) AS STRING) AS min,
        CAST(MOD(CAST(FLOOR(uptime_sec/60/60) AS INT64), 24) AS STRING) AS hr,
        CAST(CAST(FLOOR(uptime_sec/60/60/24) AS INT64) AS STRING) AS day,
        IF(stop_ts IS NULL, start_ts, stop_ts) AS last_action_ts
        -- MAX(ts) OVER(PARTITION BY project_id, instance_id, instance_name) AS last_action_ts
    FROM (
        SELECT
            *,
            CASE
                WHEN action = 'start' AND next_action = 'stop' THEN TIMESTAMP_DIFF(next_ts, ts, SECOND)
                WHEN action = 'start' AND next_action IS NULL THEN TIMESTAMP_DIFF(TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')), ts, SECOND)
                ELSE NULL
            END AS uptime_sec,
            IF(action = 'start', username, NULL) AS start_by,
            IF(action = 'start', ts, NULL) AS start_ts,
            IF(action = 'start' AND next_action = 'stop', next_username, NULL) AS stop_by,
            IF(action = 'start' AND next_action = 'stop', next_ts, NULL) AS stop_ts,
            IF(event = 'insert', username, NULL) AS insert_by,
            IF(event = 'insert', ts, NULL) AS insert_ts,
            IF(event = 'delete', username, NULL) AS delete_by,
            IF(event = 'delete', ts, NULL) AS delete_ts
        FROM (
            SELECT
                *,
                LEAD(username) OVER(PARTITION BY project_id, instance_id, instance_name ORDER BY ts ASC) AS next_username,
                LEAD(ts) OVER(PARTITION BY project_id, instance_id, instance_name ORDER BY ts ASC) AS next_ts,
                LEAD(action) OVER(PARTITION BY project_id, instance_id, instance_name ORDER BY ts ASC) AS next_action
            FROM (
                SELECT
                    *,
                    LAG(action) OVER(PARTITION BY project_id, instance_id, instance_name ORDER BY ts ASC) AS prev_action
                FROM log_categorize_event_2_action
            )
            WHERE action != prev_action OR prev_action IS NULL OR event IN('insert', 'delete')
        )
    )
)
WHERE uptime_sec IS NOT NULL
