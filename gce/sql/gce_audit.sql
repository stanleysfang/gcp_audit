SELECT
    project_id, instance_id, instance_name, CONCAT(instance_name, ':', SUBSTR(CAST(instance_id AS STRING), 1, 4)) AS instance, DATE(start_ts) AS dt,
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
        CAST(CAST(FLOOR(uptime_sec/60/60/24) AS INT64) AS STRING) AS day
    FROM (
        SELECT
            *,
            CASE
                WHEN action = 'start' AND next_action = 'stop' THEN TIMESTAMP_DIFF(next_ts, ts, SECOND)
                WHEN action = 'start' AND next_action IS NULL THEN TIMESTAMP_DIFF(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 8 HOUR), ts, SECOND)
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
                    -- ROW_NUMBER() OVER(PARTITION BY project_id, instance_id, instance_name ORDER BY ts ASC) AS rn_asc,
                    -- ROW_NUMBER() OVER(PARTITION BY project_id, instance_id, instance_name ORDER BY ts DESC) AS rn_desc,
                    LAG(action) OVER(PARTITION BY project_id, instance_id, instance_name ORDER BY ts ASC) AS prev_action
                FROM (
                    SELECT
                        *,
                        CASE
                            WHEN event IN('start', 'insert', 'migrateOnHostMaintenance') THEN 'start'
                            WHEN event IN('stop', 'delete') THEN 'stop'
                            ELSE 'OTHER'
                        END AS action
                    FROM (
                        SELECT 
                            project_id AS project_id,
                            instance_id AS instance_id,
                            name AS instance_name,
                            user AS username,
                            TIMESTAMP_SUB(timestamp, INTERVAL 8 HOUR) AS ts,
                            REGEXP_EXTRACT(event_subtype, r'^compute\.instances\.(.+)$') AS event
                        FROM (
                            SELECT resource.labels.project_id, resource.labels.instance_id, jsonPayload.resource.name, jsonPayload.actor.user, timestamp, jsonPayload.event_subtype
                            FROM `stanleysfang.monitoring_logging.compute_googleapis_com_activity_log`
                            WHERE jsonPayload.event_type = 'GCE_OPERATION_DONE'
                            
                            UNION ALL
                            
                            SELECT resource.labels.project_id, resource.labels.instance_id, jsonPayload.resource.name, jsonPayload.actor.user, timestamp, jsonPayload.event_subtype
                            FROM `stanleysfang.monitoring_logging.compute_googleapis_com_activity_log_*`
                            WHERE jsonPayload.event_type = 'GCE_OPERATION_DONE'
                        )
                    )
                )
            )
            WHERE action != prev_action OR prev_action IS NULL OR event IN('insert', 'delete')
        )
    )
)
WHERE uptime_sec IS NOT NULL