
WITH
dt_arr_table AS (
    SELECT
        project_id, instance_id, instance_name, start_ts, stop_ts, uptime_sec,
        IF(stop_ts IS NULL, GENERATE_DATE_ARRAY(DATE(start_ts), CURRENT_DATE('-08')), GENERATE_DATE_ARRAY(DATE(start_ts), DATE(stop_ts))) AS dt_arr,
        TIMESTAMP_DIFF(TIMESTAMP_TRUNC(TIMESTAMP_ADD(start_ts, INTERVAL 1 DAY), DAY), start_ts, SECOND) AS first_dt_sec,
        IF(stop_ts IS NULL, TIMESTAMP_DIFF(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 8 HOUR), TIMESTAMP(CURRENT_DATE('-08')), SECOND), TIMESTAMP_DIFF(stop_ts, TIMESTAMP_TRUNC(stop_ts, DAY), SECOND)) AS last_dt_sec
    FROM `stanleysfang.monitoring_logging.gce_audit`
)

SELECT
    project_id, instance_id, instance_name, CONCAT(instance_name, ':', SUBSTR(CAST(instance_id AS STRING), 1, 4)) AS instance, dt,
    SUM(uptime_hr) AS uptime_hr
FROM (
    SELECT
        * EXCEPT(uptime_sec),
        uptime_sec/60/60 AS uptime_hr
    FROM (
        SELECT
            project_id, instance_id, instance_name, dt,
            CASE
                WHEN DATE(start_ts) = CURRENT_DATE('-08') THEN uptime_sec
                WHEN DATE(start_ts) = DATE(stop_ts) THEN uptime_sec
                WHEN rn_asc = 1 THEN first_dt_sec
                WHEN rn_desc = 1 THEN last_dt_sec
                ELSE 86400
            END AS uptime_sec
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY project_id, instance_id, instance_name, start_ts ORDER BY dt ASC) AS rn_asc,
                ROW_NUMBER() OVER(PARTITION BY project_id, instance_id, instance_name, start_ts ORDER BY dt DESC) AS rn_desc
            FROM (
                SELECT * EXCEPT(dt_arr)
                FROM dt_arr_table, UNNEST(dt_arr_table.dt_arr) AS dt
            )
        )
    )
)
GROUP BY 1,2,3,4,5
