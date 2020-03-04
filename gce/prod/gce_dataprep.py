
from google.cloud import bigquery
import pandas as pd

project_id = 'stanleysfang'
client = bigquery.Client(project=project_id)

#### Dataprep ####

# Query
gce_audit_t_query = \
"""
SELECT *, TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')) AS last_updated_ts
FROM `stanleysfang.monitoring_logging.gce_audit`
"""

# Job Config
job_config = bigquery.QueryJobConfig()

job_config.use_legacy_sql = False
job_config.destination = 'stanleysfang.monitoring_logging.gce_audit_t'
job_config.write_disposition = 'WRITE_TRUNCATE'
job_config.dry_run = False

# Query Job
query_job = client.query(gce_audit_t_query, job_config=job_config)
query_job.result()

# Query
gce_daily_uptime_t_query = \
"""
SELECT *, TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')) AS last_updated_ts
FROM `stanleysfang.monitoring_logging.gce_daily_uptime`
"""

# Job Config
job_config = bigquery.QueryJobConfig()

job_config.use_legacy_sql = False
job_config.destination = 'stanleysfang.monitoring_logging.gce_daily_uptime_t'
job_config.write_disposition = 'WRITE_TRUNCATE'
job_config.dry_run = False

# Query Job
query_job = client.query(gce_daily_uptime_t_query, job_config=job_config)
query_job.result()

print("Query job successful!")

#### Extract Table ####

# Job Config
job_config = bigquery.ExtractJobConfig()

job_config.destination_format = 'CSV'

# Extract Job
extract_job = client.extract_table(
    'stanleysfang.monitoring_logging.gce_audit_t',
    'gs://gcp_audit/gce_audit_t.csv',
    job_config=job_config
)
extract_job.result()

extract_job = client.extract_table(
    'stanleysfang.monitoring_logging.gce_daily_uptime_t',
    'gs://gcp_audit/gce_daily_uptime_t.csv',
    job_config=job_config
)
extract_job.result()

print("Extract job successful!")
