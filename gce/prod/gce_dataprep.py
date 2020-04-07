
from google.cloud import bigquery
import pandas as pd
from BigQueryWrapper import QueryRunner, Extractor

project_id = 'stanleysfang'
client = bigquery.Client(project=project_id)

qr = QueryRunner(client=client)
extractor = Extractor(client=client)

#### Dataprep ####
# stanleysfang.monitoring_logging.gce_audit_t
gce_audit_t_query = \
"""
SELECT *, TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')) AS last_updated_ts
FROM `stanleysfang.monitoring_logging.gce_audit`
"""
gce_audit_t_query_job = qr.run_query(gce_audit_t_query, destination_table='stanleysfang.monitoring_logging.gce_audit_t')

# stanleysfang.monitoring_logging.gce_daily_uptime_t
gce_daily_uptime_t_query = \
"""
SELECT *, TIMESTAMP(REGEXP_REPLACE(STRING(CURRENT_TIMESTAMP, "America/Los_Angeles"), r'[\+-][0-9]{2}$', '')) AS last_updated_ts
FROM `stanleysfang.monitoring_logging.gce_daily_uptime`
"""
gce_daily_uptime_t_query_job = qr.run_query(gce_daily_uptime_t_query, destination_table='stanleysfang.monitoring_logging.gce_daily_uptime_t')

max_results = 20
for job in qr.job_history:
    job.result()
    
    bq_table = client.get_table(job.destination)
    df = client.list_rows(bq_table, max_results=max_results).to_dataframe()
    
    print(bq_table.full_table_id)
    print(df.head(max_results))

#### Extract Table ####
gce_audit_t_extract_job = extractor.extract('stanleysfang.monitoring_logging.gce_audit_t', 'gs://gcp_audit/gce_audit_t.csv')
gce_daily_uptime_t_extract_job = extractor.extract('stanleysfang.monitoring_logging.gce_daily_uptime_t', 'gs://gcp_audit/gce_daily_uptime_t.csv')

for job in extractor.job_history:
    job.result()
