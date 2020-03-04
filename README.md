# Audit of GCP Services
This repository contains dashboards of GCP usage and their data pipelines.

**GCE Audit Dashboard:** https://public.tableau.com/profile/stanleysfang#!/vizhome/GCEAudit/GCEAudit  

### Data Pipeline
Tableau Public has limited options for connecting to live data, and Google Spreadsheet is one of the free tools that can support a live dashboard. Therefore, the final destination of the pipeline will be at Google Spreadsheet. Figure-1 is a diagram that describes the pipeline.

<img src="https://github.com/stanleysfang/surveillance_2019_ncov/raw/master/image/pipeline_diagram.png" alt="pipeline_diagram" width="730" height="300">

*Figure-1: The arrows show the flow of data. Compute Engine and Cloud Functions*  
*support the transfer of data. Data processing is done in BigQuery.*

### References
**Exporting with Logs Viewer:** https://cloud.google.com/logging/docs/export/configure_export_v2  
