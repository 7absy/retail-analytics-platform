# Airflow Pipelines

Responsible for orchestration of:
- Data ingestion workflows
- Databricks job triggers
- Retry and failure handling

## Design Principles
- Idempotent execution
- Incremental processing only
- Clear dependency management