# Pipeline SLA Definitions

| Pipeline | Schedule | Max Duration | Alert If Late |
|---|---|---|---|
| batch_ingestion_dag | @daily | 30 min | After 6:30 AM |
| api_ingestion_dag | @hourly | 10 min | After :15 each hour |
| event_ingestion_dag | */15 min | 5 min | After 20 min |

## Alerting
- Email on failure: enabled for all DAGs
- Retries: 3 attempts with 5 min delay
