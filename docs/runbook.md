# Platform Runbook

## Prerequisites
- Docker Desktop
- Azure subscription
- Microsoft Fabric account
- Azure CLI

## Setup Steps
1. Clone repository
2. Copy docker/.env.example to docker/.env and fill values
3. Run: cd docker && docker compose up --build
4. Access Airflow at http://localhost:8080 (admin/admin)
5. Enable and trigger DAGs manually for first run
6. Open Databricks workspace and run Bronze → Silver → Gold notebooks
7. Verify data in Microsoft Fabric Lakehouse
8. Open Power BI report from dashboards/ folder
