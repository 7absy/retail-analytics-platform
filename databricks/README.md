# Databricks Processing Layer

This layer implements the Medallion Architecture using Delta Lake.

## Structure
- bronze/ → Raw ingestion layer (append-only)
- silver/ → Cleaned and standardized datasets
- gold/ → Business-ready star schema models

## Key Responsibilities
- Data transformation using PySpark
- Incremental processing
- Data quality enforcement
- SCD Type 2 handling (Gold layer)

## Storage Format
All datasets stored in Delta Lake format for:
- ACID transactions
- Time travel
- Efficient merges