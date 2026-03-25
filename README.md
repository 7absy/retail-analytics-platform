# Unified Retail Intelligence & Omnichannel Analytics Platform

## 📌 Business Problem
A global retail organization operates across physical stores, e-commerce platforms, mobile applications, and third-party marketplaces. Data is currently fragmented across systems, resulting in no unified customer view, delayed analytics, and poor inventory and marketing visibility.

This project designs a modern Lakehouse-based analytics platform to unify all data sources and enable scalable batch and near real-time analytics.

---

## 🧰 Tech Stack

- **Azure Databricks** → Distributed processing using Spark & Delta Lake for scalable ETL pipelines.
Chosen over Microsoft Fabric Spark for heavy transformation workloads due to better control over cluster configuration, fine-tuned performance optimization, and mature support for large-scale distributed processing patterns such as complex joins, incremental pipelines, and streaming + batch unification in a single engine
- **Microsoft Fabric** → Unified analytics layer for Lakehouse, Warehouse, and BI integration.
Used primarily for semantic modeling, governance, and tight integration with Power BI rather than heavy transformation workloads
- **Azure Data Lake Storage (ADLS)** → Centralized storage for all data layers (Raw, Curated).
Acts as the single source of truth with structured separation between immutable ingestion data (Raw Zone) and processed, analytics-ready datasets (Curated Zone).
- **Apache Airflow** → Workflow orchestration, scheduling, and pipeline monitoring
- **Docker** → Simulation of APIs and event-driven data sources
- **Power BI** → Business intelligence dashboards and reporting

---

## 🏗️ Architecture Layers

### 1. Data Sources
Simulated systems generating:
- Batch files (CSV)
- API responses
- Event streams (JSON)

### 2. Docker Simulation Layer
Generates realistic enterprise data using:
- FastAPI services
- Python event generators
- docker-compose orchestration

### 3. Orchestration Layer (Airflow)
Handles:
- Scheduling pipelines
- Retry mechanisms
- Dependency management

### 4. Data Lake (ADLS)
- **Raw Zone** → Immutable ingestion layer partitioned by ingestion_date

### 5. Databricks Processing Layer
- Bronze → Raw ingestion (append-only)
- Silver → Cleaned, validated, deduplicated data
- Gold → Business-ready star schema datasets

### 6. Microsoft Fabric Layer
- Lakehouse → Direct Delta consumption
- Warehouse → SQL-based star schema serving layer

### 7. BI Layer
- Power BI dashboards using DirectLake or Import mode
- Executive and operational reporting

---

## 🎯 Goal
To provide a unified retail data platform that enables a single source of truth for customer, inventory, and sales data across all channels, delivering real-time visibility and consistent analytics for operational and strategic decision-making.

---

## 🚀 Status
Phase 1: Repository initialized
