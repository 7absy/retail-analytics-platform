# Unified Retail Intelligence & Omnichannel Analytics Platform

## 📌 Business Problem
A global retail organization operates across physical stores, e-commerce platforms, mobile applications, and third-party marketplaces. Data is currently fragmented across systems, resulting in no unified customer view, delayed analytics, and poor inventory and marketing visibility.

This project designs a modern Lakehouse-based analytics platform to unify all data sources and enable scalable batch and near real-time analytics.

---

## 🧰 Tech Stack

- **Azure Databricks** → Distributed processing using Spark & Delta Lake for scalable ETL pipelines
- **Microsoft Fabric** → Unified analytics layer for Lakehouse, Warehouse, and BI integration
- **Azure Data Lake Storage (ADLS)** → Centralized raw and curated data storage
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
To build a production-grade, scalable, and modular data platform that demonstrates enterprise-level Data Engineering capabilities suitable for FAANG interviews.

---

## 🚀 Status
Phase 1: Repository initialized