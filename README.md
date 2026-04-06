# Unified Retail Intelligence & Omnichannel Analytics Platform

> **Production-Grade Lakehouse Architecture** · Azure Databricks · Microsoft Fabric · Apache Airflow · Docker

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat\&logo=python\&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.4-017CEE?style=flat\&logo=apacheairflow\&logoColor=white)
![Azure Databricks](https://img.shields.io/badge/Databricks-Medallion-FF3621?style=flat\&logo=databricks\&logoColor=white)
![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-OneLake-7B2FBE?style=flat\&logo=microsoft\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat\&logo=docker\&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-4_Reports-F2C811?style=flat\&logo=powerbi\&logoColor=black)
![Status](https://img.shields.io/badge/Status-Active_Development-brightgreen?style=flat)

---

## Business Problem

A global retail organisation operates across **physical stores**, **e-commerce platforms**, **mobile applications**, and **third-party marketplaces**. Data is fragmented across systems, causing:

* No unified customer view
* Delayed analytics (24h+ lag)
* Poor inventory and marketing visibility
* No real-time operational awareness

This platform unifies all data sources into a single lakehouse with **both batch reporting and real-time streaming**, enabling real-time operations and reliable analytics.

---

## Architecture Overview

```
DATA SOURCES
├── Physical Stores (POS)          → Batch CSV     @daily
├── E-commerce Platform            → API JSON      @hourly
├── Mobile Application             → Events        @15min + real-time
└── Third-party Marketplace        → Event batch   @15min

DOCKER SIMULATION LAYER
├── API Simulator     :8000        FastAPI · /orders · /campaigns
├── Event Generator   :9000        FastAPI · clickstream · footfall · orders
└── Batch Generator                Python · CSV daily files

ORCHESTRATION (Apache Airflow)
├── batch_ingestion_dag   @daily
├── api_ingestion_dag     @hourly
└── event_ingestion_dag   */15min
    └── Postgres Metadata DB

AZURE DATA LAKE STORAGE GEN2
├── Raw Zone    (immutable)        raw/batch/ · raw/api/ · raw/events/
└── Curated Zone (Delta Lake)      curated/bronze/ · silver/ · gold/

AZURE DATABRICKS — Medallion Architecture
├── Bronze  (9 tables)
├── Silver  (9 tables)
└── Gold    (10 tables)

MICROSOFT FABRIC
├── Batch path:       Lakehouse → SQL Endpoint → Semantic Model
└── Streaming path:   Eventstreams → Eventhouse KQL DB

VISUALIZATION
├── Power BI (4 pages)
└── Real-Time Dashboard (30s refresh)
```

---

## Data Flow Paths

| Path        | Flow                                                              | Latency        |
| ----------- | ----------------------------------------------------------------- | -------------- |
| Batch CSV   | Batch Generator → Airflow → ADLS → Databricks → Fabric → Power BI | 15 min – 24 hr |
| API JSON    | API Simulator → Airflow → ADLS → Databricks → Fabric → Power BI   | ~1 hr          |
| Event Batch | Event Generator → Airflow → ADLS → Databricks → Fabric → Power BI | ~15 min        |
| Real-time   | Event Generator → Fabric Eventstreams → Eventhouse → Dashboard    | Seconds        |

---

## Tech Stack

* **Simulation:** Docker, FastAPI
* **Orchestration:** Apache Airflow
* **Storage:** Azure Data Lake Gen2
* **Processing:** Azure Databricks (Delta Lake)
* **Serving:** Microsoft Fabric
* **BI:** Power BI
* **Streaming:** Fabric Eventhouse (KQL)

---

## Project Structure

```
retail-analytics-platform/
├── architecture/
├── dashboards/
├── data/
├── databricks/
├── docker/
├── docs/
├── fabric/
├── monitoring/
├── pipelines/
├── README.md
└── requirements.txt
```

---

## Getting Started

### Prerequisites

* Docker Desktop
* Python 3.11+
* Azure account (Databricks + ADLS + Fabric)

---

### 1. Clone Repository

```bash
git clone https://github.com/7absy/retail-analytics-platform.git
cd retail-analytics-platform
pip install -r requirements.txt
```

---

### 2. Run Full Platform (Recommended)

```bash
cd docker
docker-compose up --build
```

This starts:

* API Simulator → http://localhost:8000
* Event Generator → http://localhost:9000
* Batch Generator
* Airflow → http://localhost:8080

---

### Airflow Access

* Username: `admin`
* Password: `admin`

Enable DAGs:

* `batch_ingestion_dag`
* `api_ingestion_dag`
* `event_ingestion_dag`

> Airflow is fully containerized. Initialization, scheduler, and webserver are automatically managed — no manual setup required.

---

### 3. Run Databricks

Run notebooks in order:

1. Bronze
2. Silver
3. Gold

---

### 4. Connect Microsoft Fabric

* Create Lakehouse
* Add shortcuts to Gold layer
* Configure Eventstreams
* Connect Eventhouse

---

### 5. Open Power BI

Load dashboards and connect to Fabric SQL Endpoint.

---

## Medallion Architecture

* **Bronze:** Raw data
* **Silver:** Cleaned data
* **Gold:** Star schema

---

## Roadmap

* [x] Data ingestion pipelines
* [x] Medallion architecture
* [x] Real-time streaming
* [x] Power BI dashboards
* [ ] Data quality monitoring
* [ ] CI/CD pipeline
* [ ] dbt integration

---

*Built for modern data engineering on Azure.*
