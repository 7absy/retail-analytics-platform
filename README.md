# Unified Retail Intelligence & Omnichannel Analytics Platform

> **Production-Grade Lakehouse Architecture** · Azure Databricks · Microsoft Fabric · Apache Airflow · Docker

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.4-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Azure Databricks](https://img.shields.io/badge/Databricks-Medallion-FF3621?style=flat&logo=databricks&logoColor=white)
![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-OneLake-7B2FBE?style=flat&logo=microsoft&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-4_Reports-F2C811?style=flat&logo=powerbi&logoColor=black)
![Status](https://img.shields.io/badge/Status-Active_Development-brightgreen?style=flat)

---

## Business Problem

A global retail organisation operates across **physical stores**, **e-commerce platforms**, **mobile applications**, and **third-party marketplaces**. Data is fragmented across systems, causing:

- No unified customer view
- Delayed analytics (24h+ lag)
- Poor inventory and marketing visibility
- No real-time operational awareness

This platform unifies all data sources into a single lakehouse with **both batch reporting and real-time streaming**, giving operations teams live intelligence and analysts clean, modelled data — with no compromise on either.

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
├── batch_ingestion_dag   @daily   6 tasks: generate → validate → upload
├── api_ingestion_dag     @hourly  4 tasks: fetch → validate → upload
└── event_ingestion_dag   */15min  5 tasks: fetch → validate → upload
    └── Postgres Metadata DB

AZURE DATA LAKE STORAGE GEN2
├── Raw Zone    (immutable)        raw/batch/ · raw/api/ · raw/events/
└── Curated Zone (Delta Lake)      curated/bronze/ · silver/ · gold/

AZURE DATABRICKS — Medallion Architecture
├── Bronze  (9 tables)    Raw Delta, append-only, partitioned by date
├── Silver  (9 tables)    Cleaned, validated, type-cast, quarantine logic
└── Gold    (10 tables)   Star schema: 5 fact tables + 5 dimension tables

MICROSOFT FABRIC
├── Batch path:       Lakehouse (10 OneLake shortcuts) → SQL Endpoint → Semantic Model
└── Streaming path:   3 Eventstreams → Eventhouse KQL DB

VISUALIZATION
├── Power BI (4 pages)             15 min – 24 hr latency
│   ├── Sales Overview             13.77M revenue tracked
│   ├── Inventory Health           6M stock units monitored
│   ├── Customer Analytics
│   └── Product Performance
└── Real-Time Dashboard            Auto-refresh every 30 seconds
    ├── Live order count
    ├── Store footfall
    └── Clickstream activity
```

### Data Flow Paths

| Path | Source → Destination | Latency | Colour |
|------|----------------------|---------|--------|
| **Batch CSV** | Batch Generator → Airflow @daily → ADLS raw/batch → Bronze → Silver → Gold → Fabric → Power BI | 15 min – 24 hr | Blue |
| **API JSON** | API Simulator → Airflow @hourly → ADLS raw/api → Bronze → Silver → Gold → Fabric → Power BI | ~1 hr | Green |
| **Event batch** | Event Generator → Airflow @15min → ADLS raw/events → Bronze → Silver → Gold → Fabric → Power BI | ~15 min | Amber |
| **Real-time** | Event Generator → Fabric Eventstreams → Eventhouse KQL → RT Dashboard | **Seconds** | Red (dashed) |

> The real-time path **bypasses Airflow entirely** — the Event Generator pushes directly to Microsoft Fabric Eventstreams.

---

## Tech Stack & Design Decisions

| Layer | Technology | Why |
|-------|-----------|-----|
| Simulation | Docker + FastAPI + Faker | Realistic enterprise data without real systems |
| Orchestration | Apache Airflow 2.8.4 | DAG-based scheduling, retry logic, dependency management, Postgres metadata |
| Storage | Azure Data Lake Storage Gen2 | Immutable raw zone + Delta Lake curated zone, single source of truth |
| Processing | Azure Databricks (Spark + Delta Lake) | Fine-grained cluster control, complex joins, streaming + batch in one engine |
| Serving | Microsoft Fabric (Lakehouse + Eventstreams) | OneLake shortcuts eliminate data copy; tight Power BI integration |
| BI | Power BI (DirectLake / Import mode) | 4 report pages, semantic model governance |
| Real-time | Fabric Eventhouse (KQL) | Sub-second query latency on live event streams |

**Why Databricks over Fabric Spark for transformation?** Databricks gives better control over cluster sizing, partition tuning, and incremental pipeline patterns at scale. Fabric is used for semantic modeling, governance, and BI serving — where its tight Power BI integration wins.

---

## Project Structure

```
retail-analytics-platform/
├── architecture/          Architecture diagrams and design docs
├── dashboards/            Power BI report files and real-time dashboard configs
├── data/                  Sample/seed data files
├── databricks/            Databricks notebooks — Bronze, Silver, Gold transformations
├── docker/                Docker Compose + FastAPI simulators (API + Event generators)
├── docs/                  Extended documentation
├── fabric/                Microsoft Fabric configuration — Lakehouse, Eventstreams, KQL
├── monitoring/            Airflow DAG monitoring, alerting configs
├── pipelines/             Airflow DAG definitions
├── .gitignore
├── README.md
└── requirements.txt       Python dependencies
```

---

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.11+
- Azure subscription (Databricks + ADLS + Fabric)
- Apache Airflow (or use the Docker Compose setup)

### 1. Clone and install

```bash
git clone https://github.com/7absy/retail-analytics-platform.git
cd retail-analytics-platform
pip install -r requirements.txt
```

### 2. Start the simulation layer

```bash
cd docker
docker-compose up -d
```

This starts:
- **API Simulator** at `http://localhost:8000` (`/orders`, `/campaigns`)
- **Event Generator** at `http://localhost:9000` (clickstream, footfall, orders)
- **Batch Generator** producing daily CSV files

### 3. Start Airflow

```bash
airflow db init
airflow scheduler &
airflow webserver --port 8080
```

Access the UI at `http://localhost:8080`. Enable the three DAGs:
- `batch_ingestion_dag`
- `api_ingestion_dag`
- `event_ingestion_dag`

### 4. Run Databricks notebooks

Upload the notebooks from `/databricks` to your Azure Databricks workspace and run them in order:
1. `bronze_ingestion.py`
2. `silver_transformation.py`
3. `gold_star_schema.py`

### 5. Connect Microsoft Fabric

- Create a Lakehouse and add 10 OneLake shortcuts pointing to the Gold Delta tables in ADLS
- Configure 3 Eventstreams from the Event Generator endpoint
- Create the Eventhouse KQL database and connect the Eventstreams
- Import the Semantic Model from `/fabric`

### 6. Open Power BI

Open the `.pbix` files from `/dashboards` and connect to the Fabric SQL Endpoint.

---

## Medallion Architecture Detail

### Bronze Layer — 9 tables
Raw Delta tables, append-only, partitioned by `ingestion_date`. No transformation — exact copy of source data.

### Silver Layer — 9 tables
Cleaned, validated, and type-cast from Bronze. Includes quarantine logic to isolate bad records without breaking pipelines.

### Gold Layer — 10 tables (Star Schema)

| Table | Type | Description |
|-------|------|-------------|
| `fact_sales` | Fact | Transactional sales records |
| `fact_inventory` | Fact | Daily inventory snapshots |
| `fact_orders` | Fact | Order-level detail |
| `fact_clickstream` | Fact | Digital engagement events |
| `fact_footfall` | Fact | In-store traffic counts |
| `dim_product` | Dimension | Product catalogue |
| `dim_customer` | Dimension | Customer profiles |
| `dim_store` | Dimension | Store/channel metadata |
| `dim_date` | Dimension | Date spine |
| `dim_campaign` | Dimension | Marketing campaigns |

---

## Power BI Report Pages

| Page | Key KPIs |
|------|----------|
| Sales Overview | 13.77M total revenue, channel breakdown, trend |
| Inventory Health | 6M stock units, turnover rate, reorder alerts |
| Customer Analytics | Segmentation, LTV, retention |
| Product Performance | Top/bottom performers, margin analysis |

---

## Real-Time Dashboard

- **Refresh interval:** 30 seconds
- **Source:** Fabric Eventhouse KQL DB
- **Tiles:** Live order count · Store footfall · Clickstream activity · Revenue per minute

---

## Roadmap

- [x] Docker simulation layer (API + Event + Batch generators)
- [x] Airflow DAGs (batch, API, event ingestion)
- [x] ADLS Gen2 raw + curated zones
- [x] Databricks medallion pipeline (Bronze → Silver → Gold)
- [x] Microsoft Fabric Lakehouse + Eventstreams + KQL DB
- [x] Power BI 4-page report
- [x] Real-time dashboard
- [ ] Data quality monitoring with Great Expectations
- [ ] CI/CD pipeline with GitHub Actions
- [ ] dbt integration for Gold layer transformations
- [ ] Cost monitoring and alerting

---

## Author

**Mostafa Habasy** · [GitHub](https://github.com/7absy) · [LinkedIn](https://www.linkedin.com/in/mostafa-habasy)

---

*Built to go deep on the modern Azure data stack — contributions and feedback welcome.*
