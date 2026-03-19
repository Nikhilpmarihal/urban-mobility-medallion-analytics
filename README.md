# 🚖 Tez — End-to-End Real-Time Ride Data Engineering Platform

> **ತೇಜ್ — Fast. Sharp. Here.**
> An enterprise-grade, real-time data engineering and machine learning pipeline built for Bengaluru's ride ecosystem — simulating a production Uber-like platform on Azure and Databricks.

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Model](#-data-model)
- [Medallion Architecture — Layer by Layer](#-medallion-architecture--layer-by-layer)
- [Setup & Installation](#-setup--installation)
- [ML Model — Driver Reallocation](#-ml-model--driver-reallocation)
- [API Reference](#-api-reference)
- [Key Design Decisions](#-key-design-decisions)
- [Environment Variables](#-environment-variables)

---

## 📋 Project Overview

**Tez** is a full end-to-end data engineering project that simulates an enterprise ride-booking platform for Bengaluru. It processes both live streaming events and static historical data through a **Medallion Architecture (Bronze → Silver → Gold)**.

**What it does:**

- A custom **FastAPI web application** generates realistic Indian ride booking events and publishes them live to **Azure Event Hub** (running as managed Apache Kafka)
- Static mapping files and historical bulk data are fetched from **Azure Blob Storage** and loaded into the pipeline
- All data is processed on **Databricks** using **Spark Declarative Pipelines (DLT)** — streaming and batch merged seamlessly using `append_flow`
- The Silver layer uses a **Jinja2 metadata-driven architecture** to dynamically build complex SQL joins into a fully denormalised **One Big Table (OBT)**
- The Gold layer deploys a **Star Schema** including a **Slowly Changing Dimension Type 2 (SCD Type 2)** table for location tracking
- A **Random Forest ML model** recommends optimal driver reallocation zones using geospatial intelligence, tracked end-to-end with **MLflow**

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA PRODUCERS                              │
│                                                                      │
│   FastAPI Web App (Tez UI)              Azure Blob Storage           │
│   data.py → connection.py               bulk_rides.json              │
│   Generates live ride events            + 6 mapping JSON files       │
│           │                                      │                   │
└───────────┼──────────────────────────────────────┼───────────────────┘
            │                                      │
            ▼                                      │
┌───────────────────────┐                          │
│   Azure Event Hub     │                          │
│   (Kafka Protocol)    │                          │
│   rides_raw topic     │                          │
└───────────┬───────────┘                          │
            │                                      │
            ▼                                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       DATABRICKS PLATFORM                            │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                      BRONZE LAYER                              │  │
│  │   rides_raw (streaming)    +    bulk_rides + mapping (batch)   │  │
│  │   map_cities · map_vehicle_types · map_payment_methods         │  │
│  │   map_cancellation_reasons · map_ride_statuses                 │  │
│  │   map_vehicle_makes                                            │  │
│  └────────────────────────────┬───────────────────────────────────┘  │
│                               │  Delta Live Tables (DLT)             │
│                               ▼                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                      SILVER LAYER                              │  │
│  │     stg_rides  (append_flow: stream + bulk merged)             │  │
│  │     Jinja2 metadata-driven JOIN → One Big Table (OBT)          │  │
│  │     Typed · Cleaned · Timestamp-cast · Fare-validated          │  │
│  └────────────────────────────┬───────────────────────────────────┘  │
│                               │                                      │
│                               ▼                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                       GOLD LAYER                               │  │
│  │     Star Schema: fact_rides + dimension tables                 │  │
│  │     dim_location  →  SCD Type 2 (start_at / end_at)           │  │
│  │     dim_vehicle · dim_passenger · dim_driver · dim_payment     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        ML LAYER  (MLflow)                            │
│                                                                      │
│  Synthetic Demand/Supply  →  Feature Engineering (17 features)       │
│  Random Forest Classifier →  GridSearchCV + StratifiedKFold          │
│  Hybrid Rule-Based + ML   →  Zone Reallocation Recommendation        │
│  MLflow Tracking          →  Params · Metrics · Artifacts · Model    │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| **Web Application** | FastAPI · Python · Jinja2 · Uvicorn |
| **UI / Frontend** | HTML · CSS · Playfair Display · Bebas Neue |
| **Data Generation** | Python · Faker · UUID |
| **Real-Time Streaming** | Azure Event Hub (Standard Tier · Kafka API) |
| **Batch Orchestration** | Azure Data Factory (HTTP → ADLS pipeline) |
| **Cloud Storage** | Azure Blob Storage / ADLS Gen2 |
| **Data Processing** | Databricks Free Edition · PySpark · Spark SQL |
| **Pipeline Framework** | Spark Declarative Pipelines (DLT) |
| **Templating** | Jinja2 (metadata-driven SQL joins) |
| **Storage Format** | Delta Lake |
| **ML Framework** | Scikit-learn · Random Forest · GridSearchCV |
| **ML Tracking** | MLflow |
| **Geospatial** | Folium · Haversine Distance |
| **Visualisation** | ipywidgets · Folium Interactive Maps |
| **Environment** | Python-dotenv · Joblib |

---

## 📂 Project Structure

```
📦 Tez — Driver Reallocation Project
│
├── 📁 Code_Files/
│   ├── bronze_adls.ipynb    # Loads 7 JSON files from Blob Storage → Bronze Delta tables
│   ├── ingest.py            # DLT pipeline: Event Hub (Kafka) → rides_raw streaming table
│   ├── model.py             # Gold layer: Star Schema + SCD Type 2 dim_location
│   ├── silver.py            # DLT pipeline: append_flow merge + Jinja2 OBT join
│   ├── silver_obt.ipynb     # Silver One Big Table notebook exploration
│   └── silver_obt.sql       # SQL for Silver OBT transformations
│
├── 📁 Data/
│   ├── bulk_rides.json               # 100 pre-generated historical ride records
│   ├── map_cancellation_reasons.json # Driver cancelled / Passenger cancelled / No show
│   ├── map_cities.json               # 10 Bengaluru zones: lat · lng · region · state
│   ├── map_payment_methods.json      # UPI · Cash · Credit Card · Debit Card
│   ├── map_ride_statuses.json        # Completed / Cancelled
│   ├── map_vehicle_makes.json        # Maruti · Hyundai · Tata · Toyota · Mahindra · Honda · Bajaj
│   └── map_vehicle_types.json        # Uber Go · Premier · Auto · Moto · XL (with rates)
│
├── 📁 Models/
│   ├── Testing_1.ipynb        # Driver reallocation ML model 
│   
│
├── 📁 templates/
│   ├── home.html            # Tez booking page: mobile + desktop responsive
│   └── confirmation.html    # Ride receipt: full fare breakdown + driver + map
│
├── api.py                   # FastAPI routes — / and /book with full context resolution
├── connection.py            # Azure Event Hub publisher (context manager · bool return)
├── data.py                  # Ride generator: Indian names · fare calc · city jitter
├── .env                     # Environment variables (not committed to git)
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 📊 Data Model

### `bulk_rides` — Core Fact Table

| Field | Type | Description |
|---|---|---|
| `ride_id` | string | Unique ride UUID |
| `confirmation_number` | string | Format: `UBXX999-9999` |
| `passenger_id` | string | Passenger UUID |
| `driver_id` | string | Driver UUID |
| `vehicle_id` | string | Vehicle UUID |
| `vehicle_type_id` | long | FK → map_vehicle_types |
| `vehicle_make_id` | long | FK → map_vehicle_makes |
| `payment_method_id` | long | FK → map_payment_methods |
| `ride_status_id` | long | FK → map_ride_statuses |
| `pickup_city_id` | long | FK → map_cities |
| `dropoff_city_id` | long | FK → map_cities |
| `cancellation_reason_id` | long | FK → map_cancellation_reasons (4 = null for completed) |
| `distance_km` | double | Trip distance in kilometres |
| `duration_minutes` | long | Trip duration |
| `booking_timestamp` | string → timestamp | ISO 8601, cast to `TimestampType` in DLT |
| `pickup_timestamp` | string → timestamp | ISO 8601, cast to `TimestampType` in DLT |
| `dropoff_timestamp` | string → timestamp | ISO 8601, cast to `TimestampType` in DLT |
| `base_fare` | double | Vehicle base rate (₹) |
| `distance_fare` | double | `distance_km × per_km` |
| `time_fare` | double | `duration_minutes × per_minute` |
| `surge_multiplier` | double | 1.0× · 1.25× · 1.5× · 1.75× · 2.0× |
| `subtotal` | double | `(base + distance + time) × surge` |
| `tip_amount` | double | Optional tip in ₹ |
| `total_fare` | double | `subtotal + tip` |
| `rating` | double | 1–5 for completed rides · null for cancelled |

### Mapping / Dimension Tables

| Table | Rows | Key Fields |
|---|---|---|
| `map_cities` | 10 | city_id · city_name · latitude · longitude · region · state |
| `map_vehicle_types` | 5 | vehicle_type_id · vehicle_type · base_rate · per_km · per_minute |
| `map_vehicle_makes` | 7 | vehicle_make_id · vehicle_make |
| `map_payment_methods` | 4 | payment_method_id · payment_method · is_card · requires_auth |
| `map_ride_statuses` | 2 | ride_status_id · ride_status · is_completed |
| `map_cancellation_reasons` | 4 | cancellation_reason_id · cancellation_reason |

---

## 🥉🥈🥇 Medallion Architecture — Layer by Layer

### Bronze Layer

`Code_Files/bronze_adls.ipynb` loads all 7 JSON files from Azure Blob Storage into Delta tables using SAS token authentication:

```python
for file in files:
    url      = f"https://...blob.core.windows.net/raw/{file['file']}.json?{SAS_TOKEN}"
    df       = pd.read_json(url)
    df_spark = spark.createDataFrame(df)
    df_spark.write.format("delta").mode("overwrite") \
            .saveAsTable(f"driverreallocation.bronze.{file['file']}")
```

`Code_Files/ingest.py` reads the live stream from Azure Event Hub using the Kafka consumer API:

```python
@dlt.table
def rides_raw():
    df = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()
    df = df.withColumn("rides", col("value").cast(StringType()))
    return df
```

### Silver Layer

`Code_Files/silver.py` uses two `@dlt.append_flow` functions writing into a single `stg_rides` streaming table — solving the challenge of merging a heavy one-time batch load with a continuous stream:

```
bulk_rides  (batch, runs once)  ──┐
                                   ├──► stg_rides  (Silver — append_flow)
rides_raw   (stream, continuous) ──┘
```

A **Jinja2 metadata-driven architecture** then dynamically renders the complex LEFT JOIN SQL to build the fully denormalised **One Big Table (OBT)** — no hardcoded join logic:

```python
# Python dict drives the join config
join_config = [
    {"table": "map_cities",          "alias": "pc", "on": "pickup_city_id"},
    {"table": "map_vehicle_types",   "alias": "vt", "on": "vehicle_type_id"},
    # ... more joins
]

# Jinja2 renders the full SQL at runtime
sql = jinja_template.render(joins=join_config, source="stg_rides")
spark.sql(sql)
```

### Gold Layer

`Code_Files/model.py` builds the Star Schema:

- **`fact_rides`** — central fact table with all metrics and FK references
- **`dim_vehicle`** — vehicle type and make attributes
- **`dim_passenger`** — passenger profile
- **`dim_driver`** — driver profile and ratings
- **`dim_payment`** — payment method attributes
- **`dim_location`** — SCD Type 2 table tracking pickup/dropoff location changes over time using Databricks AutoCDC with auto-generated `start_at` and `end_at` timestamps

---

## 🚀 Setup & Installation

### Prerequisites

- Python 3.9+
- Azure subscription (Event Hub Standard Tier · Blob Storage)
- Databricks Free Edition workspace
- Google Colab (for ML models)

### 1. Clone the repository

```bash
git clone https://github.com/your-username/tez-DriverReallocation.git
cd tez-DriverReallocation
```

### 2. Create virtual environment

```bash
python -m venv .venv
source .venv/bin/activate      # Mac / Linux
.venv\Scripts\activate         # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the root:

```env
CONNECTION_STRING=Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=SendPolicy;SharedAccessKey=...;EntityPath=your-topic
EVENT_HUBNAME=your-event-hub-topic-name
```

### 5. Azure Event Hub Setup

1. Create an **Event Hub Namespace** on **Standard Tier** (required for Kafka API)
2. Create a topic (e.g. `reallocationtopic`)
3. Create two Shared Access Policies:
   - `SendPolicy` — for the FastAPI web app
   - `ListenPolicy` — for Databricks ingestion

### 6. Run the web application

```bash
uvicorn api:app --reload
```

Open **http://localhost:8000** to see the Tez booking UI. Every booking generates a ride and publishes it to Event Hub.

### 7. Databricks Pipeline Execution

```
Step 1  →  Run bronze_adls.ipynb   (loads 7 mapping + bulk JSON files)
Step 2  →  Run ingest.py           (starts Event Hub streaming → rides_raw)
Step 3  →  Run silver.py           (merge + Jinja2 OBT join → stg_rides)
Step 4  →  Run model.py            (Star Schema + SCD Type 2 → Gold layer)
```

---

## 🤖 ML Model — Driver Reallocation

Located in `Models/Model_2.ipynb` — fully tracked with MLflow.

### Problem Statement

Given a driver's current zone, time of day, supply and demand — **which zone should the driver relocate to** in order to maximise ride fulfilment across Bengaluru?

### Approach

```
Synthetic Demand/Supply Data  (Poisson + time-of-day multipliers)
            ↓
Rule-Based Label Generation   (training targets via scoring heuristic)
            ↓
Feature Engineering           (17 features — temporal · ratio · spatial)
            ↓
Random Forest + GridSearchCV  (StratifiedKFold · class_weight=balanced)
            ↓
Hybrid Decision Engine        (ML prediction + rule-based spatial constraints)
            ↓
Zone Recommendation           (interactive widget + Folium map)
```

### Features (17 total)

| Category | Features |
|---|---|
| **Temporal** | Time_Window_enc · Hour_Sin · Hour_Cos |
| **Zone** | Current_Zone_enc |
| **Supply / Demand** | Current_Zone_Supply · Current_Zone_Demand · Supply_Demand_Ratio · Log_Supply · Log_Demand · Abs_Gap · Is_Deficit · Is_Surplus |
| **Surrounding Zones** | Surrounding_Zone_1_Gap · Surrounding_Zone_2_Gap · Avg_Surrounding_Gap · Max_Surrounding_Gap · Total_Surrounding_Gap |

### Spatial Constraints

| Scenario | Max Radius |
|---|---|
| Current zone has deficit | 3.0 km |
| Surplus + Peak hours (7–9, 17–20) | 3.0 km |
| Surplus + Off-peak | 4.0 km |

### MLflow Tracking (Google Colab)

```python
from google.colab import drive
drive.mount('/content/drive')

mlflow.set_tracking_uri("file:///content/drive/MyDrive/mlflow_runs")
mlflow.set_experiment("driver_reallocation")

with mlflow.start_run(run_name="RandomForest_v1"):
    mlflow.set_tag("model_type", "RandomForest")
    mlflow.log_params(grid_search.best_params_)
    mlflow.log_metric("cv_accuracy",    round(grid_search.best_score_, 4))
    mlflow.log_metric("train_accuracy", round(train_acc, 4))
    mlflow.log_artifact("feature_importance.csv")
    mlflow.log_artifact("le_time.pkl")
    mlflow.log_artifact("le_input_zone.pkl")
    mlflow.log_artifact("le_target_zone.pkl")
    mlflow.sklearn.log_model(best_model, "model")
```

### Viewing MLflow UI in Colab

```python
import subprocess, time
from pyngrok import ngrok

ngrok.set_auth_token("your_ngrok_token")   # from dashboard.ngrok.com

subprocess.Popen([
    "mlflow", "ui",
    "--backend-store-uri", "file:///content/drive/MyDrive/mlflow_runs",
    "--host", "0.0.0.0",
    "--port", "5000"
])
time.sleep(3)

url = ngrok.connect(5000)
print("MLflow UI:", url.public_url)
```

---

## 📡 API Reference

### `GET /`
Returns the Tez home page — ride type selector, city zone info, promo banner.

### `GET /book`
Generates a new ride event → publishes to Azure Event Hub → returns confirmation page.

**Template context passed to `confirmation.html`:**

| Variable | Description |
|---|---|
| `ride` | Full ride dict from `generate_uber_ride_confirmation()` |
| `vehicle_type` | Human-readable name e.g. `"Uber Go"` |
| `vehicle_icon` | Emoji e.g. `"🚗"` |
| `vehicle_desc` | Description e.g. `"Compact"` |
| `payment_method` | e.g. `"UPI"` |
| `ride_status` | `"Completed"` or `"Cancelled"` |
| `pickup_city` | e.g. `"Indiranagar"` |
| `dropoff_city` | e.g. `"Koramangala"` |
| `event_hub_result` | `True` / `False` |

---

## 💡 Key Design Decisions

**Why `StringType` for timestamps in the DLT schema?**
Timestamps arrive as ISO strings from the Kafka/JSON payload. Declaring them as `TimestampType` in the schema causes `DELTA_FAILED_TO_MERGE_FIELDS` when the bulk load and stream load try to merge into the same Delta table. The fix — keep them as `StringType` in the schema and cast with `.withColumn(..., col(...).cast(TimestampType()))` inside each flow.

**Why explicit `DoubleType` casts after `from_json`?**
When JSON values are whole numbers (e.g. `"tip_amount": 0`, `"base_fare": 50`), Spark's `from_json` infers them as `LongType` even when the schema declares `DoubleType`. Explicit `.cast(DoubleType())` after parsing guarantees type consistency and prevents `DELTA_MERGE_INCOMPATIBLE_DATATYPE` errors.

**Why `append_flow` instead of a standard streaming join?**
Joining a massive one-time historical batch (`bulk_rides`) with a continuous live stream in a single query creates huge state management overhead. `@dlt.append_flow` lets both sources write independently into the same target table — the batch runs once, the stream runs continuously, and Delta handles deduplication.

**Why Jinja2 for SQL joins?**
Hardcoded `LEFT JOIN` chains break every time a new mapping table is added. A Python dict drives the join configuration and Jinja2 renders the full SQL at runtime — adding a new dimension requires only one line in the config dict.

**Why Indian names instead of Faker?**
`Faker().name()` generates Western names. The project uses a curated pool of 261 Indian first names × 86 last names (North, South, and pan-Indian surnames) giving 22,446 unique combinations — realistic for a Bengaluru-based platform.

**Why `class_weight='balanced'` in the ML model?**
With 130+ Bengaluru zones as prediction classes, some zones appear far less frequently in the training data than others. Without `balanced`, the model learns to ignore rare zones entirely. `class_weight='balanced'` automatically adjusts class weights inversely proportional to frequency.

---

## 🔐 Environment Variables

| Variable | Description |
|---|---|
| `CONNECTION_STRING` | Full Azure Event Hub connection string with `EntityPath` |
| `EVENT_HUBNAME` | Event Hub topic name (e.g. `reallocationtopic`) |

---

## 🗺 City Zones Covered

| Zone | Region |
|---|---|
| Indiranagar · Whitefield · Marathahalli | East Bengaluru |
| Koramangala · Jayanagar · HSR Layout · Electronic City | South Bengaluru |
| MG Road | Central Bengaluru |
| Hebbal | North Bengaluru |
| Rajajinagar | West Bengaluru |

The ML model covers **130+ granular Bengaluru zones** including Basaveswaranagar, Malleswaram, Bellandur, Sarjapur Road, Yelahanka, and more.

---

*Built with ❤️ for Bengaluru &nbsp;·&nbsp; ತೇಜ್ — Swift as the wind, sharp as intent.*
