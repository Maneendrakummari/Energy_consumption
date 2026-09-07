# ⚡ Energy Grid Data Engineering Pipeline

## 🚀 Project Overview

The Energy Grid Data Engineering Pipeline is an end-to-end data engineering
project designed to ingest, process, validate, transform, and analyze
energy-grid data using a modern cloud data platform.

The project processes energy usage, device, weather, traffic, and grid-load
data through a layered Bronze → Silver → Gold architecture.

The solution uses Azure Data Lake Storage, Databricks, Delta Lake, dbt,
Apache Airflow, Slack monitoring, and GitHub for version control.

The final Gold layer provides dimensional and fact tables that can be used
for energy consumption, grid load, household, feeder, substation, operator,
and zone analysis.

Source Data -> Azure Data Lake Storage -> Azure Data Lake Storage -> Bronze Layer -> Silver Layer -> Gold Layer
 -> Analytics / Dashboards -> Monitoring & Alerts




 ## 🎯 Project Objectives

- Build an end-to-end energy grid data pipeline.
- Ingest raw energy-related datasets into Azure Data Lake Storage.
- Maintain raw source data in the Bronze layer.
- Clean and standardize data in the Silver layer.
- Apply data-quality and validation rules.
- Build dimensional and fact tables in the Gold layer.
- Implement a Star Schema for analytical workloads.
- Use dbt for SQL-based transformation and modelling.
- Use Apache Airflow for pipeline orchestration.
- Integrate Airflow with dbt Cloud.
- Implement failure handling and Slack notifications.
- Create analytics-ready datasets for energy-grid reporting.
- Maintain the project using Git and GitHub.





High-Level Architecture

<img width="1578" height="997" alt="image" src="https://github.com/user-attachments/assets/73665a46-da2e-4539-9f22-b7a2bc5b8169" />



🧩 Low-Level Design


<img width="1580" height="995" alt="Low-Level_Design" src="https://github.com/user-attachments/assets/99392aeb-e1fb-4c28-a10c-786755168eba" />



## 🛠️ Technology Stack

<table>
  <thead>
    <tr>
      <th>Technology</th>
      <th>Purpose</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Azure Data Lake Storage Gen2</strong></td>
      <td>Source/raw data storage</td>
    </tr>
    <tr>
      <td><strong>Databricks</strong></td>
      <td>Data processing and Delta tables</td>
    </tr>
    <tr>
      <td><strong>Apache Spark / PySpark</strong></td>
      <td>Distributed data processing</td>
    </tr>
    <tr>
      <td><strong>Delta Lake</strong></td>
      <td>Reliable storage for Bronze, Silver and Gold layers</td>
    </tr>
    <tr>
      <td><strong>Unity Catalog</strong></td>
      <td>Catalog management and data governance</td>
    </tr>
    <tr>
      <td><strong>dbt Cloud</strong></td>
      <td>SQL transformations and data modelling</td>
    </tr>
    <tr>
      <td><strong>Apache Airflow</strong></td>
      <td>Pipeline orchestration and workflow management</td>
    </tr>
    <tr>
      <td><strong>Slack</strong></td>
      <td>Pipeline and task failure notifications</td>
    </tr>
    <tr>
      <td><strong>Git</strong></td>
      <td>Version control</td>
    </tr>
    <tr>
      <td><strong>GitHub</strong></td>
      <td>Source-code repository and collaboration</td>
    </tr>
    <tr>
      <td><strong>Databricks SQL</strong></td>
      <td>Data exploration, SQL queries, reporting and dashboards</td>
    </tr>
  </tbody>
</table>



## 📂 Source Datasets

The Energy Grid pipeline processes multiple energy-related datasets.

<table>
  <thead>
    <tr>
      <th>Dataset</th>
      <th>Purpose</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>energy_usage_stream</strong></td>
      <td>Household energy consumption and electrical measurements</td>
    </tr>
    <tr>
      <td><strong>device_metrics_stream_v2_raw</strong></td>
      <td>Device performance and energy consumption</td>
    </tr>
    <tr>
      <td><strong>weather_source_v2_messy</strong></td>
      <td>Weather and environmental measurements</td>
    </tr>
    <tr>
      <td><strong>tariff_metrics_stream_v2</strong></td>
      <td>Electricity tariff and billing information</td>
    </tr>
    <tr>
      <td><strong>grid_load_stream_messy</strong></td>
      <td>Grid load, capacity and substation measurements</td>
    </tr>
  </tbody>
</table>


# 🥉 Bronze Layer – Raw Data

The Bronze layer stores the raw source data with minimal transformation.

### Bronze Responsibilities

- Preserve the original source structure.
- Store raw records in Delta format.
- Maintain source-level data.
- Provide a reliable starting point for downstream processing.
- Preserve data for reprocessing and auditing.
- Support downstream Silver transformations.

```text
Azure Data Lake Storage
          │
          ▼
       Bronze
          │
          ├── energy_metrics
          ├── device_metrics
          ├── grid_load
          ├── traffic_metrics
          └── weather
```

  

# 🥈 Silver Layer – Data Cleaning & Transformation

The Silver layer converts raw Bronze data into clean, standardized and
validated datasets.

### Key Activities

- Remove duplicate records.
- Handle NULL values.
- Trim and standardize attributes.
- Convert columns to appropriate data types.
- Validate energy measurements.
- Validate grid measurements.
- Standardize categorical values.
- Validate timestamps.
- Apply business rules.
- Generate data-quality indicators.


```text
energydbs.silver
│
├── energy_metrics
├── silver_device_metrics
├── silver_grid_load_metrics
├── silver_traffic_metrics
└── silver_weather_metrics
```


# 🥇 Gold Layer – Dimensional Model

The Gold layer provides business-ready dimensional and fact tables
for energy-grid analytics.

The model follows a Star Schema.

```
text
energydbs.gold
│
├── dim_household
├── dim_feeder
├── dim_operator
├── dim_substation
├── dim_zone
└── fact_grid_load
```

Gold Data Model

<img width="1536" height="1024" alt="WhatsApp Image 2026-09-04 at 5 50 16 PM" src="https://github.com/user-attachments/assets/558fb50f-7afb-4db9-b6cc-050071bdf364" />

### Fact Grain

The `fact_grid_load` table represents the grain of a grid-load
measurement/record at the source measurement level.

The dimension tables provide descriptive attributes for households,
feeders, operators, substations and zones.

Dimension Tables

dim_household
Purpose:
Stores descriptive attributes related to energy consumers/households.

Important attributes:
- household_id
- region_name
- city_name
- meter_type
- customer_category
- grid_zone

dim_feeder

Purpose:
Stores feeder-related information used for grid distribution analysis.

dim_operator

Purpose:
Stores grid operator information.

dim_substation

Purpose:
Stores substation-related information.

dim_zone

Purpose:
Stores distribution/grid-zone information.

## FACT_GRID_LOAD

The `fact_grid_load` table contains measurable grid-load metrics used
for analytical reporting.

Typical measures include:

- Grid load
- Grid capacity
- Transformer load
- Line loss
- Load variation
- Frequency variation
- Demand forecast
- Reserve margin


Source-to-Target Mapping

```
text
Bronze
energy_metrics
      │
      ▼
Silver
energy_metrics
      │
      ▼
Gold
dim_household
      │
      └───────────────┐
                      ▼
                 fact_grid_load



```


### Transformation Rules

- Trim string columns.
- Standardize categorical values.
- Convert numeric fields to appropriate data types.
- Validate electrical measurements.
- Handle NULL values.
- Remove duplicates.
- Validate timestamps.
- Standardize region and zone attributes.
- Map source identifiers to dimensional attributes.



# 🧪 Data Quality & Testing

Data quality is implemented across the Bronze, Silver and Gold layers.

### Validation Areas

- Schema validation
- NULL validation
- Duplicate validation
- Data type validation
- Range validation
- Timestamp validation
- Referential validation
- Business-rule validation
- Row-count validation
- Transformation validation



# 🧰 dbt Implementation

dbt is used for SQL-based transformation and dimensional modelling.

The dbt project reads Bronze tables from Databricks and creates
Silver and Gold models.

### Source

energydbs.bronze

### Silver

energydbs.silver

### Gold

energydbs.gold


```
text

dbt/
│
├── models/
│   ├── silver/
│   │   ├── energy_metrics.sql
│   │   ├── silver_device_metrics.sql
│   │   ├── silver_grid_load_metrics.sql
│   │   ├── silver_traffic_metrics.sql
│   │   └── silver_weather_metrics.sql
│   │
│   └── gold/
│       ├── dim_household.sql
│       ├── dim_feeder.sql
│       ├── dim_operator.sql
│       ├── dim_substation.sql
│       ├── dim_zone.sql
│       └── fact_grid_load.sql
│
├── macros/
│   └── generate_schema_name.sql
│
├── tests/
│
├── sources.yml
└── dbt_project.yml
```




# 🔄 Pipeline Orchestration

Apache Airflow is used to orchestrate the dbt Cloud transformation pipeline.

Airflow triggers the dbt Cloud deployment job, waits for its completion,
and receives the final execution status.



Airflow
   │
   ▼
DbtCloudRunJobOperator
   │
   ▼
dbt Cloud
   │
   ▼
ENERGY_GRID_AIRFLOW_JOB
   │
   ▼
dbt build
   │
   ▼
Databricks



# 🚨 Failure Handling & Slack Alerts

The pipeline implements centralized task-failure handling in Airflow.

When an Airflow task fails, the failure callback sends a notification
to Slack.

The Slack notification contains:

- DAG name
- Task name
- Execution time
- Failure status
- Instructions to check Airflow/dbt Cloud logs

```
text

Airflow Task
     │
     ├── SUCCESS
     │      │
     │      ▼
     │   Continue
     │
     └── FAILURE
            │
            ▼
      failure_handler.py
            │
            ▼
        Slack Webhook
            │
            ▼
       🚨 Alert

```



# 📊 Monitoring

The pipeline provides monitoring at multiple levels.

### Airflow

- DAG execution
- Task status
- Task failures
- Execution logs

### dbt Cloud

- dbt build status
- Model execution
- Test execution
- Transformation logs

### Databricks

- Job execution
- Table creation
- Transformation execution
- Data validation

### Slack

- Task failure notifications





# 🔐 Version Control & Security

Git and GitHub are used for source-code version control.

The repository contains:

- Airflow DAGs
- Failure handlers
- dbt models
- dbt macros
- Tests
- Documentation
- Architecture diagrams
- Configuration templates

Sensitive credentials are not committed to GitHub.

The following must remain outside source control:

- Databricks PAT
- dbt Cloud API token
- Slack webhook URL
- Client secrets
- Database passwords


```
text

Energy-Grid-Data-Engineering/
│
├── Alerts/
│   └── Slack_Failure_Alert.md
│
├── Dashboards/
│   └── Energy_Grid_Dashboard.pdf
│
├── Datasets/
│   └── Source/
│       ├── energy_usage_stream.csv
│       ├── device_metrics_stream_v2_raw.csv
│       ├── weather_source_v2_messy.csv
│       ├── tariff_metrics_stream_v2.csv
│       └── grid_load_stream_messy.csv
│
├── Design/
│   ├── high_level_architecture.png
│   ├── low_level_design.png
│   ├── data_model_diagram.png
│   └── data_lake_architecture.png
│
├── Development/
│   ├── Bronze/
│   ├── Silver/
│   └── Gold/
│       ├── Dimension Tables/
│       └── Fact Tables/
│
├── dbt/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── tests/
│   ├── sources.yml
│   └── dbt_project.yml
│
├── Airflow/
│   ├── dags/
│   │   ├── energy_grid_dbt_pipeline.py
│   │   └── failure_handler.py
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
│
├── Tests/
│   ├── Test_Bronze.sql
│   ├── Test_Silver.sql
│   └── Test_Gold.sql
│
├── README.md
└── .gitignore

```



# 📌 Key Outcomes

- Implemented an end-to-end Energy Grid data engineering pipeline.
- Implemented Bronze, Silver and Gold architecture.
- Processed energy, device, weather, traffic and grid-load datasets.
- Built standardized Silver Delta tables.
- Implemented Gold dimensional and fact models.
- Implemented a Star Schema for analytical workloads.
- Implemented dbt Cloud transformations.
- Successfully executed dbt build against Databricks.
- Implemented Apache Airflow orchestration.
- Successfully integrated Airflow with dbt Cloud.
- Implemented centralized Airflow failure handling.
- Integrated Slack failure notifications.
- Implemented Git/GitHub version control.
- Created an analytics-ready Energy Grid data platform.




# 🔮 Future Enhancements

- Automated CI/CD using GitHub Actions.
- Automated dbt documentation generation.
- Automated data-quality reporting.
- Enhanced Databricks dashboarding.
- Pipeline SLA monitoring.
- Centralized audit logging.
- Advanced grid-load analytics.
- Real-time streaming ingestion.
- Automated anomaly detection.
- Advanced energy-consumption forecasting.
- Production-grade Airflow scheduling.







