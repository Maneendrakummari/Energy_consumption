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
