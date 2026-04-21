# 🏀 NBA End-to-End Data Engineering Pipeline

This project demonstrates a production-ready **Medallion Architecture** designed to ingest, transform, and analyze NBA performance data. It automates the flow from an S3-to-Snowflake ingestion layer into a highly modeled analytics suite using **dbt** and **Apache Airflow**.

## 🏗️ Architecture & Data Flow
The pipeline is built on a modular architecture that ensures data quality and historical traceability:

1.  **Ingestion:** Raw player, team, and game statistics are landed in **Amazon S3**.
2.  **Bronze (Raw):** Data is surfaced in **Snowflake** via external tables, preserving the original source format for auditability.
3.  **Silver (Transformation & History):** * **Snapshots (SCD Type 2):** Implemented dbt snapshots with a **Check Strategy** to track historical changes in player attributes, team affiliations, and seasonal standings.
    * **Fact & Dim Modeling:** Transforms raw data into a star schema featuring `fact_player_performances`, `fact_team_games`, `dim_players`, and `dim_teams`.
4.  **Gold (Curated Marts):** Final aggregation layer providing consumer-ready tables such as `mart_player_analytics` and `mart_team_analytics` optimized for BI tools.

## 🛠️ Tech Stack
* **Cloud Warehouse:** Snowflake
* **Data Transformation:** dbt (Data Build Tool)
* **Orchestration:** Apache Airflow (utilizing Astronomer Cosmos)
* **Cloud Storage:** Amazon S3
* **Environment:** Docker, Python, SQL

## ⏳ Slowly Changing Dimensions (SCD Type 2)
To handle the dynamic nature of the NBA (trades, physical updates, and ranking shifts), I implemented **dbt Snapshots**. 

* **Strategy:** `check`
* **Implementation:** Rather than relying on potentially missing source timestamps, the pipeline checks specific columns for changes. This ensures a reliable historical record is maintained with `dbt_valid_from` and `dbt_valid_to` logic, allowing for accurate "point-in-time" analysis.

## 🚦 Data Quality & Testing
Data integrity is enforced throughout the pipeline with a multi-layered testing strategy:
* **Referential Integrity:** Relationship tests ensure every performance record maps correctly to player and team dimensions.
* **Schema Validation:** Unique and Not Null constraints on primary keys (e.g., `box_score_id`).
* **Custom Logic:** Specialized dbt tests to handle data-specific edge cases, such as `MINUTES_PLAYED` formatting and conference mapping.

## 📂 Project Structure
The core logic resides within the `nba_pipeline` directory:
```text
nba_pipeline/
├── models/
│   ├── bronze/     # Raw external tables & staging
│   ├── silver/     # Fact and Dim tables (Normalized logic)
│   └── gold/       # Analytics Marts (Business aggregations)
├── snapshots/      # SCD Type 2 logic for players, teams, and standings
├── seeds/          # Static data (e.g., conference mappings)
└── tests/          # Custom data quality assertions