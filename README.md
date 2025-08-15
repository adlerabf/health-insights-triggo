# Health Insights Data Warehouse

## Project Overview

The **Health Insights Data Warehouse** project is a robust analytics solution designed to process, clean, and model Brazilian hospital admission data (SIH) to support public health insights and financial analysis. This project leverages **dbt** for data transformation and **Snowflake** as the cloud data platform, with integration to **Power BI** for visualization.

## Documentation and Presentation

* DBT Docs: [Link](https://adlerabf.github.io/health-insights-triggo/#!/overview)
* Power BI Dashboard: [Link](https://app.powerbi.com/view?r=eyJrIjoiNDA2NDM3ZTEtYjQxMC00ODcxLThmZTYtZjc2YTJjOGZmMDhmIiwidCI6IjFiYzVlY2E4LTEzNjAtNDhhMy05NWRiLTM5ZmRmNDRiMWEzNiJ9)

---

## Architecture

### Data Source Overview

The primary dataset for this project originates from the **Hospital Information System (SIH) of Brazil**, which records hospital admissions across public healthcare facilities. The data includes patient demographics, admission and discharge dates, procedures performed, diagnostic codes (ICD), and associated costs.

To enrich the dataset, we integrated auxiliary sources:

- **Municipal Data**: City and region information from IBGE for patient and hospital localities.
- **Procedure Reference**: Official procedure names corresponding to the SIH procedure codes.
- **ICD Codes**: Descriptions of principal diagnoses using the International Classification of Diseases.

This combination ensures a **comprehensive and analyzable dataset** for healthcare insights, enabling demographic, procedural, and regional analyses.


The solution follows a **star schema design**, consisting of a fact table (`fact_sih_data`) and several dimension tables:

- **Fact Table**
  - `fact_sih_data`: Contains enriched hospital admission records with costs, procedures, ICD codes, and patient locality information.

- **Dimension Tables**
  - `dim_date`: Provides daily granularity for admission and discharge dates.
  - `dim_demographics`: Categorizes patients by age, sex, and age group.
  - `dim_disease`: Stores ICD codes and descriptions for principal diagnoses.
  - `dim_location`: Contains city, state, and region information for patient and hospital locations.
  - `dim_procedure`: Stores medical procedure codes and descriptions.

**Data Flow:**
1. Data Ingestion: Raw hospital admission data is extracted from Brazilian SIH databases using Databricks, leveraging PySUS library for efficient data retrieval and PySpark for large-scale data processing.
2. Data Storage: Processed datasets are securely stored in Amazon S3 buckets, establishing a reliable cloud storage infrastructure.
3. Raw data is staged in **Snowflake raw schema** from SIH datasets and auxiliary data (cities, procedures, ICD).
4. `int_sih_data` transforms and enriches raw SIH data with joins to cities, procedures, and ICD datasets.
5. Dimension tables are created from the cleaned intermediate data.
6. `fact_sih_data` is populated using surrogate keys for facts and references dimension keys.

---

## How to Run the Project

1. **Run Data Ingestion Notebook**
   - Ingesting the SIH, IBGE and ICD-10 data.
     
2. **Set up Snowflake**
   - Create database: `health_insights_db`
   - Create schemas: `raw`, `alaytics`
   - Stage raw files (sih_data, cities_data, procedures_data, ICD_data)

3. **Run dbt**
   ```bash
   dbt deps
   dbt run
   dbt test
   dbt docs generate

3. Connect Power BI

* Use Snowflake connector
* Load dimensions and fact table
* Create relationships according to the star schema

## Design Decisions & Justifications

* Star Schema: Chosen for query performance and simplicity in reporting.
* Surrogate Keys: Generated for admissions (admission_sk) and patients (patient_id) to maintain consistency and uniqueness.
* Patient Age Groups: Categorized for easier demographic analysis.
* City Matching: Used first 6 digits of municipality codes for patient and hospital locations to align with IBGE standards.
* ICD and Procedure Join: Enriches fact table for better analytics and reporting.
* Procedures docs from TabWin Datasus source.

## Innovations Implemented

* Dynamic surrogate key generation using MD5 hashes for admissions and patients.
* Integration of multiple auxiliary datasets to enrich hospital admission data.
* Fully dbt-managed transformations with automated testing for data quality.
* Ready-to-use Power BI dashboard with pre-built star schema for advanced analytics.

## Future Work

* Include additional metrics such as readmission rates and cost per procedure.
* Implement incremental loading for faster dbt runs.
* Expand dimensional analysis to include more regional and socio-economic factors.
* Snowpipe for auto-refresh data in Swnoflake
