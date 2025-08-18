# CPS Cases — Medallion on Microsoft Fabric (Azure)

## Overview
This project demonstrates a **Medallion Architecture (Bronze → Silver → Gold)** implementation in **Microsoft Fabric** for processing **CPS (Crown Prosecution Service)** case data by region.  
The **Bronze** layer ingests raw regional CSV files from a public GitHub repository into a **Fabric Lakehouse**. The **Silver** and **Gold** layers progressively clean, transform, and aggregate this data for analytics.

> All data in this repository is **fictional** and provided for demonstration purposes.

**Tech highlights:** Microsoft Fabric **Lakehouse (OneLake)** • **Data Pipeline** • **PySpark** • **Spark SQL** • **Delta Lake** • **MERGE** upserts • **Partitioning** • **V-Order OPTIMIZE** • **Power BI** (semantic model).

---

## Medallion stages (at a glance)

**Bronze — Raw landing (Lakehouse Files)**
- Fabric **Data Pipeline** (`Ingest_Regional_Files`) copies each region’s CSV to `Files/bronze/<Region>/<Region>.csv`.
- No transformations; audit-friendly raw store.

**Silver — Curated Delta (PySpark + Spark SQL)**
- Cleansing/standardisation, date parsing, and `Year`/`Month` derivation in **PySpark**.
- Within-batch de-dup by latest `SourceModifiedTS`.
- Writes **Delta** table **`silver.cps_cases`** (partitioned by `Year, Month`) and uses **Spark SQL MERGE** for upserts.
- Optimised via **OPTIMIZE (V-Order)** in Fabric.

**Gold — Star schema (Spark SQL + PySpark)**
- Dimensions: `gold.dim_date`, `gold.dim_offence`, `gold.dim_outcome`, `gold.dim_demographic`, `gold.dim_region`.
- Fact: `gold.fact_cps_case` (partitioned by charge year/month); Type-1 upsert by `CaseID`.
- Exposed through a **Power BI** semantic model (visuals shown via screenshots).

---

## Repository map

**Source — Synthetic CPS Cases (sample data)**
- [synthetic_cps_cases_London.csv](https://github.com/g-k-demo/cps-cases-medallion/blob/main/synthetic_cps_cases/synthetic_cps_cases_London.csv)
- [synthetic_cps_cases_North_West.csv](https://github.com/g-k-demo/cps-cases-medallion/blob/main/synthetic_cps_cases/synthetic_cps_cases_North_West.csv)
- [synthetic_cps_cases_South_East.csv](https://github.com/g-k-demo/cps-cases-medallion/blob/main/synthetic_cps_cases/synthetic_cps_cases_South_East.csv)
- [synthetic_cps_cases_West_Midlands.csv](https://github.com/g-k-demo/cps-cases-medallion/blob/main/synthetic_cps_cases/synthetic_cps_cases_West_Midlands.csv)
- [synthetic_cps_cases_Yorkshire.csv](https://github.com/g-k-demo/cps-cases-medallion/blob/main/synthetic_cps_cases/synthetic_cps_cases_Yorkshire.csv)

**0 — Orchestrator**
- [0 CPS Cases — Medallion Orchestrator pipeline.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/0%20CPS%20Cases%20%E2%80%94%20Medallion%20Orchestrator%20pipeline.png) *(Shows Bronze pipeline → Silver notebook → Gold notebook orchestration.)*

**1 — Bronze folder contains**
- [1 Ingest_Regional_Files pipeline and lookup setting.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/1%20Bronze/1%20Ingest_Regional_Files%20pipeline%20and%20lookup%20setting.png)
- [1_1 Regions.json](https://github.com/g-k-demo/cps-cases-medallion/blob/main/1%20Bronze/1_1%20Regions.json)
- [2 ForEach settings.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/1%20Bronze/2%20ForEach%20settings.png)
- [3 Copy Activity – Source tab - with dynamic URL expression.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/1%20Bronze/3%20Copy%20Activity%20%E2%80%93%20Source%20tab%20-%20with%20dynamic%20URL%20expression.png)
- [4 Copy Activity – Sink tab – with dynamic folder and file name expressions.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/1%20Bronze/4%20Copy%20Activity%20%E2%80%93%20Sink%20tab%20%E2%80%93%20with%20dynamic%20folder%20and%20file%20name%20expressions.png)
- [5 Lakehouse Bronze folder view – showing region folders with CSV files after the run.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/1%20Bronze/5%20Lakehouse%20Bronze%20folder%20view%20%E2%80%93%20showing%20region%20folders%20with%20CSV%20files%20after%20the%20run.png)

**2 — Silver folder contains**
- [6 Notebook Bronze to Silver output.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/2%20Silver/6%20Notebook%20Bronze%20to%20Silver%20output.png)
- [6_1 nb_bronze_to_silver.py](https://github.com/g-k-demo/cps-cases-medallion/blob/main/2%20Silver/6_1%20nb_bronze_to_silver.py)

**3 — Gold folder contains**
- [7 Notebook Silver to Gold output.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/3%20Gold/7%20%20Notebook%20Silver%20to%20Gold%20output.png)
- [7 nb_silver_to_gold.py](https://github.com/g-k-demo/cps-cases-medallion/blob/main/3%20Gold/7_1%20nb_silver_to_gold.py)
- [8 CPS Cases Gold Semantic Model.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/3%20Gold/8%20CPS%20Cases%20Gold%20Semantic%20Model.png)
- [9 Test Gold Semantic Model using visuals in Power BI.png](https://github.com/g-k-demo/cps-cases-medallion/blob/main/3%20Gold/9%20Test%20Gold%20Semantic%20Model%20using%20visuals%20in%20Power%20BI.png)


---

## How to explore (for CPS reviewers)

- Open the **Silver** notebook (`6_1 nb_bronze_to_silver.py`) to see the PySpark cleansing and **Spark SQL MERGE** logic writing to the Lakehouse (`silver.cps_cases`).
- Open the **Gold** notebook (`7_1 nb_silver_to_gold.py`) to see dimension/fact creation, partitioning, and **OPTIMIZE** steps.
- Use the **screenshots** to verify the pipeline configuration, Delta outputs, semantic model, and example visuals.

---

## Azure/Fabric alignment

- Built on **Microsoft Fabric** (Azure) Lakehouse with **Delta Lake** storage.
- Uses **Data Pipeline** patterns familiar to Azure Data Factory users.
- Leverages **PySpark** and **Spark SQL** for transformations, governance, and incremental upserts.






