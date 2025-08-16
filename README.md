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

**0 — Orchestrator**
- `0 CPS Cases — Medallion Orchestrator pipeline.png`  
  *(Shows Bronze pipeline → Silver notebook → Gold notebook orchestration.)*

**1 — Bronze folder contains**
- `1 Ingest_Regional_Files pipeline and lookup setting.png`  
- `1_1 Regions.json`  
- `2 ForEach settings.png`  
- `3 Copy Activity – Source tab - with dynamic URL expression.png`  
- `4 Copy Activity – Sink tab – with dynamic folder and file name expressions.png`  
- `5 Lakehouse Bronze folder view – showing region folders with CSV files after the run.png`

**2 — Silver folder contains**
- `6 Notebook Bronze to Silver output.png`  
- `6_1 nb_bronze_to_silver.py`

**3 — Gold folder contains**
- `7 Notebook Silver to Gold output.png` 
- `7 nb_silver_to_gold.py`   
- `8 CPS Cases Gold Semantic Model.png`  
- `9 Test Gold Semantic Model using visuals in Power BI.png`

---

## How to explore (for CPS reviewers)

- Open the **Silver** notebook (`6_1 nb_bronze_to_silver.py`) to see the PySpark cleansing and **Spark SQL MERGE** logic writing to the Lakehouse (`silver.cps_cases`).
- Open the **Gold** notebook (`7 nb_silver_to_gold.py`) to see dimension/fact creation, partitioning, and **OPTIMIZE** steps.
- Use the **screenshots** to verify the pipeline configuration, Delta outputs, semantic model, and example visuals.

---

## Azure/Fabric alignment

- Built on **Microsoft Fabric** (Azure) Lakehouse with **Delta Lake** storage.
- Uses **Data Pipeline** patterns familiar to Azure Data Factory users.
- Leverages **PySpark** and **Spark SQL** for transformations, governance, and incremental upserts.



