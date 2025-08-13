#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold
# 
# null

# In[1]:


# CPS – Gold Layer Notebook
# -----------------------------------------------------
# Purpose: Build star schema from Silver curated data.
#          Creates dimensions + fact at case grain:
#          - dim_date, gold.dim_offence, gold.dim_outcome, gold.dim_demographics, gold.dim_region
#          - gold.fact_cases (partitioned by Year/Month)

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import datetime as dt

# --------------------
# 0) Config & ensure schemas
# --------------------
SILVER_FQN = "silver.cps_cases"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# --------------------
# 1) Load source (Silver)
# --------------------
src = spark.table(SILVER_FQN)

src.createOrReplaceTempView("src")

# --------------------
# 2) Build/refresh Date Dimension (gold.dim_date)
#    Range: min(ChargeDate) .. max(ChargeDate, ResolutionDate)
# --------------------
agg = src.select(
    F.min("ChargeDate").alias("minChargeDate"),
    F.max("ChargeDate").alias("maxChargeDate"),
    F.max("ResolutionDate").alias("maxResolutionDate")
).first()

min_charge = agg["minChargeDate"]
max_edge = max(d for d in [agg["maxChargeDate"], agg["maxResolutionDate"]] if d is not None)

# Build contiguous date series in PySpark
days = (max_edge - min_charge).days + 1
date_df = (
    spark.range(0, days)
    .select(F.expr(f"date_add(date'{min_charge.strftime('%Y-%m-%d')}', CAST(id AS INT))").alias("Date"))
    .withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int"))
    .withColumn("Year", F.year("Date"))
    .withColumn("Quarter", F.quarter("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("MonthName", F.date_format("Date", "MMMM"))
    .withColumn("MonthYearKey", F.date_format("Date", "yyyyMM").cast("int"))  # e.g., 202501
    .withColumn("MonthYear", F.date_format("Date", "MMM yyyy"))               # e.g., Jan 2025
    .withColumn("Day", F.dayofmonth("Date"))
    .withColumn("DayOfWeek", F.date_format("Date", "EEEE"))
    .withColumn("WeekOfYear", F.weekofyear("Date"))
)

# Replace date dim each run (deterministic)
(
    date_df.select(
        "DateKey","Date","Year","Quarter","Month","MonthName",
        "MonthYearKey","MonthYear",
        "Day","DayOfWeek","WeekOfYear"
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_date")
)

# --------------------
# 3) Demographic / Offence / Outcome Dims
# --------------------
# DEMOGRAPHIC
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.dim_demographic (
  DemographicKey BIGINT,
  AgeGroup STRING,
  Gender STRING,
  Ethnicity STRING
) USING DELTA
""")
# Append only: generate surrogate keys as MAX(key)+ROW_NUMBER() for new members
spark.sql(f"""
INSERT INTO {GOLD_SCHEMA}.dim_demographic
SELECT start + ROW_NUMBER() OVER (ORDER BY AgeGroup, Gender, Ethnicity) AS DemographicKey,
       AgeGroup, Gender, Ethnicity
FROM (
  SELECT nm.*
  FROM (SELECT DISTINCT AgeGroup, Gender, Ethnicity FROM src) nm
  LEFT ANTI JOIN {GOLD_SCHEMA}.dim_demographic d
    ON nm.AgeGroup=d.AgeGroup AND nm.Gender=d.Gender AND nm.Ethnicity=d.Ethnicity
) t
CROSS JOIN (SELECT COALESCE(MAX(DemographicKey),0) AS start FROM {GOLD_SCHEMA}.dim_demographic)
""")

# OFFENCE
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.dim_offence (
  OffenceKey BIGINT,
  OffenceType STRING
) USING DELTA
""")
# Append only: generate surrogate keys as MAX(key)+ROW_NUMBER() for new members
spark.sql(f"""
INSERT INTO {GOLD_SCHEMA}.dim_offence
SELECT start + ROW_NUMBER() OVER (ORDER BY OffenceType) AS OffenceKey,
       OffenceType
FROM (
  SELECT nm.*
  FROM (SELECT DISTINCT OffenceType FROM src WHERE OffenceType IS NOT NULL AND OffenceType <> '') nm
  LEFT ANTI JOIN {GOLD_SCHEMA}.dim_offence d
    ON nm.OffenceType = d.OffenceType
) t
CROSS JOIN (SELECT COALESCE(MAX(OffenceKey),0) AS start FROM {GOLD_SCHEMA}.dim_offence)
""")

# OUTCOME
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.dim_outcome (
  OutcomeKey BIGINT,
  Outcome STRING
) USING DELTA
""")
# Append only: generate surrogate keys as MAX(key)+ROW_NUMBER() for new members
spark.sql(f"""
INSERT INTO {GOLD_SCHEMA}.dim_outcome
SELECT start + ROW_NUMBER() OVER (ORDER BY Outcome) AS OutcomeKey,
       Outcome
FROM (
  SELECT nm.*
  FROM (SELECT DISTINCT Outcome FROM src WHERE Outcome IS NOT NULL AND Outcome <> '') nm
  LEFT ANTI JOIN {GOLD_SCHEMA}.dim_outcome d
    ON nm.Outcome = d.Outcome
) t
CROSS JOIN (SELECT COALESCE(MAX(OutcomeKey),0) AS start FROM {GOLD_SCHEMA}.dim_outcome)
""")

# REGION
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.dim_region (
  RegionKey BIGINT,
  Region STRING
) USING DELTA
""")
# Append only: generate surrogate keys as MAX(key)+ROW_NUMBER() for new members
spark.sql(f"""
INSERT INTO {GOLD_SCHEMA}.dim_region
SELECT start + ROW_NUMBER() OVER (ORDER BY Region) AS RegionKey,
       Region
FROM (
  SELECT nm.*
  FROM (SELECT DISTINCT Region FROM src WHERE Region IS NOT NULL AND Region <> '') nm
  LEFT ANTI JOIN {GOLD_SCHEMA}.dim_region d
    ON nm.Region = d.Region
) t
CROSS JOIN (SELECT COALESCE(MAX(RegionKey),0) AS start FROM {GOLD_SCHEMA}.dim_region)
""")

# --------------------
# 4) Build Fact view by joining dims
# --------------------
dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date").alias("dd")
dim_demo = spark.table(f"{GOLD_SCHEMA}.dim_demographic").alias("dem")
dim_off  = spark.table(f"{GOLD_SCHEMA}.dim_offence").alias("off")
dim_out  = spark.table(f"{GOLD_SCHEMA}.dim_outcome").alias("outc")
dim_reg  = spark.table(f"{GOLD_SCHEMA}.dim_region").alias("reg")

# Prepare source with standardized attribute values to match dims
s = src.alias("s")

# Join to date dim twice (charge/resolution)
fact_df = (
    s
    # Demographic
    .join(dim_demo, on=[
        s.AgeGroup == dim_demo.AgeGroup,
        s.Gender == dim_demo.Gender,
        s.Ethnicity == dim_demo.Ethnicity
    ], how="left")
    # Offence
    .join(dim_off, s.OffenceType == dim_off.OffenceType, "left")
    # Outcome
    .join(dim_out, s.Outcome == dim_out.Outcome, "left")
    # Region
    .join(dim_reg, s.Region == dim_reg.Region, "left")
    # Charge DateKey
    .join(dim_date.alias("dch"), s.ChargeDate == F.col("dch.Date"), "left")
    # Resolution DateKey
    .join(dim_date.alias("drs"), s.ResolutionDate == F.col("drs.Date"), "left")
    .select(
        s.CaseID,
        F.col("dem.DemographicKey").alias("DemographicKey"),
        F.col("off.OffenceKey").alias("OffenceKey"),
        F.col("outc.OutcomeKey").alias("OutcomeKey"),
        F.col("reg.RegionKey").alias("RegionKey"),
        F.col("dch.DateKey").cast("int").alias("ChargeDateKey"),
        F.col("drs.DateKey").cast("int").alias("ResolutionDateKey"),
        s.SourceModifiedTS,
        F.current_timestamp().alias("CreatedTS"),
        F.current_timestamp().alias("ModifiedTS"),
        F.year(s.ChargeDate).alias("ChargeYear"),
        F.month(s.ChargeDate).alias("ChargeMonth")
    )
)

fact_df.createOrReplaceTempView("vw_fact_upsert")

# --------------------
# 5) Create Fact table (if not exists) and MERGE (upsert by CaseID, using SourceModifiedTS)
# --------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.fact_cps_case (
  CaseID STRING,
  DemographicKey BIGINT,
  OffenceKey BIGINT,
  OutcomeKey BIGINT,
  RegionKey BIGINT,
  ChargeDateKey INT,
  ResolutionDateKey INT,
  SourceModifiedTS TIMESTAMP,
  CreatedTS TIMESTAMP,
  ModifiedTS TIMESTAMP,
  ChargeYear INT,
  ChargeMonth INT
) USING DELTA
PARTITIONED BY (ChargeYear, ChargeMonth)
""")

# Upsert by CaseID using SourceModifiedTS (Type-1)
spark.sql(f"""
MERGE INTO {GOLD_SCHEMA}.fact_cps_case t
USING vw_fact_upsert s
ON t.CaseID = s.CaseID
WHEN MATCHED AND (
     s.SourceModifiedTS IS NOT NULL
 AND (t.SourceModifiedTS IS NULL OR s.SourceModifiedTS > t.SourceModifiedTS)
)
THEN UPDATE SET
  t.DemographicKey     = s.DemographicKey,
  t.OffenceKey         = s.OffenceKey,
  t.OutcomeKey         = s.OutcomeKey,
  t.RegionKey          = s.RegionKey,
  t.ChargeDateKey      = s.ChargeDateKey,
  t.ResolutionDateKey  = s.ResolutionDateKey,
  t.SourceModifiedTS   = s.SourceModifiedTS,
  t.ModifiedTS         = current_timestamp(),
  t.ChargeYear         = s.ChargeYear,
  t.ChargeMonth        = s.ChargeMonth
WHEN NOT MATCHED THEN INSERT (
  CaseID, DemographicKey, OffenceKey, OutcomeKey, RegionKey,
  ChargeDateKey, ResolutionDateKey, SourceModifiedTS,
  CreatedTS, ModifiedTS, ChargeYear, ChargeMonth
) VALUES (
  s.CaseID, s.DemographicKey, s.OffenceKey, s.OutcomeKey, s.RegionKey,
  s.ChargeDateKey, s.ResolutionDateKey, s.SourceModifiedTS,
  current_timestamp(), current_timestamp(), s.ChargeYear, s.ChargeMonth
)
""")

# --------------------
# 6) Optimize (Fabric V-Order)
# --------------------
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.dim_date")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.dim_demographic")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.dim_offence")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.dim_outcome")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.fact_cps_case")

# --------------------
# Review Data
# --------------------
display(spark.table(f"{GOLD_SCHEMA}.dim_demographic").orderBy("DemographicKey").limit(20))
display(spark.table(f"{GOLD_SCHEMA}.fact_cps_case").groupBy("ChargeYear","ChargeMonth").count().orderBy("ChargeYear","ChargeMonth"))

