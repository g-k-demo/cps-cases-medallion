#!/usr/bin/env python
# coding: utf-8

# ## nb_bronze_to_silver
# 
# New notebook

# In[3]:


# CPS – Silver Layer (Create-or-MERGE Upsert)
# --------------------------------------------------------------------
# Purpose:
# - Read all regional CSVs from Bronze
# - Clean + parse dates + derive Year/Month for partition
# - Optional within-batch de-dupe cases
# - First run: create partitioned Delta table
# - Next runs: MERGE (upsert) by (CaseID) when newer data arrives determined by SourceModifiedTS
# - System columns:
#     CreatedTS   -> set on INSERT only (immutable load time)
#     ModifiedTS -> set on INSERT and UPDATE (last change time)

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# --------------------
# Configuration
# --------------------
BRONZE_ROOT = "Files/bronze"
SCHEMA = "silver"
TABLE = "cps_cases"
FQN = f"{SCHEMA}.{TABLE}"
WITHIN_BATCH_DEDUP = True

# --------------------
# 0) Ensure schema and set current DB
# --------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")

# --------------------
# 1) Read Bronze (explicit schema)
# --------------------
schema = """
  CaseID string,
  Region string,
  OffenceType string,
  AgeGroup string,
  Gender string,
  Ethnicity string,
  ChargeDate string,
  ResolutionDate string,
  Outcome string,
  SourceModifiedTS string
"""

bronze_path = f"{BRONZE_ROOT}/*/*.csv"
raw = spark.read.option("header", True).schema(schema).csv(bronze_path)


# --------------------
# 2) Clean + parse + derive
# --------------------

def norm_str(col):
    c = F.trim(F.col(col))
    return F.when(c.isNull() | (c == ""), F.lit("Unknown")).otherwise(c)

def norm_outcome(col):
    c = F.trim(F.col(col))
    return F.when(c.isNull() | (c == ""), F.lit("Open")).otherwise(c)

df = (
    raw.withColumn("CaseID", F.trim(F.col("CaseID")))
    .withColumn("Region", F.initcap(F.trim(F.col("Region"))))
    .withColumn("OffenceType", F.trim(F.col("OffenceType")))
    .withColumn("AgeGroup", norm_str("AgeGroup"))
    .withColumn("Gender", F.initcap(norm_str("Gender")))
    .withColumn("Ethnicity", F.initcap(norm_str("Ethnicity")))
    .withColumn("Outcome", norm_outcome("Outcome"))   # <-- Open if null/blank
    .withColumn("ChargeDate", F.to_date(F.col("ChargeDate"), "dd/MM/yyyy"))
    .withColumn("ResolutionDate", F.to_date(F.col("ResolutionDate"), "dd/MM/yyyy"))
    .withColumn("Year", F.year(F.col("ChargeDate")))
    .withColumn("Month", F.month(F.col("ChargeDate")))
    .withColumn(
        "SourceModifiedTS",
        F.to_timestamp(F.col("SourceModifiedTS"), "dd/MM/yyyy HH:mm"),
    )
)

# --------------------
# 3) Optional within-batch de-dupe
# --------------------
if WITHIN_BATCH_DEDUP:
    w = Window.partitionBy("CaseID").orderBy(
        F.col("SourceModifiedTS").desc_nulls_last()
    )
    df_latest = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
else:
    df_latest = df

# System timestamps
df_batch = df_latest.withColumn("CreatedTS", F.current_timestamp()).withColumn(
    "ModifiedTS", F.current_timestamp()
)

# --------------------
# 4) Create-if-not-exists (initial load)
# --------------------
table_exists = spark.catalog.tableExists(FQN)

if not table_exists:
    (
        df_batch.write.format("delta")
        .mode("overwrite")
        .partitionBy("Year", "Month")
        .option("overwriteSchema", "true")
        .saveAsTable(FQN)
    )
    print(f"Created table {FQN}")

else:
    # --------------------
    # 5) MERGE (upsert) uses SourceModifiedTS
    # --------------------
    df_batch.createOrReplaceTempView("src_batch")

    spark.sql(
        f"""
        MERGE INTO {FQN} t
        USING src_batch s
        ON t.CaseID = s.CaseID
        WHEN MATCHED AND (
             s.SourceModifiedTS IS NOT NULL
         AND (t.SourceModifiedTS IS NULL OR s.SourceModifiedTS > t.SourceModifiedTS)
        )
        THEN UPDATE SET
          t.OffenceType      = s.OffenceType,
          t.Region           = s.Region,
          t.AgeGroup         = s.AgeGroup,
          t.Gender           = s.Gender,
          t.Outcome          = s.Outcome,
          t.ChargeDate       = s.ChargeDate,
          t.ResolutionDate   = s.ResolutionDate,
          t.Year             = s.Year,
          t.Month            = s.Month,
          t.SourceModifiedTS = s.SourceModifiedTS,
          t.ModifiedTS       = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          CaseID, Region, OffenceType, AgeGroup, Gender, Ethnicity, Outcome,
          ChargeDate, ResolutionDate, Year, Month,
          SourceModifiedTS, CreatedTS, ModifiedTS
        ) VALUES (
          s.CaseID, s.Region, s.OffenceType, s.AgeGroup, s.Gender, s.Ethnicity, s.Outcome,
          s.ChargeDate, s.ResolutionDate, s.Year, s.Month,
          s.SourceModifiedTS, s.CreatedTS, s.ModifiedTS
        )
    """
    )

    print(f"Merged into {FQN}")

# --------------------
# 6) Optimize (Fabric V-Order)
# --------------------
spark.sql(f"OPTIMIZE {FQN}")

# Review data
display(
    spark.table(FQN)
    .groupBy("Region", "Year", "Month")
    .count()
    .orderBy("Region", "Year", "Month")
)

