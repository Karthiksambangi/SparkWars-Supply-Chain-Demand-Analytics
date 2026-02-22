# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark-Wars 4.0 – Supply Chain Demand Analytics
# MAGIC
# MAGIC ## Architecture: Medallion Data Engineering Pipeline
# MAGIC
# MAGIC This solution implements a scalable data pipeline using Databricks and Apache Spark:
# MAGIC
# MAGIC - Bronze Layer → Raw data ingestion from catalog
# MAGIC - Silver Layer → Data cleaning and transformation
# MAGIC - Gold Layer → Business KPI aggregation and analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer – Raw Data Ingestion
# MAGIC Raw demand forecast data is loaded from Databricks catalog into Spark DataFrame.

# COMMAND ----------

df_demand = spark.table("workspace.default.demand_forcast")

display(df_demand)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer – Data Cleaning & Structuring
# MAGIC
# MAGIC The silver layer transforms raw data into a clean structured dataset by:
# MAGIC - Removing null values
# MAGIC - Renaming columns for clarity
# MAGIC - Selecting required business columns

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = df_demand.dropna()


df_silver = df_silver.withColumnRenamed("material", "product_id")


df_silver = df_silver.select(
    "product_id",
    "period",
    "period_id"
)

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer – Business KPI Aggregation
# MAGIC
# MAGIC The gold layer creates analytical KPIs for business insights.
# MAGIC Aggregation is performed to calculate product distribution per period.

# COMMAND ----------

from pyspark.sql.functions import count


df_gold = df_silver.groupBy("period").agg(
    count("product_id").alias("total_products")
)

display(df_gold)

# COMMAND ----------

# Persist final KPI table (Gold Layer Output)
df_gold.write.mode("overwrite").saveAsTable("workspace.default.gold_kpi_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC - Spark transformations leverage distributed processing.
# MAGIC - Aggregations are executed in parallel using Databricks serverless compute.
# MAGIC - The medallion architecture ensures scalable and maintainable data pipelines.