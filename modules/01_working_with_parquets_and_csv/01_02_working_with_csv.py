# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC 1. dbutils - a must when using Python
# MAGIC 2. mount points / accessing a storage
# MAGIC 3. how to access & query csv in a smart way
# MAGIC 4. how to access & query csv in a reliable way

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils
# MAGIC One of the most useful utility which you can use for everyday tasks.

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC /databricks-datasets contains a lot of dummy datasets that you can use for trainings, exploration Databricks capabilities etc.
# MAGIC
# MAGIC for this session we'll use /databricks-datasets/nyctaxi/ datasets

# COMMAND ----------

# MAGIC %md don't get too attached to mounts - with Unity Catalog Databricks is changing the way of accessing storages, it is based on using `abfss://...` which differs from mounts under many conditions. The most important is that **it is the same for using Spark API or dbutils**.

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/modules/01/")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/modules/01/nyctaxi_taxi_rate_code")

# COMMAND ----------

# MAGIC %md the fastest & the most convenient way

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/mnt/bronze/modules/01/nyctaxi_taxi_rate_code/taxi_rate_code.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/mnt/bronze/modules/01/nyctaxi_taxi_rate_code/`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW v_tmp_nyctaxi_taxi_rate_code
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'path' '/mnt/bronze/modules/01/nyctaxi_taxi_rate_code/', -- Specify the path to your CSV folder
# MAGIC   'header' 'true' -- Specify that the first row contains headers
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_tmp_nyctaxi_taxi_rate_code

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_tmp_nyctaxi_taxi_rate_code
# MAGIC where RateCodeID > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_tmp_nyctaxi_taxi_rate_code
# MAGIC where RateCodeID > "10"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW v_tmp_nyctaxi_taxi_rate_code
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'path' '/mnt/bronze/modules/01/nyctaxi_taxi_rate_code/', -- Specify the path to your CSV folder
# MAGIC   'header' 'true', -- Specify that the first row contains headers
# MAGIC   'inferSchema' 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_tmp_nyctaxi_taxi_rate_code
# MAGIC where RateCodeID > "10"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_tmp_nyctaxi_taxi_rate_code
# MAGIC where RateCodeID > 10

# COMMAND ----------

_sqldf.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nyctaxi_taxi_rate_code
# MAGIC AS 
# MAGIC SELECT * FROM v_tmp_nyctaxi_taxi_rate_code

# COMMAND ----------

# MAGIC %md reattach the notebook - it kills existing Spark session, creates a new one with

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_tmp_nyctaxi_taxi_rate_code

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyctaxi_taxi_rate_code

# COMMAND ----------

# MAGIC %md
# MAGIC do the same with python

# COMMAND ----------

df_nyctaxi_taxi_rate_code = (
    spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/mnt/bronze/modules/01/nyctaxi_taxi_rate_code/")
)

# COMMAND ----------

df_nyctaxi_taxi_rate_code.display()

# COMMAND ----------

df_nyctaxi_taxi_rate_code.filter("RateCodeID % 2 = 0").display()

# COMMAND ----------

df_nyctaxi_taxi_rate_code.write.mode("overwrite").saveAsTable("nyctaxi_taxi_rate_code")

# COMMAND ----------

# MAGIC %md
# MAGIC # External files
# MAGIC Assumptions:
# MAGIC - external files can be considered as small (for instance sent via email)
# MAGIC - contains reference data
# MAGIC - we don't expect any issues with complex cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC Create a view as select from copy/paste

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view v_nyctaxi_taxi_rate_code
# MAGIC as
# MAGIC select "Standard Rate" as RateCodeDesc
# MAGIC union all
# MAGIC select "JFK"
# MAGIC union all
# MAGIC select "Newark"
# MAGIC union all
# MAGIC select "Nassau or Westchester"
# MAGIC union all
# MAGIC select "Negotiated fare"
# MAGIC union all
# MAGIC select "Group ride"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_nyctaxi_taxi_rate_code

# COMMAND ----------

# MAGIC %sql
# MAGIC ;with CTE as(
# MAGIC select "Standard Rate" as RateCodeDesc
# MAGIC union all
# MAGIC select "JFK"
# MAGIC union all
# MAGIC select "Newark"
# MAGIC union all
# MAGIC select "Nassau or Westchester"
# MAGIC union all
# MAGIC select "Negotiated fare"
# MAGIC union all
# MAGIC select "Group ride"
# MAGIC )
# MAGIC select * from CTE

# COMMAND ----------

# MAGIC %md
# MAGIC Catalog -> Add Data -> Upload file ./export taxi rate code.csv from this project

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.export_taxi_rate_code

# COMMAND ----------

# MAGIC %md
# MAGIC verify with data profiling

# COMMAND ----------

# MAGIC %md
# MAGIC Manage Schemas & tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists my_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table my_schema.export_taxi_rate_code
# MAGIC as 
# MAGIC select * from default.export_taxi_rate_code

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_schema.export_taxi_rate_code

# COMMAND ----------


