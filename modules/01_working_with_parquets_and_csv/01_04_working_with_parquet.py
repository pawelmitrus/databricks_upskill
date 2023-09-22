# Databricks notebook source
# MAGIC %md
# MAGIC parquet files are different from csv:
# MAGIC - columnar storage
# MAGIC - contains strict schema definition (in a scope of single file)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/modules/01/")

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/modules/01/nyctaxi_yellow_2010/year=2010/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`/mnt/silver/modules/01/nyctaxi_yellow_2010/`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW v_tmp_nyctaxi_yellow_2010
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   'path' '/mnt/silver/modules/01/nyctaxi_yellow_2010/', 
# MAGIC   'mergeSchema' 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_tmp_nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating External Tables
# MAGIC - metadata sits in Databricks
# MAGIC - data stays where it is

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE my_schema.nyctaxi_yellow_2010
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/silver/modules/01/nyctaxi_yellow_2010/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_schema.nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL my_schema.nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions my_schema.nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE my_schema.nyctaxi_yellow_2010;

# COMMAND ----------

# MAGIC %md now partitions were correctly discovered

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions my_schema.nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %md
# MAGIC whenever underlying data changes it requires refreshing the table

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table my_schema.nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %md
# MAGIC in order to speed up query execution it's beneficial to have statistics - but it takes time to calculate it and also needs to be refreshed when data changes

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE my_schema.nyctaxi_yellow_2010
# MAGIC COMPUTE STATISTICS FOR ALL COLUMNS

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED my_schema.nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_schema.nyctaxi_yellow_2010

# COMMAND ----------


