-- Databricks notebook source
-- MAGIC %md you can specify the format of your notebook, doesn't need to always be `%sql` to switch context

-- COMMAND ----------

select * from my_schema.export_taxi_rate_code

-- COMMAND ----------

-- MAGIC %md format sql

-- COMMAND ----------

select
  *
from
  my_schema.export_taxi_rate_code

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("select * from my_schema.export_taxi_rate_code").display()

-- COMMAND ----------


