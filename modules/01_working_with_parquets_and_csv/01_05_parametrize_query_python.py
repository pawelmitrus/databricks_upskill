# Databricks notebook source
# MAGIC %md
# MAGIC 1. create a notebook that will be used on regular basis to analyze data
# MAGIC 2. add parameters that users can change
# MAGIC 3. make interactive queries

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

dbutils.widgets.text("vendor_freetext", "type here something")

# COMMAND ----------

par_vendor_freetext = dbutils.widgets.get("vendor_freetext")
par_vendor_freetext

# COMMAND ----------

spark.sql(
    f"""
    select * from my_schema.nyctaxi_yellow_2010
    where vendor_id = "{par_vendor_freetext}"
    """
).display()

# COMMAND ----------

dbutils.widgets.dropdown("rate_code", "JFK", ["Standard Rate", "JFK", "Newark", "Nassau or Westchester", "Negotiated fare", "Group ride"])
par_rate_code = dbutils.widgets.get("rate_code")

# COMMAND ----------

spark.sql(
    f"""
    select vendor_id, count(*) from my_schema.nyctaxi_yellow_2010 as f
    inner join my_schema.export_taxi_rate_code as d on d.RateCodeID = f.rate_code
    where d.RateCodeDesc = "{par_rate_code}"
    group by f.vendor_id
    """
).display()

# COMMAND ----------

dbutils.widgets.removeAll()
