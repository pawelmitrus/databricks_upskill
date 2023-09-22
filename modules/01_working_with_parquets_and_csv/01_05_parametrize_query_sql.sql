-- Databricks notebook source
CREATE WIDGET DROPDOWN vendor DEFAULT "CMT" CHOICES SELECT * FROM (VALUES ("CMT"), ("VTS"), ("DDS"))

-- COMMAND ----------


CREATE WIDGET TEXT schema DEFAULT "my_schema"

-- COMMAND ----------

SHOW TABLES IN ${schema}

-- COMMAND ----------

SELECT * FROM my_schema.nyctaxi_yellow_2010
WHERE vendor_id = '${vendor}'

-- COMMAND ----------

REMOVE WIDGET schema;
REMOVE WIDGET vendor;

-- COMMAND ----------


