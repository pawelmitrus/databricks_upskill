# Databricks notebook source
# MAGIC %sh
# MAGIC rm -r /dbfs/mnt/bronze/modules/01/nyctaxi/tripdata/yellow_2010

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /dbfs/mnt/bronze/modules/01/nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2010* /dbfs/mnt/bronze/modules/01/nyctaxi_yellow_2010

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/mnt/bronze/modules/01/nyctaxi_taxi_rate_code

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /dbfs/mnt/bronze/modules/01/nyctaxi_taxi_rate_code

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv /dbfs/mnt/bronze/modules/01/nyctaxi_taxi_rate_code

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/nyctaxi")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/yellow")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/nyctaxi/taxizone")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv`

# COMMAND ----------

df_nyctaxi = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("/mnt/bronze/modules/01/nyctaxi_yellow_2010")
)

# COMMAND ----------

df_nyctaxi.display()

# COMMAND ----------

from pyspark.sql.functions import lit

(
    df_nyctaxi
    .withColumn("year", lit(2010))
    .write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("year")
    .save("/mnt/silver/modules/01/nyctaxi_yellow_2010")
)

# COMMAND ----------

(
    df_nyctaxi
    .write
    .format("parquet")
    .mode("overwrite")
    .save("/mnt/silver/modules/01/nyctaxi_yellow_2010_no_partition")
)

# COMMAND ----------

df.createOrReplaceTempView("v_tmp_nyctaxi_tripdata_yellow")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_tmp_nyctaxi_tripdata_yellow

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from v_tmp_nyctaxi_tripdata_yellow

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(pickup_datetime) from v_tmp_nyctaxi_tripdata_yellow

# COMMAND ----------


