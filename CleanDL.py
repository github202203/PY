# Databricks notebook source
from pyspark.sql.functions import desc, row_number, dense_rank, col, lit, explode, count
import re

# COMMAND ----------

# MAGIC %run /DataPlatform/Common/fn_listAllDataLakeDirectory

# COMMAND ----------

#lst_bronzePaths = fn_listAllDataLakeDirectory("mnt/bronze")
lst_eclipseBronzePaths = [p.replace("/_delta_log/","") for p in lst_bronzePaths if 'Eclipse' in p and re.match(".*_delta_log/$",p)]

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/mnt/bronze/Internal/Eclipse/DeltaLake/dbo_PolicyQuote")
display(df.groupBy("BronzeStagingSystemLoadID").agg(count("BronzeStagingSystemLoadID")).orderBy("BronzeStagingSystemLoadID"))

# COMMAND ----------

zip_watermarkValues = zip(arr_DeltaPaths, arr_DeltaTables, arr_MaxWatermarkValues)
df_watermarkValues = sqlContext.createDataFrame(zip_watermarkValues, StructType([StructField("DeltaPath", StringType(), True), StructField("DeltaTable", StringType(), True), StructField("MaxWatermark", LongType(), True)]))
