# Databricks notebook source
str_filePath = f"dbfs:/mnt/reconciliation/datalake/rawTocleanseTemp/"

df_reconciliationRawToCleansed = spark.read.parquet(str_filePath) \
                                           .filter("FileDateTime IN ('20220104_2000', '20220104_2200', '20220105_0000')")                                

# COMMAND ----------

df_reconciliationRawToCleansed.display()

# COMMAND ----------


