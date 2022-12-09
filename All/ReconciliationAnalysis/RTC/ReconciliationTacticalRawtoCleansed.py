# Databricks notebook source
str_filePath = f"dbfs:/mnt/reconciliation/datalake/rawTocleanseTemp/"

df_reconciliationRawToCleansed = spark.read.parquet(str_filePath) \
                                           .filter("FileDateTime IN ('20220118_2000', '20220118_2200', '20220119_0000')")                                

# COMMAND ----------

df_reconciliationRawToCleansed.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM mymi_pre_gxlp.dbo_accrual

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/main/Raw/Eclipse/Internal/dbo_ClaimLine/2022/202202/20220206/20220206_22/dbo_ClaimLine_20220206_2200/data_16c15c2c-dd94-4a47-9ed7-b19c5c2d52a6_8fa080d6-a757-4722-a499-d8e51922bbe7.parquet").count()

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/main/Cleansed/Eclipse/Internal/dbo_ClaimLine/2022/202202/20220206/20220206_22/dbo_ClaimLine_20220206_2200.parquet/part-00000-tid-918919337270789332-68a08d02-b110-4e36-936e-b1f4e2e62f8a-4332-1-c000.snappy.parquet").count()