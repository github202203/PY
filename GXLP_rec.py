# Databricks notebook source
df_dup= spark.read.format("parquet").option("header", "true").load("dbfs:/mnt/main/Raw/GXLP/Internal/dbo_Accrual/2022/202201/20220118/20220118_20/dbo_Accrual_20220118_2000/*.parquet")

# COMMAND ----------

df_dup.createOrReplaceTempView("GXLP_Accrual")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from GXLP_Accrual

# COMMAND ----------

df_cleansed_gxlp= spark.read.format("parquet").option("header", "true").load("dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_Accrual/2022/202201/20220118/20220118_20/dbo_Accrual_20220118_2000.parquet/*.parquet")

# COMMAND ----------

df_cleansed_gxlp.createOrReplaceTempView("GXLP_Accrual_cleansed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from GXLP_Accrual_cleansed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE DEFAULT.TRANS_GXLP_ACCRUAL USING DELTA LOCATION "dbfs:/mnt/main/Transformed/MyMI_Pre_GXLP/Internal/dbo_Accrual/"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from DEFAULT.TRANS_GXLP_ACCRUAL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * DataMartLoadID from DEFAULT.TRANS_GXLP_ACCRUAL -- where DataMartLoadID='2022011801' --215764040
