# Databricks notebook source
# MAGIC 
# MAGIC %run ../../../Datalib/Common/Master

# COMMAND ----------

str_FilePath = f"dbfs:/mnt/main/Raw/GXLP210/Internal/dbo_CTR_PM_USAGE/2022/202203/20220324/20220324_10/dbo_CTR_PM_USAGE_20220324_1000/dbo.CTR_PM_USAGE.parquet"

# COMMAND ----------

df_unfilteredData = spark.read.format('parquet').option("header", "true").load(str_FilePath)
display(df_unfilteredData)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from mymi_pre_gxlp210.dbo_ctr_pm_usage

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from mymi_pre_gxlp210.dbo_ctr_pm_usage
# MAGIC WHERE unique_key > 2

# COMMAND ----------

str_FilePath = f"dbfs:/mnt/main/Reconciliation/Raw/GXLP210/Internal/dbo_Accrual/2022/202203/20220309/20220309_12/dbo_Accrual_20220309_1200.parquet"
df = spark.read.format('parquet').option("header", "true").load(str_FilePath)
display(df)

# COMMAND ----------

str_loadType = "PreAllocatedKey"

# COMMAND ----------

str_mode = "overwrite" if str_loadType == "PreAllocatedKey" else "append"
print(str_mode)
