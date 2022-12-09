# Databricks notebook source
FilePath = "/mnt/main/Snapshot/MyMI_Snapshot_Group/Internal/DWH_FactClaimTransaction/"
File=FilePath+"DWH_FactClaimTransaction_Latest.parquet"
df = spark.read.format("parquet").option("header", "true").load(File)
display(df)
#df.show(truncate=False)
df1 = df.filter(df("DL_DWH_DimAssured_ID") == 451014)
#df.createOrReplaceTempView("tempdf")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from tempdf where DL_DWH_DimProcessedDate_Id = 20220418 --< 99991231
# MAGIC --select * from tempdf where DL_DWH_DimProcessedDate_Id = 99991231

# COMMAND ----------

df = df.filter(df("DL_DWH_DimAssured_ID") == 9142).show()

# COMMAND ----------


