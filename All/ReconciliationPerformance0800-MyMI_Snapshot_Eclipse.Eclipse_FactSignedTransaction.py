# Databricks notebook source
int_dataMartLoadID         = 2021120301
str_destinationSubjectName = "MyMI_Snapshot_Eclipse"
str_destinationEntityName  = "Eclipse_FactSignedTransaction"
str_dataHistoryCode        = "C20211203"
str_destinationSystemID    = "DL_DWH_DimSourceSystem_ID"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED MyMI_Snapshot_Eclipse.Eclipse_FactSignedTransaction

# COMMAND ----------

str_sqlQuery = f"""
  EXPLAIN FORMATTED
  SELECT
    { int_dataMartLoadID } AS DataMartLoadID,
    '{str_destinationSubjectName}' AS Layer,
    '{str_destinationEntityName}' AS Entity,
    SourceSystemName SourceSystemName,
    '{str_dataHistoryCode}' DataHistoryCode,
    CAST(COUNT(*) AS DECIMAL(38, 4)) Total
  FROM
    { str_destinationSubjectName }.{ str_destinationEntityName } a
    INNER JOIN MyMI_Snapshot_Group.DWH_Dimsourcesystem c ON a.{ str_destinationSystemID } = c.DL_DWH_DimSourceSystem_ID
    INNER JOIN MyMI_Curated_Group.DWH_DimDataHistory d on a.DL_DWH_DimDataHistory_ID = d.DL_DWH_DimDataHistory_ID
  WHERE
    d.DataHistoryCode = '{ str_dataHistoryCode }'
  GROUP BY
    SourceSystemName
"""

spark.sql(str_sqlQuery).display()

# COMMAND ----------

#Define Filepath
str_filePath = f"dbfs:/mnt/main/Snapshot/{str_destinationSubjectName}/Internal/{str_destinationEntityName}/{str_destinationEntityName}_{str_dataHistoryCode}.parquet"

# Create Snapshot Dataframe
df_snapshot = spark.read.option("recursiveFileLookup","true").parquet(str_filePath)

# Create a local table for Snapshot Dataframe    
df_snapshot.createOrReplaceTempView("vw_snapshot")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM vw_snapshot