# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED MyMI_Snapshot_Group.Eclipse_DimClaimExt

# COMMAND ----------

int_dataMartLoadID    = 2021120301
str_sourceSubjectName = "MyMI_Snapshot_Group"
str_sourceEntityName  = "DWH_FactClaimTransaction"
str_dataHistoryCode   = "F20211101"
str_sourceSystemID    = "DL_DWH_DimSourceSystem_ID"

# COMMAND ----------

str_sqlQuery = f"""
  EXPLAIN FORMATTED
  SELECT
    { int_dataMartLoadID } AS DataMartLoadID,
    '{str_sourceSubjectName}' AS Layer,
    '{str_sourceEntityName}' AS Entity,
    c.SourceSystemName SourceSystemName,
    '{str_dataHistoryCode}' DataHistoryCode,
    cast(count(1) as decimal(38, 4)) Total
  FROM
    { str_sourceSubjectName }.{ str_sourceEntityName } a
    INNER JOIN MyMI_Snapshot_Group.dwh_dimsourcesystem c on a.{ str_sourceSystemID } = c.DL_DWH_DimSourceSystem_ID
    INNER JOIN MyMI_Snapshot_Group.DWH_DimDataHistory d on a.DL_DWH_DimDataHistory_ID = d.DL_DWH_DimDataHistory_ID
  WHERE
    d.DataHistoryCode = '{ str_dataHistoryCode }'
    AND c.SourceSystemName NOT LIKE '%Adjustment%'
  GROUP BY
    c.SourceSystemName    
"""

spark.sql(str_sqlQuery).display()