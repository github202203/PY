# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationMerge
# MAGIC * **Description :** Compare Reconciliation Output Data in Delta Lake Table.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 14/09/2021 | Raja Murugan | Compare Reconciliation Output Data in Delta Lake Table. |

# COMMAND ----------

# MAGIC %md # 1. Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# %run ../../Datalib/Common/Master

# COMMAND ----------

# MAGIC %md ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("DestinationName" , "MyMI"       , "DestinationName")
dbutils.widgets.text("LevelID"         , "0500"       , "LevelID")
dbutils.widgets.text("DataLoadCode"    , "2021120701" , "DataLoadCode")
dbutils.widgets.text("Version"         , "147"        , "Version")

# COMMAND ----------

# MAGIC %md ## 1.4 Set Variables

# COMMAND ----------

str_destinationName    = dbutils.widgets.get("DestinationName")
int_levelID            = int(dbutils.widgets.get("LevelID"))
str_dataLoadCode       = dbutils.widgets.get("DataLoadCode")
int_version            = int(dbutils.widgets.get("Version"))

# COMMAND ----------

# MAGIC %md # 2. Process

# COMMAND ----------

# MAGIC %md ## 2.1 Compare Reconciliation Output

# COMMAND ----------

# Merge Reconciliation Output with Delta Lake Table    
try:          
  spark.sql(f"""
    SELECT
      DestinationID,
      DestinationName,
      LevelID,
      LevelName,
      DataLoadCode,
      FileDateTime,
      DataMartLoadID,
      DataHistoryCode,
--       ReconciliationDateTime,
      JobRunID,
--       PipelineRunID,
      ControlID,
      DataMartID,
      DataMartName,
      SourceSubjectID,
      SourceSubjectName,
      SourceEntityID,
      SourceEntityName,
      SourceSystemName,
      DestinationSubjectID,
      DestinationSubjectName,
      DestinationEntityID,
      DestinationEntityName,
      ReconciliationType,
      ReconciliationColumn,
      CurrencyType,
      CurrencyCode,
      SourceValue,
      DestinationValue,
      Variance,
      IsToleranceRatio,
      ToleranceSetGreen,
      ToleranceSetAmber,
      ToleranceActual,
      ToleranceFlag
    FROM
      reconciliation.reconciliation_output
    WHERE
      DestinationName = '{ str_destinationName }'
      AND LevelID = { int_levelID }
      AND DataLoadCode = '{ str_dataLoadCode }'
    MINUS
    SELECT
      DestinationID,
      DestinationName,
      LevelID,
      LevelName,
      DataLoadCode,
      FileDateTime,
      DataMartLoadID,
      DataHistoryCode,
--       ReconciliationDateTime,
      JobRunID,
--       PipelineRunID,
      ControlID,
      DataMartID,
      DataMartName,
      SourceSubjectID,
      SourceSubjectName,
      SourceEntityID,
      SourceEntityName,
      SourceSystemName,
      DestinationSubjectID,
      DestinationSubjectName,
      DestinationEntityID,
      DestinationEntityName,
      ReconciliationType,
      ReconciliationColumn,
      CurrencyType,
      CurrencyCode,
      SourceValue,
      DestinationValue,
      Variance,
      IsToleranceRatio,
      ToleranceSetGreen,
      ToleranceSetAmber,
      ToleranceActual,
      ToleranceFlag
    FROM
      reconciliation.reconciliation_output VERSION AS OF 147
    WHERE
      DestinationName = '{ str_destinationName }'
      AND LevelID = { int_levelID }
      AND DataLoadCode = '{ str_dataLoadCode }' 
  """).display()

except Exception as e:
  log_status(LogLevel.ERROR, "Failed to compare Reconciliation Output, error message:", e)
  raise(e) 