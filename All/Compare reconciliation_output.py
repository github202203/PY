# Databricks notebook source
# MAGIC %md ## Compare reconciliation_output (Current vs Previous)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DestinationID,
# MAGIC   DestinationName,
# MAGIC   LevelID,
# MAGIC   LevelName,
# MAGIC   DataLoadCode,
# MAGIC   FileDateTime,
# MAGIC   DataMartLoadID,
# MAGIC   DataHistoryCode,
# MAGIC --   ReconciliationDateTime,
# MAGIC   JobRunID,
# MAGIC --   PipelineRunID,
# MAGIC   ControlID,
# MAGIC   DataMartID,
# MAGIC   DataMartName,
# MAGIC   SourceSubjectID,
# MAGIC   SourceSubjectName,
# MAGIC   SourceEntityID,
# MAGIC   SourceEntityName,
# MAGIC   SourceSystemName,
# MAGIC   DestinationSubjectID,
# MAGIC   DestinationSubjectName,
# MAGIC   DestinationEntityID,
# MAGIC   DestinationEntityName,
# MAGIC   ReconciliationType,
# MAGIC   ReconciliationColumn,
# MAGIC   CurrencyType,
# MAGIC   CurrencyCode,
# MAGIC   SourceValue,
# MAGIC   DestinationValue,
# MAGIC   Variance,
# MAGIC   IsToleranceRatio,
# MAGIC   ToleranceSetGreen,
# MAGIC   ToleranceSetAmber,
# MAGIC   ToleranceActual,
# MAGIC   ToleranceFlag
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE
# MAGIC   DestinationName = 'MyMI'
# MAGIC   AND LevelID = 500
# MAGIC   AND DataLoadCode = '2021120701' 
# MAGIC MINUS
# MAGIC SELECT
# MAGIC   DestinationID,
# MAGIC   DestinationName,
# MAGIC   LevelID,
# MAGIC   LevelName,
# MAGIC   DataLoadCode,
# MAGIC   FileDateTime,
# MAGIC   DataMartLoadID,
# MAGIC   DataHistoryCode,
# MAGIC --   ReconciliationDateTime,
# MAGIC   JobRunID,
# MAGIC --   PipelineRunID,
# MAGIC   ControlID,
# MAGIC   DataMartID,
# MAGIC   DataMartName,
# MAGIC   SourceSubjectID,
# MAGIC   SourceSubjectName,
# MAGIC   SourceEntityID,
# MAGIC   SourceEntityName,
# MAGIC   SourceSystemName,
# MAGIC   DestinationSubjectID,
# MAGIC   DestinationSubjectName,
# MAGIC   DestinationEntityID,
# MAGIC   DestinationEntityName,
# MAGIC   ReconciliationType,
# MAGIC   ReconciliationColumn,
# MAGIC   CurrencyType,
# MAGIC   CurrencyCode,
# MAGIC   SourceValue,
# MAGIC   DestinationValue,
# MAGIC   Variance,
# MAGIC   IsToleranceRatio,
# MAGIC   ToleranceSetGreen,
# MAGIC   ToleranceSetAmber,
# MAGIC   ToleranceActual,
# MAGIC   ToleranceFlag
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output VERSION AS OF 147
# MAGIC WHERE
# MAGIC   DestinationName = 'MyMI'
# MAGIC   AND LevelID = 500
# MAGIC   AND DataLoadCode = '2021120701'