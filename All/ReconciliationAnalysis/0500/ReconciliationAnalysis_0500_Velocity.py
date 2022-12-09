# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationAnalysis_0500
# MAGIC * **Description :** Reconciliation Analysis - Level 500 - Source To PreTransformed.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 16/12/2021 | Raja Murugan | Reconciliation Analysis - Level 500 - Source To PreTransformed. |

# COMMAND ----------

# MAGIC %md # 1. Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# MAGIC %run /Datalib/Common/Library/MasterLibrary

# COMMAND ----------

# MAGIC %run /Datalib/Common/SQLDataAccess/MasterSQLDataAccess

# COMMAND ----------

# MAGIC %run /Users/raja.murugan@britinsurance.com/ReconciliationAnalysis/0500/fn_reconciliationRecords0500

# COMMAND ----------

# MAGIC %run /Datalib/Common/Reconciliations/MasterReconciliations

# COMMAND ----------

# MAGIC %md # 2. Velocity

# COMMAND ----------

# MAGIC %md ## 2.1 ARINV

# COMMAND ----------

df_destinationRecords_Velocity_ARINV = fn_readPreTranformed0500("MyMI_Pre_Velocity", "dbo_ARINV")

# COMMAND ----------

df_destinationRecords_Velocity_ARINV.display()

# COMMAND ----------

df_sourceRecords_Velocity_ARINV = fn_getReconciliationSourceValue("Velocity", "dbo_ARINV", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_ARINV.show()

# COMMAND ----------

# MAGIC %md ## 2.2 ARINVTRAN

# COMMAND ----------

df_sourceRecords_Velocity_ARINVTRAN = fn_getReconciliationSourceValue("Velocity", "dbo_ARINVTRAN", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_ARINVTRAN.show()

# COMMAND ----------

# MAGIC %md ## 2.3 APPENDPOLICY

# COMMAND ----------

df_sourceRecords_Velocity_APPENDPOLICY = fn_getReconciliationSourceValue("Velocity", "dbo_APPENDPOLICY", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_APPENDPOLICY.show()

# COMMAND ----------

# MAGIC %md ## 2.5 dbo_ARCUST

# COMMAND ----------

df_sourceRecords_Velocity_ARCUST = fn_getReconciliationSourceValue("Velocity", "dbo_ARCUST", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_ARCUST.show()

# COMMAND ----------

# MAGIC %md ## 2.5 dbo_CORRESPONDHEAD

# COMMAND ----------

df_sourceRecords_Velocity_CORRESPONDHEAD = fn_getReconciliationSourceValue("Velocity", "dbo_CORRESPONDHEAD", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_CORRESPONDHEAD.show()

# COMMAND ----------

# MAGIC %md ## 2.6 dbo_CORRESPONDLINE

# COMMAND ----------

df_sourceRecords_Velocity_CORRESPONDLINE = fn_getReconciliationSourceValue("Velocity", "dbo_CORRESPONDLINE", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_CORRESPONDLINE.show()

# COMMAND ----------

# MAGIC %md ## 2.7 dbo_DETAILATTDATA

# COMMAND ----------

df_sourceRecords_Velocity_DETAILATTDATA = fn_getReconciliationSourceValue("Velocity", "dbo_DETAILATTDATA", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_DETAILATTDATA.show()

# COMMAND ----------

# MAGIC %md ## 2.8 dbo_ISSUEHISTHEAD

# COMMAND ----------

df_sourceRecords_Velocity_ISSUEHISTHEAD = fn_getReconciliationSourceValue("Velocity", "dbo_ISSUEHISTHEAD", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_ISSUEHISTHEAD.show()

# COMMAND ----------

# MAGIC %md ## 2.9 dbo_policy

# COMMAND ----------

df_sourceRecords_Velocity_policy = fn_getReconciliationSourceValue("Velocity", "dbo_policy", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_policy.show()

# COMMAND ----------

# MAGIC %md ## 2.10 dbo_PolicyTran

# COMMAND ----------

df_sourceRecords_Velocity_PolicyTran = fn_getReconciliationSourceValue("Velocity", "dbo_PolicyTran", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_PolicyTran.show()

# COMMAND ----------

# MAGIC %md ## 2.11 dbo_Quote

# COMMAND ----------

df_sourceRecords_Velocity_Quote = fn_getReconciliationSourceValue("Velocity", "dbo_Quote", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_Quote.show()

# COMMAND ----------

# MAGIC %md ## 2.12 dbo_QUOTEPREM

# COMMAND ----------

df_sourceRecords_Velocity_QUOTEPREM = fn_getReconciliationSourceValue("Velocity", "dbo_QUOTEPREM", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_QUOTEPREM.show()

# COMMAND ----------

# MAGIC %md ## 2.13 dbo_SubmissionMast

# COMMAND ----------

df_sourceRecords_Velocity_SubmissionMast = fn_getReconciliationSourceValue("Velocity", "dbo_SubmissionMast", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_SubmissionMast.show()

# COMMAND ----------

# MAGIC %md ## 2.14 dbo_TIMEACTIVATE

# COMMAND ----------

df_sourceRecords_Velocity_TIMEACTIVATE = fn_getReconciliationSourceValue("Velocity", "dbo_TIMEACTIVATE", "20211217_0000", 'RecordCountPreTransFilter')
df_sourceRecords_Velocity_TIMEACTIVATE.show()