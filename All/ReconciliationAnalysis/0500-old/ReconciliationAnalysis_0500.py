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

# MAGIC %run /Users/raja.murugan@britinsurance.com/ReconciliationAnalysis/0500/fn_reconciliationRecords0500

# COMMAND ----------

# MAGIC %md # 2. Eclipse

# COMMAND ----------

# MAGIC %md ## 2.1 BusinessCode

# COMMAND ----------

df_destinationMinusSource_EclipseBusinessCode, df_sourceMinusDestination_EclipseBusinessCode = fn_reconciliationRecords0500("Eclipse", "BusinessCode", "MyMI_Pre_Eclipse", "dbo_BusinessCode", "BusinessCodeID")

print(f"Destination - Source: {df_destinationMinusSource_EclipseBusinessCode.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipseBusinessCode.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipseBusinessCode.display()

# COMMAND ----------

df_sourceMinusDestination_EclipseBusinessCode.display()

# COMMAND ----------

# MAGIC %md ## 2.1 ReportingClass

# COMMAND ----------

df_destinationMinusSource_EclipseReportingClass, df_sourceMinusDestination_EclipseReportingClass = fn_reconciliationRecords0500("Eclipse", "ReportingClass", "MyMI_Pre_Eclipse", "dbo_ReportingClass", "ReportingClassId")

print(f"Destination - Source: {df_destinationMinusSource_EclipseReportingClass.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipseReportingClass.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipseReportingClass.display()

# COMMAND ----------

df_sourceMinusDestination_EclipseReportingClass.display()