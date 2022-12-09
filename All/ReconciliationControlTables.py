# Databricks notebook source
# MAGIC %md ## 1. Include Common

# COMMAND ----------

# MAGIC %run /Datalib/Common/Database/MasterDatabase

# COMMAND ----------

# MAGIC %md ## 2. Include Common

# COMMAND ----------

str_sqlQuery = f"""
  SELECT dl.* FROM [Reconciliation].[DestinationLoad] dl WHERE InsertDate = (SELECT MAX(InsertDate) FROM [Reconciliation].[DestinationLoad]) 
"""

df_reconciliationDataLoad = fn_getSqlDataset(str_sqlQuery)

df_reconciliationDataLoad.display()