# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationDataLoad
# MAGIC * **Description :** Write Reconciliation Data Load to Delta Lake Table.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 10/11/2021 | Raja Murugan | Write Reconciliation Data Load to Delta Lake Table. |

# COMMAND ----------

# MAGIC %md # 1. Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# MAGIC %run /Datalib/Common/Master

# COMMAND ----------

# MAGIC %md ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("DebugRun", "True" , "DebugRun")

# COMMAND ----------

# MAGIC %md ## 1.4 Set Variables

# COMMAND ----------

bol_debugRun = eval(dbutils.widgets.get("DebugRun"))

# COMMAND ----------

# MAGIC %md # 2. Process

# COMMAND ----------

# MAGIC %md ## 2.1 Read Reconciliation Data Load

# COMMAND ----------

str_sqlQuery = f"""
  SELECT DISTINCT dn.DestinationName,
      dl.DataLoadCode
  FROM [Reconciliation].[ReportingLoad] rl
  JOIN [Reconciliation].[DestinationLoad]  dl ON rl.DestinationLoadID  = dl.DestinationLoadID
  JOIN [Reconciliation].[Destination]      dn ON dl.DestinationID      = dn.DestinationID
  WHERE 1 IN (rl.IsRetention, rl.IsRequired)  
"""

df_reconciliationDataLoad = fn_getSqlDataset(str_sqlQuery)

# COMMAND ----------

# MAGIC %md ## 2.2 Check Reconciliation Data Load

# COMMAND ----------

if bol_debugRun:
  df_reconciliationDataLoad.display()

# COMMAND ----------

# MAGIC %md ## 2.3 Write Reconciliation Data Load

# COMMAND ----------

df_reconciliationDataLoad.write.format("delta").mode("overwrite").saveAsTable("reconciliation.reconciliation_dataload")

# COMMAND ----------

# MAGIC %md ## 2.4 Check Reconciliation Data Load

# COMMAND ----------

if bol_debugRun:  
  spark.sql("SELECT * FROM reconciliation.reconciliation_dataload").display()