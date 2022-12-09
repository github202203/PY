# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationMerge
# MAGIC * **Description :** Merge Reconciliation Output Data with Delta Lake Table.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 14/09/2021 | Raja Murugan | Merge Reconciliation Output Data with Delta Lake Table. |

# COMMAND ----------

# MAGIC %md # 1. Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# MAGIC %run ../../Datalib/Common/Master

# COMMAND ----------

# MAGIC %md ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("DebugRun"        , "True" , "DebugRun")
dbutils.widgets.text("DestinationName" , "MyMI" , "DestinationName")
dbutils.widgets.text("LevelID"         , "0500" , "LevelID")

# COMMAND ----------

# MAGIC %md ## 1.4 Set Variables

# COMMAND ----------

bol_debugRun           = eval(dbutils.widgets.get("DebugRun"))
str_destinationName    = dbutils.widgets.get("DestinationName")
int_levelID            = int(dbutils.widgets.get("LevelID"))
str_reconciliationPath = "dbfs:/mnt/reconciliation"
str_filePath           = f"{str_reconciliationPath}/datalake/temp/{str_destinationName}/{int_levelID}"

# COMMAND ----------

# MAGIC %md # 2. Process

# COMMAND ----------

# MAGIC %md ## 2.1 Create Reconciliation Output

# COMMAND ----------

# Create Reconciliation Output Dataframe
df_reconciliationOutput = spark.read.option("recursiveFileLookup","true").parquet(str_filePath)

# Create a local table for Reconciliation Output Dataframe    
df_reconciliationOutput.createOrReplaceTempView("vw_reconciliationOutput")

# COMMAND ----------

# MAGIC %md ## 2.2 Check Reconciliation Output

# COMMAND ----------

if bol_debugRun: 
  df_reconciliationOutput.display()

# COMMAND ----------

# MAGIC %md ## 2.3 Merge Reconciliation Output

# COMMAND ----------

# Merge Reconciliation Output with Delta Lake Table    
try:          
  spark.sql(f"""
    MERGE INTO reconciliation.reconciliation_output t
    USING vw_reconciliationOutput s
    ON t.DestinationName         = s.DestinationName
      AND t.DataLoadCode         = s.DataLoadCode
      AND t.LevelID              = s.LevelID
      AND t.ControlID            = s.ControlID
      AND t.SourceSystemName     = s.SourceSystemName
      AND t.ReconciliationColumn = s.ReconciliationColumn
      AND t.CurrencyType         = s.CurrencyType
      AND t.CurrencyCode         = s.CurrencyCode
      AND t.DestinationName      = '{ str_destinationName }'
      AND t.LevelID              = { int_levelID }	        
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *  
  """)     

except Exception as e:
  log_status(LogLevel.ERROR, "Failed to merge Reconciliation Output, error message:", e)
  raise(e) 