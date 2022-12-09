# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationMerge
# MAGIC * **Description :** Reconciliation Records - Level 500 - Source To PreTransformed.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 14/09/2021 | Raja Murugan | Reconciliation Records - Level 500 - Source To PreTransformed. |

# COMMAND ----------

# MAGIC %md # 1. Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# MAGIC %run /Datalib/Common/Library/MasterLibrary

# COMMAND ----------

# MAGIC %run /Datalib/Common/Logging/logging_pyspark

# COMMAND ----------

# MAGIC %run /Users/raja.murugan@britinsurance.com/ReconciliationRecords/fn_readEclipseTable

# COMMAND ----------

# MAGIC %md ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.dropdown("SourceSubjectName"     , "Eclipse"         , ["Adhoc", "Eclipse", "FinancialPeriod", "GXLP", "MDSDev", "MDSProd", "SequelClaims", "Velocity"])
dbutils.widgets.dropdown("DestinationSubjectName", "MyMI_Pre_Eclipse", ["MyMI_Pre_Crawford", "MyMI_Pre_Eclipse", "MyMI_Pre_FinancialPeriod", "MyMI_Pre_GXLP", "MyMI_Pre_MDS", "MyMI_Pre_MountainView", "MyMI_Pre_Others", "MyMI_Pre_SequelClaims", "MyMI_Pre_Velocity"])
dbutils.widgets.text("SourceEntityName"          , "BusinessCode"    , "SourceEntityName")
dbutils.widgets.text("DestinationEntityName"     , "dbo_BusinessCode", "DestinationEntityName")

# COMMAND ----------

# MAGIC %md ## 1.4 Set Variables

# COMMAND ----------

str_sourceSubjectName      = dbutils.widgets.get("SourceSubjectName")
str_sourceEntityName       = dbutils.widgets.get("SourceEntityName")
str_destinationSubjectName = dbutils.widgets.get("DestinationSubjectName")
str_destinationEntityName  = dbutils.widgets.get("DestinationEntityName")

# COMMAND ----------

# MAGIC %md # 2. Process

# COMMAND ----------

# MAGIC %md ## 2.1 Source Records

# COMMAND ----------

if str_sourceSubjectName == "Eclipse":
  df_sourceRecords = fn_readEclipseTable(str_sourceEntityName)
  
df_sourceRecords.createOrReplaceTempView("vw_source_records")

# COMMAND ----------

df_sourceRecords.display()

# COMMAND ----------

# MAGIC %md ## 2.1 Destination Records

# COMMAND ----------

str_sqlQuery = f"""
  SELECT
    *
  FROM
    { str_destinationSubjectName }.{ str_destinationEntityName }      
"""

df_destinationRecords = spark.sql(str_sqlQuery).drop("LoadDateTime", "DataMartLoadID").distinct()
df_destinationRecords.createOrReplaceTempView("vw_destination_records")

# COMMAND ----------

df_destinationRecords.display()

# COMMAND ----------

# MAGIC %md ## 2.1 Destination Minus Source Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_destination_records
# MAGIC MINUS
# MAGIC SELECT * FROM vw_source_records

# COMMAND ----------

# MAGIC %md ## 2.1 Source Minus Destination Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_source_records
# MAGIC MINUS
# MAGIC SELECT * FROM vw_destination_records