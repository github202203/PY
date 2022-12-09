# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationMerge
# MAGIC * **Description :** Reconciliation Records - Level 500 - Source To PreTransformed (Eclipse, GXLP).
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

# MAGIC %run /Datalib/Common/Master

# COMMAND ----------

# MAGIC %md ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.dropdown("SourceSubjectName"     , "Eclipse"          , ["Adhoc", "Eclipse", "FinancialPeriod", "GXLP", "MDSDev", "MDSProd", "SequelClaims", "Velocity"])
dbutils.widgets.dropdown("DestinationSubjectName", "MyMI_Pre_Eclipse" , ["MyMI_Pre_Crawford", "MyMI_Pre_Eclipse", "MyMI_Pre_FinancialPeriod", "MyMI_Pre_GXLP", "MyMI_Pre_MDS", "MyMI_Pre_MountainView", "MyMI_Pre_Others", "MyMI_Pre_SequelClaims", "MyMI_Pre_Velocity"])
dbutils.widgets.text("SourceEntityName"          , "BusinessCode"     , "SourceEntityName")
dbutils.widgets.text("DestinationEntityName"     , "dbo_BusinessCode" , "DestinationEntityName")

# COMMAND ----------

# MAGIC %md ## 1.4 Set Variables

# COMMAND ----------

str_sourceSubjectName      = dbutils.widgets.get("SourceSubjectName")
str_sourceEntityName       = dbutils.widgets.get("SourceEntityName")
str_destinationSubjectName = dbutils.widgets.get("DestinationSubjectName")
str_destinationEntityName  = dbutils.widgets.get("DestinationEntityName")

if str_sourceSubjectName == "Eclipse":
  str_sourceObject = f"MyMI_CopyEclipse.dbo.{ str_sourceEntityName }" 
elif str_sourceSubjectName == "GXLP":
  str_sourceObject = f"MyMI_ImportGXLP.dbo.{ str_sourceEntityName }"
  
print(str_sourceObject)
  
str_destinationObject  = f"{ str_destinationSubjectName }.{ str_destinationEntityName }"
print(str_destinationObject)

# COMMAND ----------

# MAGIC %md # 2. Process

# COMMAND ----------

# MAGIC %md ## 2.1 Source Records

# COMMAND ----------

str_sqlQuery = f"""
  (
  SELECT
    *
  FROM
    { str_sourceObject }
  ) a    
"""

df_sourceRecords = fn_readSQLDatabase(str_sqlQuery)
df_sourceRecords.createOrReplaceTempView("vw_source_records")

# COMMAND ----------

df_sourceRecords.display()

# COMMAND ----------

# MAGIC %md ## 2.2 Destination Records

# COMMAND ----------

df_destination        = spark.table(str_destinationObject)
df_destination.createOrReplaceTempView("vw_destination")
df_destinationRecords = df_destination.drop("LoadDateTime", "DataMartLoadID", "SYS_CHANGE_OPERATION").distinct()
df_destinationRecords.createOrReplaceTempView("vw_destination_records")

# COMMAND ----------

df_destinationRecords.display()

# COMMAND ----------

# MAGIC %md ## 2.3 Destination Minus Source Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) 
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT BusinessCodeID FROM vw_destination_records
# MAGIC   MINUS
# MAGIC   SELECT BusinessCodeID FROM vw_source_records
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT BusinessCodeID FROM vw_destination_records
# MAGIC MINUS
# MAGIC SELECT BusinessCodeID FROM vw_source_records

# COMMAND ----------

# MAGIC %md ## 2.4 Source Minus Destination Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) 
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT BusinessCodeID FROM vw_source_records
# MAGIC   MINUS
# MAGIC   SELECT BusinessCodeID FROM vw_destination_records
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT BusinessCodeID FROM vw_source_records
# MAGIC MINUS
# MAGIC SELECT BusinessCodeID FROM vw_destination_records

# COMMAND ----------

# MAGIC %md # 3. Post Process

# COMMAND ----------

# MAGIC %md ## 3.1 Check Specific Source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   vw_source_records
# MAGIC WHERE
# MAGIC   BusinessCodeID IN (118442, 111474) -- BusinessCode
# MAGIC --   CONTRACT IN ('ZM359V21B001') -- CTR_PCOMM

# COMMAND ----------

# MAGIC %md ## 3.1 Check Specific Destination

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   vw_destination
# MAGIC WHERE
# MAGIC   BusinessCodeID IN (118442, 111474) -- dbo_BusinessCode
# MAGIC --   CONTRACT IN ('ZM359V21B001') -- dbo_CTR_PCOMM