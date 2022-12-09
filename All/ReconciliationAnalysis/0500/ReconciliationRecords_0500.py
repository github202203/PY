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
dbutils.widgets.text("UniqueKeyColumns"          , "BusinessCodeID"   , "UniqueKeyColumns")

# COMMAND ----------

# MAGIC %md ## 1.4 Set Variables

# COMMAND ----------

str_sourceSubjectName      = dbutils.widgets.get("SourceSubjectName")
str_sourceEntityName       = dbutils.widgets.get("SourceEntityName")
str_destinationSubjectName = dbutils.widgets.get("DestinationSubjectName")
str_destinationEntityName  = dbutils.widgets.get("DestinationEntityName")
str_uniqueKeyColumns       = dbutils.widgets.get("UniqueKeyColumns")

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

# MAGIC %md ## 2.2 Destination Records

# COMMAND ----------

df_destinationRecords = spark.table(str_destinationObject)
df_destinationRecords.createOrReplaceTempView("vw_destination_records")

# COMMAND ----------

# MAGIC %md ## 2.3 Destination Minus Source Records

# COMMAND ----------

str_sqlQuery = f"""  
  SELECT *
  FROM vw_destination_records D1
  RIGHT JOIN (
        SELECT concat({ str_uniqueKeyColumns }) UniqueKeyValues FROM vw_destination_records
        MINUS
        SELECT concat({ str_uniqueKeyColumns }) UniqueKeyValues FROM vw_source_records
      ) D2
  ON concat({ str_uniqueKeyColumns }) = D2.UniqueKeyValues
"""

df_destinationMinusSource = spark.sql(str_sqlQuery).drop("UniqueKeyValues")

# COMMAND ----------

df_destinationMinusSource.count()

# COMMAND ----------

df_destinationMinusSource.display()

# COMMAND ----------

# MAGIC %md ## 2.3 Source Minus Destination Records

# COMMAND ----------

str_sqlQuery = f"""  
  SELECT *
  FROM vw_source_records D1
  RIGHT JOIN (
        SELECT concat({ str_uniqueKeyColumns }) UniqueKeyValues FROM vw_source_records
        MINUS
        SELECT concat({ str_uniqueKeyColumns }) UniqueKeyValues FROM vw_destination_records
      ) D2
  ON concat({ str_uniqueKeyColumns }) = D2.UniqueKeyValues
"""

df_sourceMinusDestination = spark.sql(str_sqlQuery).drop("UniqueKeyValues")

# COMMAND ----------

df_sourceMinusDestination.count()

# COMMAND ----------

df_sourceMinusDestination.display()