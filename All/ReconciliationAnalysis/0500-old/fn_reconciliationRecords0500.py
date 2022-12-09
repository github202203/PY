# Databricks notebook source
# MAGIC %md # Notebook Function
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** fn_reconciliationRecords_0500
# MAGIC * **Description :** Reconciliation Records - Level 500 - Source To PreTransformed (Eclipse, GXLP).
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 14/09/2021 | Raja Murugan | Reconciliation Records - Level 500 - Source To PreTransformed (Eclipse, GXLP). |

# COMMAND ----------

# MAGIC %md # 1. fn_reconciliationRecords0500

# COMMAND ----------

def fn_reconciliationRecords0500(str_sourceSubjectName: str, str_sourceEntityName: str, str_destinationSubjectName: str, str_destinationEntityName: str, str_uniqueKeyColumns: str) -> DataFrame:
  
   # Set Variables
  if str_sourceSubjectName == "Eclipse":
    str_sourceObject = f"MyMI_CopyEclipse.dbo.{ str_sourceEntityName }" 
  elif str_sourceSubjectName == "GXLP":
    str_sourceObject = f"MyMI_ImportGXLP.dbo.{ str_sourceEntityName }"
    
  print(f"Source     : {str_sourceObject}")
  
  str_destinationObject  = f"{ str_destinationSubjectName }.{ str_destinationEntityName }"

  print(f"Destination: {str_destinationObject}")  
    
  # Source Records
  df_sourceRecords = fn_readSQLDatabase(f"(SELECT * FROM {str_sourceObject}) a")
  df_sourceRecords.createOrReplaceTempView("vw_source_records")
   
  # Destination Records    
  df_destinationRecords = spark.table(str_destinationObject)
  df_destinationRecords.createOrReplaceTempView("vw_destination_records")
  
  #Destination Minus Source Records  
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
  
  return df_destinationMinusSource, df_sourceMinusDestination