# Databricks notebook source
# MAGIC %md # Notebook Function
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** fn_reconciliationVelocity0500
# MAGIC * **Description :** Reconciliation Records - Level 500 - Source To PreTransformed (Velocity).
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 14/09/2021 | Raja Murugan | Reconciliation Records - Level 500 - Source To PreTransformed (Velocity). |

# COMMAND ----------

# MAGIC %md # 1. fn_reconciliationRecords0500

# COMMAND ----------

def fn_reconciliationVelocity0500(str_destinationSubjectName: str, str_destinationEntityName: str) -> DataFrame:
  
   # Set Variables  
  str_destinationObject  = f"{ str_destinationSubjectName }.{ str_destinationEntityName }"

  print(f"Destination: {str_destinationObject}")    
    
  # Destination Records    
  df_destinationRecords = spark.table(str_destinationObject)
  
  return df_destinationRecords