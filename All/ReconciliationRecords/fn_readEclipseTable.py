# Databricks notebook source
# MAGIC %md # Notebook Function
# MAGIC 
# MAGIC ####  
# MAGIC * **Title   :** fn_readEclipseTable
# MAGIC * **Description :** Read Eclipse table.
# MAGIC * **Language :** PySpark
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 20/09/2021 | Raja Murugan | Read Eclipse table |

# COMMAND ----------

# MAGIC %md # 1. Read Eclipse Table

# COMMAND ----------

def fn_readEclipseTable(str_tableName: str) -> DataFrame:
  try:
    str_url      = "jdbc:sqlserver://ECL-06-LGCY-PRD:1433;database=BritEclipseLive;"
    str_username = "datalakereaderprod"
    str_password = dbutils.secrets.get(scope = "datalib-adb-scope-01", key = "datalib-asql-eclipse-password")

    df_eclipse = spark.read \
                      .format("com.microsoft.sqlserver.jdbc.spark") \
                      .option("url"     , str_url) \
                      .option("dbtable" , str_tableName) \
                      .option("user"    , str_username) \
                      .option("password", str_password).load()    

    return df_eclipse

  except Exception as e:
    log_status(LogLevel.ERROR, f"Failed to read Eclipse table {str_tableName}, error message:", e)
    raise(e)