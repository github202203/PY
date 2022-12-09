# Databricks notebook source
# arr_databases = [db.name for db in spark.catalog.listDatabases()]

# for db in arr_databases:

#     print(db)
    
#     try:
#         arr_tables = [t.name for t in spark.catalog.listTables(db)]
#     except:
#         arr_tables = []
    
#     for t in arr_tables:
#         print("VACUUM " + db + "." + t + " RETAIN x HOURS")
    
#     print("")

# COMMAND ----------

# def fn_getDirContent(str_path):
#     """
#     Return list of directories and files based on input str_path 
#     """
#     arr_dirPaths = dbutils.fs.ls(str_path)
#     arr_subDirPaths = [fn_getDirContent(p.path) for p in arr_dirPaths if p.isDir() and p.path != str_path]
#     arr_flatSubDirPaths = [p for subdir in arr_subDirPaths for p in subdir]
    
#     return list(map(lambda p: p.path, arr_dirPaths)) + arr_flatSubDirPaths
    

# arr_paths = fn_getDirContent("/mnt/")

# arr_deltaPaths = [p.replace("_delta_log/", "") for p in arr_paths if bool(re.match(".*_delta_log/$", p))]

# print(arr_deltaPaths)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE MDMBronze

# COMMAND ----------

# MAGIC %sql set spark.databricks.delta.alterLocation.bypassSchemaCheck = true

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE EclipseSilver.dbo_Policy
# MAGIC SET LOCATION '/mnt/silver/underwriting/Internal/Eclipse/DeltaLake/dbo_Policy'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT BronzeStagingSystemLoadID FROM EclipseBronze.dbo_Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL EclipseSilver.dbo_PolicyLimit

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY '/mnt/bronze/masterdata/Internal/MDS/DeltaLake/mdm_CRSCauseCode_Development'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY EclipseBronze.dbo_Policy

# COMMAND ----------

country = spark.read.format("delta").load("/mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_Country")

# COMMAND ----------

from pyspark.sql.functions import count, col
