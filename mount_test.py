# Databricks notebook source
dbutil.fs.ls("/mnt/backup")

# COMMAND ----------

dbutils.secrets.get(scope = "datalib-adb-scope-01", key = "datalib-backupstorage-name")

str_source = "wasbs://main@" + dbutils.secrets.get(scope = "datalib-adb-scope-01", key = "datalib-backupstorage-name") + ".blob.core.windows.net"

# COMMAND ----------

databricks secrets list-scopes

# COMMAND ----------

# MAGIC %run /Users/naga.reddy@britinsurance.com/GXLP_rec

# COMMAND ----------


