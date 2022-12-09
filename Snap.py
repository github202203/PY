# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from SIT.dwh_dimbroker DL_DWH_DimOriTransaction_Id25

# COMMAND ----------

spark.read.table('SIT1.dwh_factoritransactiondatahistory').count()

# COMMAND ----------

spark.read.table('SIT1.dwh_dimriskcount').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop database sit;
# MAGIC --drop table sit.dwh_dimoritransaction

# COMMAND ----------

spark.read.table('SIT1.dwh_dimoriplacement').display()

# COMMAND ----------

spark.read.table('SIT1.dwh_dimcode').display()

# COMMAND ----------


