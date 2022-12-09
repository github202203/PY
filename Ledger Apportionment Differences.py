# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM MYMI_PRE_GXLP.DBO_LEDGER
# MAGIC WHERE contract = 'ZP000A03A001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM MYMI_PRE_GXLP25.DBO_LEDGER
# MAGIC WHERE contract = 'ZP000A03A001' 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_abst_gxlp.Stg10_LedgerApportionmentPercentage
# MAGIC WHERE contract = 'ZP000A03A001' AND AC_STAT2 = 'GC0076'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC WHERE contract = 'ZP000A03A001' AND AC_STAT2 = 'GC0076'
