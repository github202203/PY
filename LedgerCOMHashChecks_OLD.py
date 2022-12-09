# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT A.CURRENCY,Allocated_AMT_CURR,AMT_CURR
# MAGIC FROM (
# MAGIC       SELECT CURRENCY,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment
# MAGIC       group by CURRENCY
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT CURRENCY,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by CURRENCY
# MAGIC       )B
# MAGIC       ON A.CURRENCY = B.CURRENCY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.Contract,Round(Allocated_AMT_CURR,4) Allocated_AMT_CURR,round(AMT_CURR,4) AMT_CURR
# MAGIC FROM (
# MAGIC       SELECT Contract,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment
# MAGIC       group by Contract
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Contract,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Contract
# MAGIC       )B
# MAGIC       ON A.Contract = B.Contract

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.Period,Round(Allocated_AMT_CURR,4) Allocated_AMT_CURR,round(AMT_CURR,4) AMT_CURR
# MAGIC FROM (
# MAGIC       SELECT Period,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment
# MAGIC       group by Period
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Period,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Period
# MAGIC       )B
# MAGIC       ON A.Period = B.Period

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.EVENT,Round(Allocated_AMT_CURR,4) Allocated_AMT_CURR,round(AMT_CURR,4) AMT_CURR
# MAGIC FROM (
# MAGIC       SELECT EVENT,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment
# MAGIC       group by EVENT
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT EVENT,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by EVENT
# MAGIC       )B
# MAGIC       ON A.EVENT = B.EVENT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.SUB_EVENT,Round(Allocated_AMT_CURR,4) Allocated_AMT_CURR,round(AMT_CURR,4) AMT_CURR
# MAGIC FROM (
# MAGIC       SELECT SUB_EVENT,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment
# MAGIC       group by SUB_EVENT
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT SUB_EVENT,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by SUB_EVENT
# MAGIC       )B
# MAGIC       ON A.SUB_EVENT = B.SUB_EVENT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.REIN_CD,Round(Allocated_AMT_CURR,4) Allocated_AMT_CURR,round(AMT_CURR,4) AMT_CURR
# MAGIC FROM (
# MAGIC       SELECT REIN_CD,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment
# MAGIC       group by REIN_CD
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT REIN_CD,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by REIN_CD
# MAGIC       )B
# MAGIC       ON A.REIN_CD = B.REIN_CD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.BROKER_CD,Round(Allocated_AMT_CURR,4) Allocated_AMT_CURR,round(AMT_CURR,4) AMT_CURR
# MAGIC FROM (
# MAGIC       SELECT BROKER_CD,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment
# MAGIC       group by BROKER_CD
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT BROKER_CD,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by BROKER_CD
# MAGIC       )B
# MAGIC       ON A.BROKER_CD = B.BROKER_CD
