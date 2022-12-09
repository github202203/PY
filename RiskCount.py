# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT DISTINCT FK_DWH_DimCode
# MAGIC FROM mymi_trans_gxlp.FACTrISKCOUNT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT gxlpcONTRACT FROM (
# MAGIC SELECT COALESCE(CONTRACTREF,CONTRACTREF25)  AS gxlpcONTRACT
# MAGIC FROM mymi_gxlp210_testing.Stg70_ORIApplicablePremium
# MAGIC WHERE MATCHSTATUS <> 'MATCHED'
# MAGIC )A

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM mymi_gxlp210_testing.Stg70_ORIApplicablePremium
# MAGIC WHERE MATCHSTATUS <> 'MATCHED' AND (ContractRef= 'ZM351K20A001/R' OR ContractRef= 'ZM351K20A001/R')
# MAGIC order by ContractRef,ContractRef25
