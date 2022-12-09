# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT max(ORIMinimumPremiumSettCcy),min(ORIMinimumPremiumSettCcy)
# MAGIC FROM MYMI_ABST_GXLP.stg20_oriApplicablePremiumBase

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT HD.CONTRACT AS CONTRACT,HD.VERSION_NO AS VERSION_NO,HD1.VERSION_DATE AS ORIDateCreated 
# MAGIC            FROM(
# MAGIC            SELECT Contract,MIN(VERSION_NO) VERSION_NO
# MAGIC            FROM MyMI_Pre_GXLP.dbo_CTR_HDR
# MAGIC            GROUP BY Contract 
# MAGIC            )HD
# MAGIC            LEFT	JOIN	MyMI_Pre_GXLP.dbo_CTR_HDR HD1
# MAGIC            ON		HD.CONTRACT = HD1.CONTRACT
# MAGIC            AND		HD.VERSION_NO = HD1.VERSION_NO

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(ORIEstimatedPremiumSettCcy),min(ORIEstimatedPremiumSettCcy)
# MAGIC FROM MYMI_ABST_GXLP.stg20_oriApplicablePremiumBase
