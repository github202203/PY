# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC  Contract
# MAGIC  ,Currency
# MAGIC  ,Tran_Type
# MAGIC  ,PM_Type
# MAGIC  ,Event
# MAGIC  ,Sub_event
# MAGIC  ,Entity_CD,COUNT(*) FROM (
# MAGIC SELECT DISTINCT 
# MAGIC   COALESCE(L.CONTRACT,'')  Contract
# MAGIC ,COALESCE(L.CURRENCY,'')   Currency
# MAGIC ,COALESCE(L.TRAN_TYPE,'')  Tran_Type
# MAGIC ,COALESCE(L.PM_TYPE,'')    PM_Type
# MAGIC ,COALESCE(L.EVENT,'')      Event
# MAGIC ,COALESCE(L.SUB_EVENT,'')  Sub_event
# MAGIC ,COALESCE(L.ENTITY_CD,'')  Entity_CD
# MAGIC FROM mymi_pre_gxlp.DBO_LEDGER L
# MAGIC )A
# MAGIC GROUP BY 
# MAGIC  Contract
# MAGIC  ,Currency
# MAGIC  ,Tran_Type
# MAGIC  ,PM_Type
# MAGIC  ,Event
# MAGIC  ,Sub_event
# MAGIC  ,Entity_CD
# MAGIC  HAVING COUNT(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_trans_gxlp.DimORIPlacement

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT   COALESCE(L.CONTRACT,'')  Contract
# MAGIC ,COALESCE(L.CURRENCY,'')   Currency
# MAGIC ,COALESCE(L.TRAN_TYPE,'')  Tran_Type
# MAGIC ,COALESCE(L.PM_TYPE,'')    PM_Type
# MAGIC ,COALESCE(L.EVENT,'')      Event
# MAGIC ,COALESCE(L.SUB_EVENT,'')  Sub_event
# MAGIC ,COALESCE(L.ENTITY_CD,'')  Entity_CD
# MAGIC from mymi_abst_gxlp.Stg10_LedgerApportionment l

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CONCAT ('First_DL_DWH_DimSourceSystem_ID=14;GXLPBrokerCode=',BrokerCode,';GXLPContractReference=',ContractRef,';GXLPReinsurerCode=',ReinsurerCode)  AS BusinessKey
# MAGIC  , 'MDSSourceSystemCode=14' AS  FK_DWH_DimSourceSystem
# MAGIC  , CONCAT('BRMBrokerCode=Unknown;BRMBrokerPseudonym=Unknown;GXLPBrokerCode=',BrokerCode,';ORIBrokerReference=',BrokerRef,';VelocityBrokerNumber=0;VelocityLevel1Org=0') AS  `FK_DWH_DimBroker:DWH_DimORIBroker`    
# MAGIC  , CONCAT('GXLPContractReference=',ContractRef) AS  FK_DWH_DimORIContract
# MAGIC  , CONCAT('GXLPReinsurerCode=',ReinsurerCode) AS  FK_DWH_DimORIReinsurer
# MAGIC  , BrokerCode AS GXLPBrokerCode
# MAGIC  , ContractRef AS GXLPContractReference
# MAGIC  , ReinsurerCode AS GXLPReinsurerCode
# MAGIC  , CASE WHEN CommutedFlag='Y' THEN true ELSE false END AS ORICommutedFlag
# MAGIC  , CAST(ORIReinsurerPctOfOrder AS decimal(10,7)) AS ORIReinsurerPctOfOrder
# MAGIC  , CAST(ORIReinsurerPctOfWhole AS decimal(10,7)) AS ORIReinsurerPctOfWhole
# MAGIC  , CAST(ORIBrokerPct AS decimal(10,7)) AS ORIBrokerPct
# MAGIC FROM MyMI_Abst_GXLP.Stg10_LedgerApportionment

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),period from mymi_pre_gxlp.dbo_accrual
# MAGIC group by period 4,460,547
