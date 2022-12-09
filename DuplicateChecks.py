# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT count(*),businesskey
# MAGIC FROM mymi_trans_gxlp.DimORIContract
# MAGIC group by businesskey

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT 
# MAGIC    CONCAT ('GXLPContractReference=',GXLPContractReference)  AS BusinessKey
# MAGIC  , CONCAT ('MDSSourceSystemCode=',14) AS  FK_DWH_DimSourceSystem
# MAGIC  , CONCAT ('MDSCoverageBasis=',CoverageBasis) AS  FK_DWH_DimCoverageBasis
# MAGIC  , CONCAT ('GroupNewRenewal=',-1) AS  FK_DWH_DimNewRenewal
# MAGIC  , CONCAT ('MDSTerritory=',-1) AS  FK_DWH_DimTerritory
# MAGIC  , CONCAT ('ORIContractStatus=',ORIContractStatus) AS  FK_DWH_DimORIContractStatus
# MAGIC  , CONCAT ('GXLPORIContractType=',ContractType) AS  FK_DWH_DimORIContractType
# MAGIC  , CONCAT ('GXLPProgrammeCode=',ProgrammeCode) AS  FK_DWH_DimORIProgramme
# MAGIC  , GXLPContractReference
# MAGIC  , ORIContractDescription
# MAGIC  , ORIAdjustablePct
# MAGIC  , ORIAggregateDeductible
# MAGIC  , ORIAggregateLimit
# MAGIC  , ORIContractCurrency
# MAGIC  , ORIContractYear
# MAGIC  , ORIExcess100PctContractCcy
# MAGIC  , ORIExpiryDate
# MAGIC  , ORIInceptionDate
# MAGIC  , CAST(0 AS decimal(21,12)) AS ORIFixedROE
# MAGIC  , ORILimit100PctContractCcy
# MAGIC  , ORIManualContractFlag
# MAGIC  , ORIOrderPct
# MAGIC  , ORIPlacedPct
# MAGIC  , ORIProfitCommissionDate
# MAGIC  , ORIRateOnLinePct
# MAGIC  , ORIReinstatementNumber
# MAGIC  , ORIReinstatementTerm
# MAGIC  , ORIReinsurerExpensePct
# MAGIC  , OverriderCommissionPct
# MAGIC  , ORIWholeAccountQSFlag
# MAGIC  , ORIDateCreated
# MAGIC  , ORIMasterContractReference 
# MAGIC FROM MyMI_Abst_GXLP.Stg60_ORIContract
# MAGIC --WHERE ORIDateCreated IS  NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC   FROM MyMI_Pre_GXLP.dbo_CTR_HDR

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Contract,VERSION_DATE AS ORIDateCreated,version_no
# MAGIC   FROM MyMI_Pre_GXLP.dbo_CTR_HDR
# MAGIC   WHERE  CONTRACT in('ZA202G16A001/R','ZA301C16A001/R') --AND VERSION_NO=1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT c.CONTRACT AS ContractRef
# MAGIC   , CASE
# MAGIC       WHEN instr(c.CONTRACT, '/') = 0 THEN c.CONTRACT
# MAGIC       ELSE LEFT(c.CONTRACT, instr(c.CONTRACT, '/')-1)
# MAGIC     END AS ORIMasterContractReference
# MAGIC   , c.VERSION_NO AS VersionNo
# MAGIC   , COALESCE(VD.ORIDateCreated,'2000-01-01') ORIDateCreated --Revert back once Data is fixed
# MAGIC   , CASE 
# MAGIC       WHEN ISNULL(hx.START_DATE) AND hx.LOD_BASIS = 'D' THEN 'LOD'
# MAGIC       WHEN ISNULL(hx.START_DATE) AND hx.LOD_BASIS = 'C' THEN 'CMD'
# MAGIC       WHEN ISNOTNULL(hx.START_DATE) THEN 'RAD'
# MAGIC       WHEN ISNOTNULL(hp.INC_DATE) THEN 'RAD'
# MAGIC     END AS CoverageBasis
# MAGIC   , '' AS NewRenewal --Not Confirmed (note from on-premise version)
# MAGIC   , PCD.C_TYPE AS ContractType
# MAGIC   ,c.Prog_CD /*COALESCE(hx.PROG_CD, hp.PROP_CD)*/ AS ProgrammeCode
# MAGIC   ,coalesce(a.ADJ_RATE,0) AS AdjustablePct
# MAGIC   , hx.AG_DE_CCY AS AggregateDeductible
# MAGIC   , CASE
# MAGIC       WHEN C_TYPE IN ( 'CO', 'MS' ) THEN 0
# MAGIC       ELSE CASE
# MAGIC         WHEN ISNOTNULL(al.AGG_LIMIT) THEN al.AGG_LIMIT
# MAGIC         ELSE CASE
# MAGIC           WHEN ISNULL(al.AGG_LIMIT)
# MAGIC           THEN CASE
# MAGIC             WHEN ISNOTNULL(hx.AG_LI_CCY) THEN hx.AG_LI_CCY
# MAGIC             ELSE COALESCE(hp.LIM_CCY, hx.LIM_CCY) + (COALESCE(hp.LIM_CCY, hx.LIM_CCY) * COALESCE(reinst.ReinstatementNumber, 0)) 
# MAGIC           END
# MAGIC           ELSE COALESCE(hp.LIM_CCY, hx.LIM_CCY) + ( COALESCE(hp.LIM_CCY, hx.LIM_CCY) * COALESCE(reinst.ReinstatementNumber, 0)) END
# MAGIC         END
# MAGIC       END AS AggregateLimit
# MAGIC   , hx.CURRENCY AS ContractCurrency
# MAGIC   , c._DESC AS ContractDescription
# MAGIC   , c.PROT_YEAR AS ContractYear
# MAGIC   , CASE WHEN PCD.C_TYPE IN ( 'CO', 'MS' ) THEN 0 ELSE hx.EXS_CCY END AS Excess100PctContractCcy
# MAGIC   , COALESCE(hp.EXP_DATE, hx.EXP_DATE, cm.EXP_DATE, hx.END_DATE) AS ExpiryDate
# MAGIC   , COALESCE(hp.INC_DATE, hx.INC_DATE, cm.INC_DATE, hx.START_DATE) AS InceptionDate
# MAGIC   , CASE WHEN PCD.C_TYPE IN ( 'CO', 'MS' ) THEN 0 ELSE COALESCE(hp.LIM_CCY, hx.LIM_CCY) END AS Limit100PctContractCcy
# MAGIC   , false AS ManualContractFlag
# MAGIC   , 0 AS OrderPct 
# MAGIC   , c.PC_PLACED AS PlacedPct
# MAGIC   , COALESCE(hp.PC_FIRST, hx.PC_FIRST) AS ProfitCommissionDate
# MAGIC   , 0 AS RateOnLinePct
# MAGIC   , pc.REIN_EXP AS ReinsurerExpensePct
# MAGIC   , co.COMM_PC AS OverriderCommissionPct
# MAGIC   , reinst.ReinstatementNumber
# MAGIC   , reinst.ReinstatementTerm
# MAGIC   , 0 AS ReinstatementPct
# MAGIC   , CASE WHEN WholeAccountFlag.GROUP_DESC = 'Whole Account' THEN True ELSE False END AS ORIWholeAccountQSFlag
# MAGIC   
# MAGIC FROM MyMI_Pre_GXLP.dbo_CTR_HDR c
# MAGIC INNER JOIN MyMI_Abst_GXLP.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = c.CONTRACT
# MAGIC AND lac.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN (
# MAGIC   SELECT Contract,VERSION_DATE AS ORIDateCreated
# MAGIC   FROM MyMI_Pre_GXLP.dbo_CTR_HDR
# MAGIC   WHERE VERSION_NO=1
# MAGIC   ) VD
# MAGIC ON c.CONTRACT=VD.CONTRACT
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_HDRX hx
# MAGIC ON hx.CONTRACT = c.CONTRACT
# MAGIC AND hx.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_HDRP hp
# MAGIC ON hp.CONTRACT = c.CONTRACT
# MAGIC AND hp.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_HDRM cm
# MAGIC ON cm.CONTRACT = c.CONTRACT
# MAGIC AND cm.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN (
# MAGIC   SELECT distinct A.CONTRACT,A.CURRENCY,A.VERSION_NO,SUM(A.ORIAdjustmentPremiumSettCcy) AD_PM_CCY,SUM(A.ORIDepositPremiumSettCcy) MD_PM_CCY,SUM(A.EST_AMT) EPI_CCY,SUM(A.MIN_CCY) MIN_CCY
# MAGIC FROM (
# MAGIC SELECT CONTRACT,CURRENCY,VERSION_NO,SUM(CASE WHEN OPR.CATEGORY IN ('DEP','FLT') THEN AMT_CCY ELSE 0 END) AS ORIDepositPremiumSettCcy,SUM(CASE WHEN OPR.CATEGORY = 'ADJ' THEN AMT_CCY ELSE 0 END) AS ORIAdjustmentPremiumSettCcy,NULL EST_AMT,NULL MIN_CCY
# MAGIC FROM myMI_pre_GXLP.dbo_OUT_PRM OPR
# MAGIC WHERE OPR.CATEGORY IN ('DEP','FLT','ADJ') AND OPR.NOTE_TYPE IN ('A', 'F') AND OPR.PM_STAT = 0
# MAGIC GROUP BY CONTRACT,CURRENCY,VERSION_NO
# MAGIC UNION
# MAGIC SELECT CONTRACT,CURRENCY,VERSION_NO,NULL MD_PM_CCY,NULL AD_PM_CCY,EST_AMT EPI_CCY,MIN_CCY
# MAGIC FROM myMI_pre_GXLP.dbo_CTR_PRM
# MAGIC )A 
# MAGIC GROUP BY A.CONTRACT,A.CURRENCY,A.VERSION_NO 
# MAGIC ) ocp
# MAGIC ON ocp.CONTRACT = c.CONTRACT AND OCP.VERSION_NO = C.VERSION_NO
# MAGIC LEFT JOIN (
# MAGIC    SELECT A.CONTRACT
# MAGIC    , SUM(A.ADJ_RATE) AS ADJ_RATE
# MAGIC    , A.VERSION_NO
# MAGIC    FROM MyMI_pre_GXLP.dbo_CTR_PM_ADJ A
# MAGIC    INNER JOIN MyMI_pre_GXLP.dbo_CTR_PM_USAGE U
# MAGIC    ON A.CONTRACT = U.CONTRACT AND A.VERSION_NO = U.VERSION_NO
# MAGIC    WHERE (A.STAT = 0 or U.PM_STAT = 1) and (A.CEDED = 0 or U.PM_STAT = 0)
# MAGIC    AND PM_STAT = 0
# MAGIC    GROUP BY A.CONTRACT, A.VERSION_NO
# MAGIC   ) a
# MAGIC ON a.CONTRACT = c.CONTRACT
# MAGIC AND a.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_PCOMM pc
# MAGIC ON pc.CONTRACT = c.CONTRACT
# MAGIC AND pc.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_RST cr
# MAGIC ON cr.CONTRACT = c.CONTRACT
# MAGIC AND cr.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN MyMI_Abst_GXLP.Stg10_ORIContractReinstatement reinst
# MAGIC ON reinst.CONTRACT = c.CONTRACT
# MAGIC LEFT JOIN (
# MAGIC   SELECT  DISTINCT CONTRACT
# MAGIC     , VERSION_NO
# MAGIC     , co.CESS_ID
# MAGIC     , SUM(COMM_PC) AS COMM_PC
# MAGIC   FROM MyMI_Pre_GXLP.dbo_CTR_COMM co
# MAGIC   INNER JOIN MyMI_Pre_GXLP.dbo_CESSION_IDS cess
# MAGIC   ON cess.CESS_ID = co.CESS_ID
# MAGIC   WHERE   cess.CESS_ID = 050
# MAGIC   GROUP BY CONTRACT, VERSION_NO, co.CESS_ID
# MAGIC ) co
# MAGIC ON co.CONTRACT = c.CONTRACT
# MAGIC AND co.VERSION_NO = c.VERSION_NO
# MAGIC LEFT JOIN MyMI_Abst_GXLP.Stg10_ORIContractAggregateLimit al
# MAGIC ON al.CONTRACT = c.CONTRACT
# MAGIC LEFT JOIN (
# MAGIC   SELECT c1.CONTRACT
# MAGIC     , gcd.GROUP_DESC
# MAGIC   FROM MyMI_Pre_GXLP.dbo_CTR_HDR c1
# MAGIC   INNER JOIN MyMI_Pre_GXLP.dbo_GROUP_CBR gc
# MAGIC   ON gc.CBR = c1.CONTRACT
# MAGIC   INNER JOIN MyMI_Pre_GXLP.dbo_GROUP_CD gcd
# MAGIC   ON gcd.GROUP_CD = gc.GROUP_CD
# MAGIC   ) WholeAccountFlag
# MAGIC ON WholeAccountFlag.CONTRACT = c.CONTRACT
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_prog_cod PCD
# MAGIC ON C.PROG_CD = PCD.PROG_CD
# MAGIC WHERE C.CONTRACT in ('ZA202G16A001/R','ZA301C16A001/R')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_abst_gxlp.stg30_oricontractbase
# MAGIC WHERE CONTRACTREF in ('ZA202G16A001/R','ZA202G16A001')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_abst_gxlp.stg60_oricontract
# MAGIC WHERE ORIDateCreated is null
# MAGIC --GXLPContractReference in ('ZA301C16A001','ZA202G16A001')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT 
# MAGIC    CONCAT ('GXLPContractReference=',GXLPContractReference)  AS BusinessKey
# MAGIC  , CONCAT ('MDSSourceSystemCode=',14) AS  FK_DWH_DimSourceSystem
# MAGIC  , CONCAT ('MDSCoverageBasis=',CoverageBasis) AS  FK_DWH_DimCoverageBasis
# MAGIC  , CONCAT ('GroupNewRenewal=',-1) AS  FK_DWH_DimNewRenewal
# MAGIC  , CONCAT ('MDSTerritory=',-1) AS  FK_DWH_DimTerritory
# MAGIC  , CONCAT ('ORIContractStatus=',ORIContractStatus) AS  FK_DWH_DimORIContractStatus
# MAGIC  , CONCAT ('GXLPORIContractType=',ContractType) AS  FK_DWH_DimORIContractType
# MAGIC  , CONCAT ('GXLPProgrammeCode=',ProgrammeCode) AS  FK_DWH_DimORIProgramme
# MAGIC  , GXLPContractReference
# MAGIC  , ORIContractDescription
# MAGIC  , ORIAdjustablePct
# MAGIC  , ORIAggregateDeductible
# MAGIC  , ORIAggregateLimit
# MAGIC  , ORIContractCurrency
# MAGIC  , ORIContractYear
# MAGIC  , ORIExcess100PctContractCcy
# MAGIC  , ORIExpiryDate
# MAGIC  , ORIInceptionDate
# MAGIC  , CAST(0 AS decimal(21,12)) AS ORIFixedROE
# MAGIC  , ORILimit100PctContractCcy
# MAGIC  , ORIManualContractFlag
# MAGIC  , ORIOrderPct
# MAGIC  , ORIPlacedPct
# MAGIC  , ORIProfitCommissionDate
# MAGIC  , ORIRateOnLinePct
# MAGIC  , ORIReinstatementNumber
# MAGIC  , ORIReinstatementTerm
# MAGIC  , ORIReinsurerExpensePct
# MAGIC  , OverriderCommissionPct
# MAGIC  , ORIWholeAccountQSFlag
# MAGIC  , Cast(COALESCE(ORIDateCreated,'2000-01-01') as Date) ORIDateCreated
# MAGIC  , ORIMasterContractReference 
# MAGIC FROM MyMI_Abst_GXLP.Stg60_ORIContract
# MAGIC WHERE ORIDateCreated IS NULL
