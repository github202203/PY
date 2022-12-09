# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ORIContractBase
# MAGIC AS
# MAGIC SELECT DISTINCT c.CONTRACT AS ContractRef
# MAGIC   , CASE
# MAGIC       WHEN instr(c.CONTRACT, '/') = 0 THEN c.CONTRACT
# MAGIC       ELSE LEFT(c.CONTRACT, instr(c.CONTRACT, '/')-1)
# MAGIC     END AS ORIMasterContractReference
# MAGIC   , c.VERSION_NO AS VersionNo
# MAGIC   , COALESCE(VD.ORIDateCreated,'01-01-2020') ORIDateCreated --Revert back once Data is fixed
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
# MAGIC             SELECT Contract,VERSION_DATE AS ORIDateCreated,MIN(VERSION_NO) VERSION_NO
# MAGIC             FROM MyMI_Pre_GXLP.dbo_CTR_HDR hd	
# MAGIC               --WHERE  CONTRACT= 'ZA301C16A001/R' 
# MAGIC             GROUP BY Contract,VERSION_DATE
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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_ORIContractBase

# COMMAND ----------

# DBTITLE 1,stg60_ORIContract
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ContractLimitUSD
# MAGIC AS
# MAGIC SELECT oc.ContractRef
# MAGIC   , oc.ContractCurrency 
# MAGIC   , EM.RateToUSD 
# MAGIC   , oc.Limit100PctContractCcy 
# MAGIC   , oc.Limit100PctContractCcy * EM.rateToUSD AS Limit100PctUSD
# MAGIC FROM vw_ORIContractBase oc
# MAGIC INNER JOIN MyMI_Abst_MDM.stg20_exchangeratemonthly EM
# MAGIC ON EM.CurrencyISOCode = oc.ContractCurrency
# MAGIC WHERE EM.MonthID = (
# MAGIC   SELECT MAX(MonthID)
# MAGIC   FROM MyMI_Abst_MDM.stg20_exchangeratemonthly
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ORIExpectedNetPremiumOrderUSD
# MAGIC AS
# MAGIC SELECT DISTINCT oap.ContractRef
# MAGIC   , SUM(oap.ORIExpectedNetPremiumOrder * EM.RateToUSD) AS ORIExpectedNetPremiumOrderUSD
# MAGIC FROM MyMI_Abst_GXLP.stg50_oriapplicablepremium oap
# MAGIC INNER JOIN vw_ORIContractBase oc
# MAGIC ON oc.ContractRef = oap.ContractRef 
# MAGIC INNER JOIN MyMI_Abst_MDM.stg20_exchangeratemonthly EM
# MAGIC ON EM.CurrencyISOCode = oap.Currency
# MAGIC WHERE    EM.MonthID = (
# MAGIC   SELECT MAX(MonthID)
# MAGIC   FROM MyMI_Abst_MDM.stg20_exchangeratemonthly
# MAGIC )
# MAGIC GROUP BY oap.ContractRef

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ORIRateOnLinePct
# MAGIC AS
# MAGIC SELECT oe.ContractRef
# MAGIC   , CASE
# MAGIC       WHEN SUM(lc.Limit100PctUSD) = 0 THEN 0
# MAGIC       ELSE SUM(oe.ORIExpectedNetPremiumOrderUSD) / SUM(lc.Limit100PctUSD)
# MAGIC     END AS ORIRateOnLinePct
# MAGIC FROM vw_ORIExpectedNetPremiumOrderUSD oe
# MAGIC INNER JOIN vw_ContractLimitUSD lc
# MAGIC ON lc.ContractRef = oe.ContractRef
# MAGIC GROUP BY oe.Contractref

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ORIContract
# MAGIC AS
# MAGIC SELECT 
# MAGIC     c.ContractRef AS GXLPContractReference
# MAGIC   , c.ORIMasterContractReference
# MAGIC   , VersionNo
# MAGIC   , c.ORIDateCreated
# MAGIC   , COALESCE(CoverageBasis, 'Unknown') AS CoverageBasis
# MAGIC   , COALESCE(NewRenewal, 0) AS NewRenewal
# MAGIC   , ContractType
# MAGIC   , COALESCE(ProgrammeCode, 0) AS ProgrammeCode
# MAGIC   , CAST(COALESCE(AdjustablePct, 0) AS DECIMAL(10, 7)) AS ORIAdjustablePct
# MAGIC   , CAST(COALESCE(AggregateDeductible, 0) AS DECIMAL(19, 4)) AS ORIAggregateDeductible 
# MAGIC   , CAST(COALESCE(AggregateLimit, 0) AS DECIMAL(19, 4)) AS ORIAggregateLimit 
# MAGIC   , COALESCE(ContractCurrency, '') AS ORIContractCurrency
# MAGIC   , ContractDescription AS ORIContractDescription
# MAGIC   , ContractYear AS ORIContractYear
# MAGIC   , CAST(COALESCE(Excess100PctContractCcy, 0) AS DECIMAL(19, 4)) AS ORIExcess100PctContractCcy 
# MAGIC   , CAST(COALESCE(ExpiryDate, '9999-01-01') AS Date) AS ORIExpiryDate 
# MAGIC   , CAST(COALESCE(InceptionDate, '1900-01-01') AS Date) AS ORIInceptionDate 
# MAGIC   , CAST(COALESCE(Limit100PctContractCcy,0) AS DECIMAL(19, 4)) AS ORILimit100PctContractCcy 
# MAGIC   , COALESCE(ManualContractFlag, false) AS ORIManualContractFlag
# MAGIC   , CAST(COALESCE(OrderPct, 0) AS DECIMAL(10, 7)) AS ORIOrderPct 
# MAGIC   , CAST(COALESCE((CASE
# MAGIC       WHEN PlacedPct > 999 THEN 999
# MAGIC       ELSE PlacedPct
# MAGIC     END ), 0) AS DECIMAL(10, 7))  AS ORIPlacedPct 
# MAGIC   , CAST(COALESCE(ProfitCommissionDate, '1900-01-01') AS Date) AS ORIProfitCommissionDate
# MAGIC   , CAST(COALESCE((CASE
# MAGIC       WHEN COALESCE(ORL.ORIRateOnLinePct, 0) > 999 THEN 999
# MAGIC       ELSE CASE
# MAGIC         WHEN COALESCE(ORL.ORIRateOnLinePct, 0) < -999 THEN -999
# MAGIC         ELSE COALESCE(ORL.ORIRateOnLinePct, 0)
# MAGIC       END
# MAGIC     END ), 0) AS DECIMAL(10, 7)) AS ORIRateOnLinePct 
# MAGIC   , CAST(COALESCE(ReinsurerExpensePct, 0) AS DECIMAL(10, 7)) AS ORIReinsurerExpensePct 
# MAGIC   , CAST(COALESCE(OverriderCommissionPct, 0) AS DECIMAL(10, 7)) AS OverriderCommissionPct
# MAGIC   , CAST(COALESCE(c.ReinstatementNumber, 0) AS Int) AS ORIReinstatementNumber 
# MAGIC   , COALESCE(c.ReinstatementTerm, '') AS ORIReinstatementTerm 
# MAGIC   , CAST(COALESCE(Reinstatementpct, 0) AS DECIMAL(10, 7)) AS ReinstatementPct 
# MAGIC   , IFNULL(
# MAGIC       CASE 
# MAGIC         --Fully automated (recoveries & premium)
# MAGIC         WHEN   c.ContractType IN ( 'PR', 'FP' )
# MAGIC            AND cpp.USAGE & 16 = 16
# MAGIC            AND cpp.USAGE & 32 <> 32 
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 1 )
# MAGIC         WHEN   c.ContractType IN ( 'XL', 'FX' )
# MAGIC            AND cpp.USAGE & 1 = 1
# MAGIC            AND cpp.USAGE & 2 <> 2
# MAGIC            AND cpp.ADJ_BASIS = 'F' 
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 1 )
# MAGIC         WHEN   c.ContractType IN ( 'XL', 'FX' )
# MAGIC            AND cpp.USAGE & 1 = 1
# MAGIC            AND cpp.USAGE & 4 = 4
# MAGIC            AND cpp.ADJ_BASIS = 'A' 
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 1 )
# MAGIC 
# MAGIC         --Partially automated (recoveries or premium)	
# MAGIC         WHEN   c.ContractType IN ( 'PR', 'FP' )
# MAGIC            AND cpp.USAGE & 16 = 16
# MAGIC            AND cpp.USAGE & 32 = 32 
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 5 )  
# MAGIC         WHEN   c.ContractType IN ( 'XL', 'FX' )
# MAGIC            AND cpp.USAGE & 1 = 1
# MAGIC            AND cpp.USAGE & 2 = 2
# MAGIC            AND cpp.ADJ_BASIS = 'F' THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 5 ) 
# MAGIC         WHEN   c.ContractType IN ( 'XL', 'FX' )
# MAGIC            AND ((cpp.USAGE & 1 <> 1 AND cpp.USAGE & 4 = 4) OR (cpp.USAGE & 1 = 1 AND cpp.USAGE & 4 <> 4))
# MAGIC            AND cpp.ADJ_BASIS = 'A' THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 5 ) 
# MAGIC 
# MAGIC         --Fixed fee contract
# MAGIC         WHEN c.ContractType = 'CO' 
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 6 ) 
# MAGIC 
# MAGIC         --Not automated - Manuals
# MAGIC         WHEN  ( cpp.USAGE = 64 OR cpp.USAGE = 72 OR cpp.USAGE = 8)
# MAGIC            AND (cpp.PROP_CD NOT IN ('Dummy','Dummy1', 'DUMMY', 'DUMMY1'  ) 
# MAGIC              OR cpp.PROG_CD NOT IN ('Dummy', 'Dummy1', 'DUMMY', 'DUMMY1' ) ) 
# MAGIC            AND (cpp.PROP_CD NOT LIKE 'SKELETON%' 
# MAGIC              OR cpp.PROG_CD NOT LIKE 'SKELETON%') 
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 2 ) 
# MAGIC 
# MAGIC         --Not automated - Skeletons (monitored)
# MAGIC         WHEN   cpp.PROP_CD LIKE 'SKELETON%'
# MAGIC             OR cpp.PROG_CD LIKE 'SKELETON%' 
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 3 ) 
# MAGIC 
# MAGIC         --Not automated - Skeletons (not monitored)
# MAGIC         WHEN ( cpp.PROP_CD IN ( 'Dummy', 'Dummy1', 'DUMMY', 'DUMMY1' )  
# MAGIC             OR cpp.PROG_CD IN ( 'Dummy', 'Dummy1', 'DUMMY', 'DUMMY1' ) )                  
# MAGIC         THEN ( SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus WHERE StatusID = 4 )
# MAGIC       END, '') AS ORIContractStatus
# MAGIC   , ORIWholeAccountQSFlag 
# MAGIC   , ROW_NUMBER() OVER ( PARTITION BY c.ContractRef, ContractDescription, ContractYear ORDER BY c.ContractRef ) AS RowNumber                
# MAGIC   
# MAGIC FROM vw_ORIContractBase c
# MAGIC LEFT JOIN vw_ORIRateOnLinePct ORL
# MAGIC        ON ORL.ContractRef = c.ContractRef
# MAGIC LEFT JOIN MyMI_Abst_GXLP.Stg20_ORIContractPropProgCode cpp
# MAGIC        ON cpp.Contract = c.ContractRef

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW VW_stg60_ORIContract
# MAGIC AS
# MAGIC SELECT DISTINCT 
# MAGIC   CoverageBasis
# MAGIC  , ORIContractStatus
# MAGIC  , ContractType
# MAGIC  , ProgrammeCode
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
# MAGIC FROM vw_ORIContract
# MAGIC WHERE RowNumber=1

# COMMAND ----------

# DBTITLE 1,DimORIContract
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW VW_DimORIContract
# MAGIC AS
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
# MAGIC  ,  Cast(COALESCE(ORIDateCreated,'2000-01-01') as Date) ORIDateCreated --revert this after Source Data is fixed
# MAGIC  , ORIMasterContractReference 
# MAGIC FROM VW_stg60_ORIContract
# MAGIC --WHERE GXLPContractReference = 'ZA301C16A001/R' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from VW_DimORIContract UQ
# MAGIC FULL OUTER JOIN  mymi_trans_gxlp.dimoricontract OQ
# MAGIC ON UQ.BusinessKey = OQ.BusinessKey
# MAGIC AND UQ.FK_DWH_DimSourceSystem = OQ.FK_DWH_DimSourceSystem
# MAGIC AND UQ.FK_DWH_DimCoverageBasis = OQ.FK_DWH_DimCoverageBasis
# MAGIC AND UQ.FK_DWH_DimNewRenewal = OQ.FK_DWH_DimNewRenewal
# MAGIC AND UQ.FK_DWH_DimTerritory = OQ.FK_DWH_DimTerritory
# MAGIC AND UQ.FK_DWH_DimORIContractStatus = OQ.FK_DWH_DimORIContractStatus
# MAGIC AND UQ.FK_DWH_DimORIContractType = OQ.FK_DWH_DimORIContractType
# MAGIC AND UQ.FK_DWH_DimORIProgramme = OQ.FK_DWH_DimORIProgramme
# MAGIC AND UQ.GXLPContractReference = OQ.GXLPContractReference
# MAGIC AND UQ.ORIContractDescription = OQ.ORIContractDescription
# MAGIC AND UQ.ORIAdjustablePct = OQ.ORIAdjustablePct
# MAGIC AND UQ.ORIAggregateDeductible = OQ.ORIAggregateDeductible
# MAGIC AND UQ.ORIAggregateLimit = OQ.ORIAggregateLimit
# MAGIC AND UQ.ORIContractCurrency = OQ.ORIContractCurrency
# MAGIC AND UQ.ORIContractYear = OQ.ORIContractYear
# MAGIC AND UQ. = OQ.
# MAGIC AND UQ. = OQ.
# MAGIC AND UQ. = OQ.
# MAGIC AND UQ. = OQ.
# MAGIC AND UQ. = OQ.
# MAGIC AND UQ. = OQ.

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT(*)
# MAGIC from mymi_trans_gxlp.dimoricontract
# MAGIC --WHERE GXLPContractReference = 'ZA301C16A001/R' 
