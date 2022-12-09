# Databricks notebook source
dbutils.widgets.text("EntityName","dbo_ACCRUAL","EntityName")
dbutils.widgets.text("Subject","MyMI_Pre_GXLP210","Subject")
dbutils.widgets.text("Security","Internal","Security")
dbutils.widgets.text("DataMart","MyMI","DataMart")
dbutils.widgets.text("DataMartLoadID","1","DataMartLoadID")
dbutils.widgets.text("InitialDateTime","00000000_0000","InitialDateTime")
dbutils.widgets.text("EndDateTime","20220209_0800","EndDateTime")


## set variables
str_entityName      = dbutils.widgets.get("EntityName")
str_subject         = dbutils.widgets.get("Subject")
str_security        = dbutils.widgets.get("Security")
str_dataMart        = dbutils.widgets.get("DataMart")
str_dataMartLoadID  = dbutils.widgets.get("DataMartLoadID")
str_initialDateTime   = dbutils.widgets.get("InitialDateTime")
str_endDateTime     = dbutils.widgets.get("EndDateTime")
str_layer          = 'Transformed'
## Fix vars
str_pattern = 'Pre'

# COMMAND ----------

str_year     = str_endDateTime[0:4]
str_month    = str_endDateTime[0:6]
str_day      = str_endDateTime[0:8]
str_folter   = str_endDateTime[0:11]
str_filename = str_entityName + '_' + str_endDateTime
str_hour     = str(int(str_endDateTime[-4:]) - 100)
str_endRange  = str_day + '_' + str_hour

# COMMAND ----------

str_filePath = 'dbfs:/mnt/main/' + str_layer + '/' + str_subject + '/' + str_security + '/' + str_entityName + '/' + str_year + '/' + str_month + '/' + str_day + '/' + str_folter + '/' + str_filename + '.parquet'
df_unfilteredData = spark.read.format('parquet').option("header", "true").load(str_filePath)
##df= spark.read.format('parquet').option("header", "true").load(str_filePath)
display(df_unfilteredData)


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from MyMI_pre_gxlp210.dbo_accrual

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct PROG_CD
# MAGIC FROM mymi_pre_gxlp.dbo_ctr_hdrx

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT prog_cd,prop_cd
# MAGIC FROM mymi_abst_gxlp.Stg20_ORIContractPropProgCode

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM  MyMI_Pre_GXLP210.dbo_Ledger

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM myMI_abst_gxlp.stg10_ORIProgramme

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM MYMI_ABST_GXLP210.STG10_ORIProgramme gx10
# MAGIC LEFT JOIN MYMI_ABST_GXLP.STG10_ORIProgramme gx
# MAGIC on gx10.Prog_CD = gx.Prog_CD
# MAGIC and gx10.Prog_Desc = gx.Prog_Desc
# MAGIC and gx10.Dept_IND = gx.Dept_IND
# MAGIC --WHERE gx.prog_cd is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM MYMI_ABST_GXLP210.Stg20_ORIUSMSigningTransaction gx10
# MAGIC LEFT JOIN MYMI_ABST_GXLP.Stg20_ORIUSMSigningTransaction gx
# MAGIC on gx10.ORIContractReference = gx.ORIContractReference
# MAGIC and gx10.ORIBrokerReference = gx.ORIBrokerReference
# MAGIC and gx10.LORSORIReference = gx.LORSORIReference
# MAGIC and gx10.Syndicate = gx.Syndicate
# MAGIC and gx10.yoa = gx.yoa
# MAGIC and gx10.brokercode = gx.brokercode
# MAGIC and gx10.BrokerPseudonym = gx.BrokerPseudonym
# MAGIC and gx10.SettlementCurrencyISO = gx.SettlementCurrencyISO
# MAGIC and gx10.TrustFundCode = gx.TrustFundCode
# MAGIC and gx10.RiskCode = gx.RiskCode
# MAGIC and gx10.FILCode = gx.FILCode

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM MYMI_ABST_GXLP210.Stg20_ORIUSMSigningTransaction gx10

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*)
# MAGIC FROM MYMI_ABST_GXLP210.Stg10_ORILatestActiveContract GX10
# MAGIC LEFT JOIN MYMI_ABST_GXLP.Stg10_ORILatestActiveContract GX
# MAGIC ON GX10.CONTRACT = GX.CONTRACT
# MAGIC AND GX10.VERSION_NO = GX.VERSION_NO
# MAGIC WHERE GX.CONTRACT IS NULL

# COMMAND ----------



# COMMAND ----------

df_load = spark.sql("SELECT * FROM MyMI_abst_GXLP210.stg20_oriaccrualbase")
#display(df_load)
df_load.schema
display(df_load)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(gx10.Contract)
# MAGIC FROM MYMI_ABST_GXLP210.stg20_oriaccrualbase gx10
# MAGIC LEFT JOIN MYMI_ABST_GXLP.stg20_oriaccrualbase gx
# MAGIC ON gx10.Contract						= gx.Contract
# MAGIC and gx10.BrokerCode						= gx.BrokerCode
# MAGIC and gx10.ReinsurerCode					= gx.ReinsurerCode
# MAGIC and gx10.EventCode						= gx.EventCode
# MAGIC and gx10.SubEventCode					= gx.SubEventCode
# MAGIC and gx10.Source							= gx.Source
# MAGIC and gx10.TransactionType				= gx.TransactionType
# MAGIC and gx10.ExternalSyndicate				= gx.ExternalSyndicate
# MAGIC and gx10.GroupClassCode					= gx.GroupClassCode
# MAGIC and gx10.SubClassCode					= gx.SubClassCode
# MAGIC and gx10.StatsMinorClassCode			= gx.StatsMinorClassCode
# MAGIC and gx10.StatsMajorClassCode			= gx.StatsMajorClassCode
# MAGIC and gx10.TrustFundCode					= gx.TrustFundCode
# MAGIC and gx10.LloydsRiskCode					= gx.LloydsRiskCode
# MAGIC and gx10.FILCode4						= gx.FILCode4
# MAGIC and gx10.StatsClassTypeCode				= gx.StatsClassTypeCode
# MAGIC and gx10.InternalSyndicate				= gx.InternalSyndicate
# MAGIC and gx10.YearofAccount					= gx.YearofAccount
# MAGIC and gx10.XLProPolicyReference			= gx.XLProPolicyReference
# MAGIC and gx10.ClaimReference					= gx.ClaimReference
# MAGIC and gx10.SettlementCurrency				= gx.SettlementCurrency
# MAGIC and gx10.Period							= gx.Period
# MAGIC and gx10.EclipsePolicyID				= gx.EclipsePolicyID
# MAGIC and gx10.EclipsePolicyLineID			= gx.EclipsePolicyLineID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM MYMI_ABST_GXLP210.STG10_ORIProgramme gx10
# MAGIC LEFT JOIN MYMI_ABST_GXLP.STG10_ORIProgramme gx
# MAGIC on gx10.Prog_CD = gx.Prog_CD
# MAGIC and gx10.Prog_Desc = gx.Prog_Desc
# MAGIC and gx10.Dept_IND = gx.Dept_IND
# MAGIC --WHERE gx.prog_cd is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  CPR.CONTRACT,CPR.CURRENCY,
# MAGIC 		CPR.EST_AMT,
# MAGIC 		CPR.MIN_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY IN ( 'DEP','FLT') THEN AMT_CCY ELSE 0 END) AS MD_PM_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY = 'ADJ' THEN AMT_CCY ELSE 0 END) AS AD_PM_CCY
# MAGIC FROM MyMI_pre_gxlp210.dbo_out_prm  OPR
# MAGIC inner join MyMI_pre_gxlp210.dbo_CTR_PRM CPR
# MAGIC ON	CPR.CONTRACT = OPR.CONTRACT
# MAGIC AND		CPR.CURRENCY = OPR.CURRENCY
# MAGIC WHERE OPR.CATEGORY IN ('DEP','FLT','ADJ') AND  OPR.NOTE_TYPE IN ('A', 'F') AND OPR.PM_STAT = 0 --and ( opr.CONTRACT = 'ZP300J20A001' or cpr.CONTRACT = 'ZP300J20A001')
# MAGIC GROUP BY CPR.CONTRACT , CPR.CURRENCY,
# MAGIC CPR.EST_AMT,
# MAGIC CPR.MIN_CCY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ch.CONTRACT
# MAGIC   , b.BROKER_CD
# MAGIC   , sc.REIN_CD
# MAGIC   , ocp.CURRENCY
# MAGIC   , ocp.MD_PM_CCY AS ORIDepositPremiumSettCcy
# MAGIC   , ocp.EPI_CCY AS ORIEstimatedPremiumSettCcy
# MAGIC   , ocp.AD_PM_CCY AS ORIAdjustmentPremiumSettCcy
# MAGIC   , ocp.MIN_CCY AS ORIMinimumPremiumSettCcy
# MAGIC   , a.ADJ_RATE AS AdjustmentRate
# MAGIC 
# MAGIC FROM MyMI_Pre_GXLP210.dbo_CTR_HDR ch
# MAGIC INNER JOIN MyMI_Abst_GXLP210.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = ch.CONTRACT
# MAGIC AND lac.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP210.dbo_CTR_BRK b
# MAGIC ON b.CONTRACT = ch.CONTRACT
# MAGIC AND b.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP210.dbo_CTR_SEC sc
# MAGIC ON sc.CONTRACT = ch.CONTRACT
# MAGIC AND sc.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP210.dbo_CTR_HDRX cp
# MAGIC ON cp.CONTRACT = ch.CONTRACT
# MAGIC AND cp.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC     SELECT  CPR.CONTRACT,CPR.CURRENCY CURRENCY,
# MAGIC 		CPR.EST_AMT EPI_CCY,
# MAGIC 		CPR.MIN_CCY MIN_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY IN ( 'DEP','FLT') THEN AMT_CCY ELSE 0 END) AS MD_PM_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY = 'ADJ' THEN AMT_CCY ELSE 0 END) AS AD_PM_CCY
# MAGIC FROM MyMI_pre_gxlp210.dbo_out_prm  OPR
# MAGIC inner join MyMI_pre_gxlp210.dbo_CTR_PRM CPR
# MAGIC ON	CPR.CONTRACT = OPR.CONTRACT
# MAGIC AND		CPR.CURRENCY = OPR.CURRENCY
# MAGIC WHERE OPR.CATEGORY IN ('DEP','FLT','ADJ') AND  OPR.NOTE_TYPE IN ('A', 'F') AND OPR.PM_STAT = 0 --and ( opr.CONTRACT = 'ZP300J20A001' or cpr.CONTRACT = 'ZP300J20A001')
# MAGIC GROUP BY CPR.CONTRACT , CPR.CURRENCY,
# MAGIC CPR.EST_AMT,
# MAGIC CPR.MIN_CCY
# MAGIC ) ocp
# MAGIC ON ocp.CONTRACT = ch.CONTRACT
# MAGIC LEFT JOIN (
# MAGIC SELECT A.CONTRACT
# MAGIC    , SUM(A.ADJ_RATE) AS ADJ_RATE
# MAGIC    , A.VERSION_NO
# MAGIC  FROM MyMI_pre_gxlp210.dbo_CTR_PM_ADJ A
# MAGIC INNER JOIN MyMI_pre_gxlp210.dbo_CTR_PM_USAGE U
# MAGIC ON A.CONTRACT = U.CONTRACT AND A.VERSION_NO = U.VERSION_NO
# MAGIC WHERE (A.STAT = 0 or U.PM_STAT = 1) and (A.CEDED = 0 or U.PM_STAT = 0)
# MAGIC AND PM_STAT = 0
# MAGIC GROUP BY A.CONTRACT, A.VERSION_NO
# MAGIC ) a
# MAGIC ON a.CONTRACT = ch.CONTRACT
# MAGIC AND a.VERSION_NO = ch.VERSION_NO
# MAGIC where ch.contract = '1694-X303'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ch.CONTRACT
# MAGIC   , b.BROKER_CD
# MAGIC   , sc.REIN_CD
# MAGIC   , ocp.CURRENCY
# MAGIC   , ocp.MD_PM_CCY AS ORIDepositPremiumSettCcy
# MAGIC   , ocp.EPI_CCY AS ORIEstimatedPremiumSettCcy
# MAGIC   , ocp.AD_PM_CCY AS ORIAdjustmentPremiumSettCcy
# MAGIC   , ocp.MIN_CCY AS ORIMinimumPremiumSettCcy
# MAGIC   , a.ADJ_RATE AS AdjustmentRate
# MAGIC 
# MAGIC FROM MyMI_Pre_GXLP.dbo_CTR_HDR ch
# MAGIC INNER JOIN MyMI_Abst_GXLP.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = ch.CONTRACT
# MAGIC AND lac.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_BRK b
# MAGIC ON b.CONTRACT = ch.CONTRACT
# MAGIC AND b.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_SEC sc
# MAGIC ON sc.CONTRACT = ch.CONTRACT
# MAGIC AND sc.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_HDRX cp
# MAGIC ON cp.CONTRACT = ch.CONTRACT
# MAGIC AND cp.VERSION_NO = ch.VERSION_NO
# MAGIC LEFT JOIN MyMI_Pre_GXLP.dbo_CTR_PM ocp
# MAGIC ON ocp.CONTRACT = ch.CONTRACT
# MAGIC LEFT JOIN (
# MAGIC   SELECT  CONTRACT
# MAGIC     , VERSION_NO
# MAGIC     , SUM(ADJ_RATE) AS ADJ_RATE
# MAGIC   FROM    MyMI_Pre_GXLP.dbo_CTR_PM_ADJ
# MAGIC   WHERE   PM_STAT = 0
# MAGIC   GROUP BY CONTRACT, VERSION_NO
# MAGIC ) a
# MAGIC ON a.CONTRACT = ch.CONTRACT
# MAGIC AND a.VERSION_NO = ch.VERSION_NO
# MAGIC where ch.contract = '1694-X303'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.CONTRACT
# MAGIC    , SUM(A.ADJ_RATE) AS ADJ_RATE
# MAGIC    , A.VERSION_NO
# MAGIC  FROM MyMI_pre_gxlp210.dbo_CTR_PM_ADJ A
# MAGIC INNER JOIN MyMI_pre_gxlp210.dbo_CTR_PM_USAGE U
# MAGIC ON A.CONTRACT = U.CONTRACT AND A.VERSION_NO = U.VERSION_NO
# MAGIC WHERE (A.STAT = 0 or U.PM_STAT = 1) and (A.CEDED = 0 or U.PM_STAT = 0)
# MAGIC AND PM_STAT = 0 and a.contract = '1694-X303'
# MAGIC GROUP BY A.CONTRACT, A.VERSION_NO

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *
# MAGIC from MyMI_pre_gxlp210.dbo_CTR_PM_ADJ
# MAGIC where contract = '1694-X303'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *
# MAGIC from MyMI_pre_gxlp210.dbo_ctr_pm_usage
# MAGIC --where contract = '1694-X303'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT GXLPPeriod, ROW_NUMBER() OVER (ORDER BY GXLPPeriod ) AS PeriodOrder, MyMIProcessPeriod, FinancialPeriodEndDate, IsMDMClosedPeriod
# MAGIC FROM (
# MAGIC   SELECT mdm.GXLPPeriod, mdm.MyMIProcessPeriod, mdm.FinancialPeriodEndDate, true AS IsMDMClosedPeriod
# MAGIC   FROM MyMI_Pre_MDS.mdm_financialperiod mdm
# MAGIC   WHERE IsNotNull(mdm.GXLPPeriod)
# MAGIC   UNION 
# MAGIC   SELECT min(OpenGXLPPeriod.GXLPPeriod) AS GXLPPeriod
# MAGIC     , min(OpenMDMPeriod.MyMIProcessPeriod) AS MyMIProcessPeriod
# MAGIC     , min(OpenMDMPeriod.FinancialPeriodEndDate) AS FinancialPeriodEndDate
# MAGIC     , false AS IsMDMClosedPeriod
# MAGIC   FROM (
# MAGIC     SELECT a.GXLPPeriod
# MAGIC     FROM (
# MAGIC       SELECT Min(gxlp.Period) AS GXLPPeriod
# MAGIC       FROM  MyMI_Pre_GXLP210.dbo_ACCRUAL gxlp
# MAGIC       WHERE gxlp.Period > (
# MAGIC         SELECT max(mdm.GXLPPeriod) as LastMDMClosedPeriod
# MAGIC         FROM MyMI_Pre_MDS.mdm_financialperiod mdm
# MAGIC         WHERE IsNotNull(mdm.GXLPPeriod)
# MAGIC         )
# MAGIC       ) a
# MAGIC   ) OpenGXLPPeriod
# MAGIC CROSS JOIN MyMI_Pre_MDS.mdm_financialperiod OpenMDMPeriod
# MAGIC WHERE OpenMDMPeriod.MyMIProcessPeriod>(SELECT MAX(a.MyMIProcessPeriod) FROM MyMI_Pre_MDS.mdm_financialperiod a WHERE IsNotNull(a.GXLPPeriod))
# MAGIC ) Periods

# COMMAND ----------

str_FilePath = f"/mnt/main/Raw/GXLP210/Internal/dbo_CTR_PM_USAGE/2022/202203/20220309/20220309_12/dbo_CTR_PM_USAGE_20220309_1200/*.*"
df_load = df_unfilteredData = spark.read.format('parquet').option("header", "true").load(str_FilePath)
display(df_load)
result_df = df_load[df_load['CONTRACT']=='1694-X303']
result_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select joinflag,count(*)
# MAGIC from mymi_gxlp210_testing.factoriapplicablepremium
# MAGIC group by joinflag
