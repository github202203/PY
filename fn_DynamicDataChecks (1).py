# Databricks notebook source
# MAGIC %run ../../../Datalib/Common/Master

# COMMAND ----------

#Get Entity MetaData Details from DataLib
str_GetEntitiesGXLP210 = """SELECT	S.name+'.'+E.Name EntityName
FROM	METADATA.ENTITY E
JOIN	METADATA.SUBJECT S
ON		E.SUBJECTID = S.SUBJECTID
WHERE	S.NAME LIKE '%GXLP210' AND SUBLAYER IN ('Abstract','transformed') --and e.EntityID = 5457
order by s.name,sequence"""

str_resultgxlp210 = fn_queryControlDatabase(str_GetEntitiesGXLP210)


# COMMAND ----------

spark.sql("drop table IF EXISTS MyMI_GXLP210_Testing.Datacheckscount")
for GXLPEntity in str_resultgxlp210:
    ##Generating Select Statement Dynamically 
    str_Select = "SELECT * FROM "
    ## Loading GXLP 2.5 data to Dataframes and Renaming the columns to suffix with 25
    df_GXLP25Select = spark.sql(str_Select+GXLPEntity[0].replace('210',''))
    df_GXLP25Select = df_GXLP25Select.toDF(*(cols.replace(cols,cols+"25") for cols in df_GXLP25Select.columns))
    ## Loading GXLP 2.10 data to Dataframes 
    df_GXLP210Select = spark.sql(str_Select+GXLPEntity[0])
    df_ColumnsGXLP25 = df_GXLP25Select.columns
    df_ColumnsGXLP210 = df_GXLP210Select.columns
    df_GXLP210Select = df_GXLP210Select.withColumn("GXLP210",lit("Yes"))
    df_GXLP25Select = df_GXLP25Select.withColumn("GXLP25",lit("Yes"))
    df_GXLP25Select = df_GXLP25Select.withColumn("RowHash25",xxhash64(*df_GXLP25Select.schema.names))
    #display(df_GXLP25Select)
    df_GXLP210Select = df_GXLP210Select.withColumn("RowHash",xxhash64(*df_GXLP210Select.schema.names))
    #display(df_GXLP210Select)
    df_Join = df_GXLP210Select.join(df_GXLP25Select,df_GXLP210Select.RowHash==df_GXLP25Select.RowHash25,"fullouter")
    #df_Join = df_GXLP210Select.join(df_GXLP25Select,[col(GXLP25)==col(GXLP210) for (GXLP25,GXLP210) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)],"fullouter")
    df_Join.createOrReplaceTempView("GXLPJoinOutput")
    var_df = spark.sql("SELECT *,CASE WHEN GXLP25 IS NULL THEN '25' WHEN GXLP210 IS NULL THEN '210' ELSE 'MATCHED' END JoinFlag FROM GXLPJoinOutput")
    #var_df.show()
    str_Ename = GXLPEntity[0].split('.')
    str_FilePath = f"dbfs:/mnt/main/Transformed/MyMI_GXLP210_Testing/Internal/{str_Ename[1]}"
    #var_df.write.mode('overwrite').parquet(str_FilePath)
    var_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str_FilePath)
    ## create delta database
    spark.sql(""" 
               CREATE DATABASE IF NOT EXISTS {0}
               COMMENT '{0} database for {1}'
               LOCATION '{2}'
               """.format('MyMI_GXLP210_Testing', str_Ename[1], str_FilePath)
               )

    ## create delta table
    spark.sql(""" 
              CREATE TABLE IF NOT EXISTS {0}.{1}
              USING DELTA 
              LOCATION '{2}'
              """.format('MyMI_GXLP210_Testing', str_Ename[1], str_FilePath)
             )
    #####################################
    ## Print Message
    #####################################
    print ("mgs: Successfully created '" + str_Ename[1] + "' delta table")
    print ("mgs: Data check started to check match/unmatch '" + str_Ename[1] + "' delta table")
    str_Query = f""" SELECT count(*) as cnt,joinflag FROM MYMI_GXLP210_TESTING.{str_Ename[1]} GROUP BY JOINFLAG """
    df_Count = spark.sql(str_Query)
    df_Count.withColumn("TableName",lit(str_Ename[1])).show()
    #df_test.show()
    str_FilePath = f"dbfs:/mnt/main/Transformed/MyMI_GXLP210_Testing/Internal/Datacheckscount"
    #var_df.write.mode('overwrite').parquet(str_FilePath)
    df_Count.write.format("delta").mode("append").save(str_FilePath)
    ## create delta database
    spark.sql(""" 
               CREATE DATABASE IF NOT EXISTS {0}
               COMMENT '{0} database for {1}'
               LOCATION '{2}'
               """.format('MyMI_GXLP210_Testing', 'Datacheckscount', str_FilePath)
               )

    ## create delta table
    spark.sql(""" 
              CREATE TABLE IF NOT EXISTS {0}.{1}
              USING DELTA 
              LOCATION '{2}'
              """.format('MyMI_GXLP210_Testing', 'Datacheckscount', str_FilePath)
             )
    #df = spark.sql("select count(*) from MyMI_GXLP210_Testing.Datacheckscount")
    print ("mgs: Datacheck count updated for '" + str_Ename[1] + "' delta table")
    #=====================================================================================#
    """ df_Join = df_GXLP210Select.join(df_GXLP25Select,[col(GXLP25)==col(GXLP210) for (GXLP25,GXLP210) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)],"fullouter")
    df_Join = df_Join.withColumn("JoinFlag",when((col(GXLP25[0])==col(GXLP210[0]) for (GXLP25[0],GXLP210[0]) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)),lit("Matched")).otherwise(lit("Matched")))
    display(df_Join)
    df_Join = df_GXLP210Select.join(df_GXLP25Select,[col(GXLP25)==col(GXLP210) for (GXLP25,GXLP210) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)],"fullouter")
    df_Join = df_Join.withColumn("JoinFlag",when((col(GXLP25[0])==col(GXLP210[0]) for (GXLP25[0],GXLP210[0]) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)),lit("Matched")).otherwise(lit("Matched")))
    display(df_Join)
    #df_Join.withColumn('Join Flag',when())
    #str_Ename = GXLPEntity[0].split('.')
    #str_FilePath = f"dbfs:/mnt/main/Transformed/MyMI_GXLP210_Testing/Internal/{str_Ename[1]}"
    #df_Join.write.parquet(str_FilePath)"""

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*),joinflag
# MAGIC FROM MYMI_GXLP210_TESTING.stg10_orilatestactivecontract
# MAGIC --where /*joinflag in ('25','210') and */(contract = 'IM103511702' or contract25 = 'IM103511702')
# MAGIC GROUP BY JOINFLAG

# COMMAND ----------

# MAGIC %sql
# MAGIC select  period,sum(amt_gross) from mymi_pre_gxlp210.dbo_accrual group by period 
# MAGIC --where joinflag in ('25','210') and (contract = 'ZP401C15A001' or contract25 = 'ZP401C15A001')--and (oricontractreference ='ZM500Z17A00A' or oricontractreference25 = 'ZM500Z17A00A')

# COMMAND ----------

# MAGIC %sql
# MAGIC select  period,sum(amt_gross) from mymi_pre_gxlp.dbo_accrual group by period

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.stg20_oriaccrualbase where (PERIOD >= 202204 OR PERIOD25 >=202204) and JOINFLAG IN ('25','210') --GROUP BY JOINFLAG--(PERIOD = 202110 OR PERIOD25 =202110) AND JOINFLAG IN ('25','210') AND( CONTRACT = 'ZA295H93A001' OR CONTRACT25 = 'ZA295H93A001') and (yearofaccount = 1994 or yearofaccount25 = 1994) and (SettlementCurrency = 'USD' OR SettlementCurrency25 ='USD') and (LloydsRiskCode = 'K'or LloydsRiskCode25 = 'K') and (ReinsurerCode='A73058' or ReinsurerCode25='A73058') -- GROUP BY JOINFLAG

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(amt_gross),sum(amt_gross25) FROM mymi_gxlp210_testing.stg20_oriaccrualbase WHERE (PERIOD >= 202204 OR PERIOD25 >=202204)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg20_ORIApplicablePremiumBase WHERE JOINFLAG IN ('25','210') and (contract = 'ZM502Z06B001' or contract25 = 'ZM502Z06B001')   and (rein_cd = 'A10460' or rein_cd25 = 'A10460') and (currency = 'GBP' OR CURRENCY25 = 'GBP')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(ORIMinimumPremiumSettCcy),sum(ORIMinimumPremiumSettCcy25),sum(ORIMinimumPremiumSettCcy)-sum(ORIMinimumPremiumSettCcy25) AS DIFFERENCE25210 FROM mymi_gxlp210_testing.Stg20_ORIApplicablePremiumBase

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  CPR.CONTRACT ,
# MAGIC         CPR.CURRENCY CURRENCY,
# MAGIC 		CPR.EST_AMT EPI_CCY,
# MAGIC 		CPR.MIN_CCY MIN_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY IN ( 'DEP','FLT') THEN AMT_CCY ELSE 0 END) AS MD_PM_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY = 'ADJ' THEN AMT_CCY ELSE 0 END) AS AD_PM_CCY
# MAGIC FROM MyMI_pre_gxlp210.dbo_out_prm  OPR
# MAGIC inner join MyMI_pre_gxlp210.dbo_CTR_PRM CPR
# MAGIC ON	CPR.CONTRACT = OPR.CONTRACT
# MAGIC AND		CPR.CURRENCY = OPR.CURRENCY
# MAGIC WHERE OPR.CATEGORY IN ('DEP','FLT','ADJ') AND  OPR.NOTE_TYPE IN ('A', 'F') AND OPR.PM_STAT = 0 --and ( opr.CONTRACT = '11573H14' or cpr.CONTRACT = '11573H14')
# MAGIC GROUP BY CPR.CONTRACT , CPR.CURRENCY,
# MAGIC CPR.EST_AMT,
# MAGIC CPR.MIN_CCY

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MyMI_pre_gxlp.dbo_ctr_pm --where contract = '11573H14'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM MyMI_pre_gxlp210.dbo_out_prm
# MAGIC --WHERE CONTRACT = '11573H14'

# COMMAND ----------

df_Read = spark.read.parquet("dbfs:/mnt/main/Raw/GXLP210/Internal/dbo_OUT_PRM/2022/202204/20220411/20220411_17/dbo_OUT_PRM_20220411_1700/dbo.OUT_PRM.parquet")
#df_Read.show()
df_res= df_Read[df_Read.CONTRACT == '11573H14']
df_res.show()

# COMMAND ----------

df_Read = spark.read.parquet("dbfs:/mnt/main/Cleansed/GXLP210/Internal/dbo_OUT_PRM/2022/202204/20220411/20220411_17/dbo_OUT_PRM_20220411_1700.parquet/part-00000-tid-3435243275018275124-af17689a-d96d-4474-b07b-882caa9fa359-68-1-c000.snappy.parquet")
#df_Read.show()
df_Read.groupby().max("AMT_CCY").show()
#update to decimal(14,2)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT  *
# MAGIC FROM mymi_pre_gxlp.dbo_ACCRUAL
# MAGIC WHERE CONTRACT= 'ZA295H93A001' AND AC_YEAR =1994 AND PERIOD = 202204 AND AC_STAT7='K' AND REIN_CD='A73058'--AND CCY_SETT = 'USD'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg20_ORIContractPropProgCode  where JOINFLAG IN ('25','210') order by   contract ,contract25 --and (contract = 'JA560L11A001' or contract25 = 'JA560L11A001') 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg30_ORIContractBase  where JOINFLAG IN ('25','210') order by   contractref ,contractref25

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
# MAGIC     SELECT  CPR.CONTRACT ,
# MAGIC         CPR.CURRENCY CURRENCY,
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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(ORIMinimumPremiumSettCcy),sum(ORIMinimumPremiumSettCcy25),sum(ORIMinimumPremiumSettCcy)-sum(ORIMinimumPremiumSettCcy25) AS DIFFERENCE25210 FROM mymi_gxlp210_testing.Stg20_ORIApplicablePremiumBase

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(ORIDepositPremiumSettCcy),sum(ORIDepositPremiumSettCcy25),sum(ORIDepositPremiumSettCcy)-sum(ORIDepositPremiumSettCcy25) AS DIFFERENCE25210 FROM mymi_gxlp210_testing.Stg20_ORIApplicablePremiumBase

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(ORIAdjustmentPremiumSettCcy25),sum(ORIAdjustmentPremiumSettCcy),sum(ORIAdjustmentPremiumSettCcy)-sum(ORIAdjustmentPremiumSettCcy25) AS DIFFERENCE25210 FROM mymi_gxlp210_testing.Stg20_ORIApplicablePremiumBase

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(AdjustmentRate),sum(AdjustmentRate25),sum(AdjustmentRate25)-sum(AdjustmentRate) AS DIFFERENCE25210 FROM mymi_gxlp210_testing.Stg20_ORIApplicablePremiumBase

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg20_ORIApplicablePremiumBase WHERE JOINFLAG IN ('25','210') order by contract,contract25--and (contract = 'ZA233V97A001' or contract25 = 'ZA233V97A001' ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg20_ORIUSMSigningTransaction  WHERE JOINFLAG IN ('25','210') order by ORIContractReference,ORIContractReference25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg40_ORIPlacement  WHERE JOINFLAG IN ('25','210')  order by ContractRef,ContractRef25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg40_ORICessionBase  WHERE JOINFLAG IN ('25','210')  order by ContractRef,ContractRef25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg30_ORIAccrualUnallocatedPolicyLines    WHERE  JOINFLAG IN ('25','210')--(contract25='ZP191Q15A001' or contract25='ZP191Q15A001') --order by PROG_CD,PROG_CD25  and

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg50_ORIApplicablePremium    WHERE  JOINFLAG IN ('25','210')  order by ContractRef,ContractRef25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mymi_gxlp210_testing.Stg60_ORIContract    WHERE  JOINFLAG NOT IN ('25','210') order by ORIContractStatus,ORIContractStatus25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM MyMI_Abst_GXLP210.Stg20_ORIContractPropProgCode
# MAGIC WHERE  CONTRACT IN ('00029F001000','389FAC1','CD587N10A001') --USAGE IN  (64,72,8) AND

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Name FROM MyMI_Pre_MDS.MDM_OriContractStatus 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  count(GXLPContractReference)
# MAGIC FROM  mymi_gxlp210_testing.Stg60_ORIContract 
# MAGIC WHERE GXLPContractReference  IN (
# MAGIC SELECT ContractRef
# MAGIC FROM  mymi_gxlp210_testing.Stg30_OriContractBase)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  mymi_gxlp210_testing.Stg40_ORIClaimSubEvent 
# MAGIC WHERE JOINFLAG IN ('25','210')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_check
# MAGIC AS
# MAGIC SELECT COALESCE(o.ORIClaimEventDescription, '') AS ORIClaimEventDescription
# MAGIC   , COALESCE(o.ORIClaimSubEventCode, '') AS ORIClaimSubEventCode
# MAGIC   , COALESCE(o.ORIClaimSubEventDescription, '') AS ORIClaimSubEventDescription 
# MAGIC   , TRIM(UPPER(COALESCE(e.ClaimEventCode, ''))) AS MDSClaimEventCode 
# MAGIC   , TRIM(UPPER(COALESCE(ec.CatCode, COALESCE(e.CatastropheCode, '')))) AS MDSCatastropheCode 
# MAGIC FROM MyMI_Pre_MDS.mdm_ClaimEvent e
# MAGIC LEFT JOIN (SELECT e.Event AS ORIClaimEventCode
# MAGIC   , CONCAT(e._Desc,e.DESC2) AS ORIClaimEventDescription
# MAGIC   , s.Sub_EVENT AS ORIClaimSubEventCode
# MAGIC   , CONCAT(s._DESC,s.DESC2) AS ORIClaimSubEventDescription
# MAGIC FROM  MyMI_Pre_GXLP210.dbo_Event e 
# MAGIC INNER JOIN MyMI_Pre_GXLP210.dbo_SUB_EVENT s 
# MAGIC         ON s.EVENT=e.EVENT) o
# MAGIC        ON e.ClaimEventCode = o.ORIClaimEventCode
# MAGIC LEFT JOIN MyMI_Abst_Eclipse.Stg30_Claim ec
# MAGIC        ON ec.EventCode = e.ClaimEventCode
# MAGIC GROUP BY ORIClaimEventCode 
# MAGIC   , ORIClaimSubEventCode 
# MAGIC   , e.ClaimEventCode 
# MAGIC   , ORIClaimSubEventDescription
# MAGIC   , ORIClaimEventDescription
# MAGIC   , e.CatastropheCode 
# MAGIC   , ec.CatCode 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from vw_check
# MAGIC where MDSClaimEventCode= '80440'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.Event AS ORIClaimEventCode
# MAGIC   , CONCAT(e._Desc,e.DESC2) AS ORIClaimEventDescription
# MAGIC   , s.Sub_EVENT AS ORIClaimSubEventCode
# MAGIC   , CONCAT(s._DESC,s.DESC2) AS ORIClaimSubEventDescription
# MAGIC FROM  MyMI_Pre_GXLP210.dbo_Event e 
# MAGIC INNER JOIN MyMI_Pre_GXLP210.dbo_SUB_EVENT s 
# MAGIC         ON s.EVENT=e.EVENT
# MAGIC where e.Event = '80440'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from mymi_gxlp210_testing.Stg40_InwardOutwardContract 
# MAGIC where (PERIOD >= 202204 OR PERIOD25 >=202204) and JOINFLAG IN ('25','210') and (GXLPContractReference = 'ZM502Z16R001' or GXLPContractReference25= 'ZM502Z16R001')
# MAGIC order by GXLPContractReference,GXLPContractReference25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC      PL.EclipsePolicyLineID AS EclipsePolicyLineID 
# MAGIC     ,A.InternalSyndicate    AS MDSSyndicate
# MAGIC     ,GC.MDSORIClass         AS MDSORIClass 
# MAGIC     ,A.GroupClassCode       AS GroupClassCode
# MAGIC     ,14                     AS DimSourceSystemID 
# MAGIC     ,0                      AS VelocityLevel1Org 
# MAGIC     ,0                      AS VelocityPolicySequence 
# MAGIC     ,0                      AS VelocityCoverageNumber 
# MAGIC     ,0                      AS VelocityCoverageLineCode 
# MAGIC     ,''                     AS VelocitySyndicate 
# MAGIC     ,0                      AS ActualSigningDown 
# MAGIC     ,0                      AS DimEclipsePolicyGranularDetailExtID 
# MAGIC     ,'SIGNED'               AS PolicyStatus 
# MAGIC     ,0                      AS DimVelocityProductID 
# MAGIC     ,0                      AS DimVelocityPolicyGranularDetailExtID 
# MAGIC     ,0                      AS EstimatedSignedPct 
# MAGIC     ,0                      AS FullWrittenLinePct 
# MAGIC     ,100                    AS LinePct 
# MAGIC     ,100                    AS SignedLinePct 
# MAGIC     ,100                    AS SignedOrderPct 
# MAGIC     ,0                      AS WrittenLinePct 
# MAGIC     ,0                      AS WrittenOrderPct 
# MAGIC     ,0                      AS DimLeaderStatusID 
# MAGIC     ,0                      AS DimNewRenewalID
# MAGIC     
# MAGIC FROM  MyMI_Abst_GXLP210.Stg20_ORIAccrualBase A
# MAGIC LEFT JOIN MyMI_Trans_GXLP.DimORIEclipsePolicyLine PL
# MAGIC        ON A.GroupClassCode    = PL.ORIClassCode
# MAGIC       AND A.InternalSyndicate = PL.MDSSyndicate 
# MAGIC LEFT JOIN (SELECT 
# MAGIC    GC.BusinessKey 
# MAGIC   ,GC.MDSORIClass
# MAGIC   ,GC.MDSSyndicate
# MAGIC   ,OC.ORIClassCode
# MAGIC FROM       MyMI_Trans_GXLP.DimORIClass  OC
# MAGIC INNER JOIN MyMI_Trans_MDM.DimGroupClass GC ON GC.FK_DWH_DimORIClass = OC.BusinessKey) GC
# MAGIC        ON GC.ORIClassCode     = PL.ORIClassCode
# MAGIC       AND GC.MDSSyndicate     = PL.MDSSyndicate 
# MAGIC          
# MAGIC WHERE A.EclipsePolicyLineID = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC     FROM   MyMI_Abst_GXLP210.Stg20_ORIAccrualBase A
# MAGIC 	WHERE  EXISTS ( SELECT *
# MAGIC                     FROM  MyMI_Abst_GXLP210.Stg30_ORIPlacementBase   OP
# MAGIC 			     	WHERE OP.Contract  = A.Contract
# MAGIC 					  AND OP.REIN_CD   = A.ReinsurerCode
# MAGIC 					  AND OP.BROKER_CD = A.BrokerCode )
# MAGIC     and Contract = 'ZM502Z16R001' and FILCode4 = 'BDA2' AND LloydsRiskCode = 'V' AND Period=202204 AND EclipsePolicyID IN (703288,
# MAGIC 703302
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC     FROM   MyMI_Abst_GXLP.Stg20_ORIAccrualBase A
# MAGIC 	WHERE  EXISTS ( SELECT *
# MAGIC                     FROM  MyMI_Abst_GXLP.Stg30_ORIPlacementBase   OP
# MAGIC 			     	WHERE OP.Contract  = A.Contract
# MAGIC 					  AND OP.REIN_CD   = A.ReinsurerCode
# MAGIC 					  AND OP.BROKER_CD = A.BrokerCode )
# MAGIC     and Contract = 'ZM502Z16R001' and FILCode4 = 'BDA2' AND LloydsRiskCode = 'V' AND Period=202204 AND EclipsePolicyID IN (703288,
# MAGIC 703302
# MAGIC )

# COMMAND ----------



# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select *
# MAGIC from mymi_gxlp210_testing.Stg70_ORIApplicablePremium 
# MAGIC where JOINFLAG IN ('25','210') --and (GXLPContractReference = 'ZM502Z16R001' or GXLPContractReference25= 'ZM502Z16R001')
# MAGIC order by ContractRef,ContractRef25

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from mymi_gxlp210_testing.stg30_oricontractbase
# MAGIC where (ContractRef = 'EB501F17A0001' or ContractRef25 ='EB501F17A0001')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT ap.ContractRef
# MAGIC   , ap.BrokerCode
# MAGIC   , ap.ReinsurerCode
# MAGIC   , ap.Currency
# MAGIC   , ap.ORIDepositPremiumSettCcy
# MAGIC   , ap.ORIEstimatedPremiumSettCcy
# MAGIC   , ap.ORIAdjustmentPremiumSettCcy 
# MAGIC   , ap.ORIExpectedNetPremiumOrder 
# MAGIC   , ap.ORIMinimumPremiumSettCcy
# MAGIC   , ROW_NUMBER() OVER (PARTITION BY ap.ContractRef, ap.BrokerCode, ap.ReinsurerCode, ap.Currency ORDER BY ap.ORIDepositPremiumSettCcy DESC ) AS RowNumber
# MAGIC FROM (
# MAGIC   SELECT COALESCE(COALESCE(p.BROKER_CD, oe.BrokerCode), 'UNK') AS BrokerCode
# MAGIC     , COALESCE(COALESCE(p.Contract, oe.ContractReference), 'UNK') AS ContractRef 
# MAGIC     , COALESCE(COALESCE(p.Currency, oe.Currency), 'UNK') AS Currency 
# MAGIC     , COALESCE(COALESCE(p.REIN_CD, oe.ReinsurerCode), 'UNK') AS ReinsurerCode 
# MAGIC     , SUM(COALESCE(p.ORIDepositPremiumSettCcy, 0) * ( COALESCE(ob.BROKER_PC, 100) / 100 ) * ( COALESCE(op.ReinsurerPctOfOrder, 100) / 100 )) AS ORIDepositPremiumSettCcy
# MAGIC     , SUM(COALESCE(p.ORIAdjustmentPremiumSettCcy, 0) * ( COALESCE(ob.BROKER_PC, 100) / 100 ) * ( COALESCE(op.ReinsurerPctOfOrder, 100) / 100 )) AS ORIAdjustmentPremiumSettCcy
# MAGIC     , SUM(COALESCE(p.ORIMinimumPremiumSettCcy, 0) * ( COALESCE(ob.BROKER_PC, 100) / 100 ) * ( COALESCE(op.ReinsurerPctOfOrder, 100) / 100 )) AS ORIMinimumPremiumSettCcy 
# MAGIC     , SUM(COALESCE(oe.ORIEstimatedPremiumSettCcy, 0)) AS ORIEstimatedPremiumSettCcy 
# MAGIC     , COALESCE(CASE 
# MAGIC         WHEN oc.ContractType IN ( 'FX', 'XL' ) THEN CASE
# MAGIC           WHEN ISNULL(oc.AdjustablePct) OR oc.AdjustablePct = 0 THEN SUM(COALESCE(p.ORIDepositPremiumSettCcy, 0) * ( COALESCE(ob.BROKER_PC, 100) / 100 ) * ( COALESCE(op.ReinsurerPctOfOrder, 100) / 100 ))
# MAGIC           ELSE SUM(COALESCE(oe.ORIEstimatedPremiumSettCcy, 0)) * oc.AdjustablePct / 100
# MAGIC         END
# MAGIC       WHEN oc.contracttype IN ( 'FP', 'PR' ) THEN SUM(COALESCE(oe.ORIEstimatedPremiumSettCcy, 0))
# MAGIC       WHEN oc.contracttype IN ( 'CO', 'MS' ) THEN 0
# MAGIC       END ,0)AS ORIExpectedNetPremiumOrder
# MAGIC   FROM MyMI_Abst_GXLP210.stg20_oriapplicablepremiumbase p
# MAGIC   INNER JOIN MyMI_Abst_GXLP210.stg20_oricontractbroker ob
# MAGIC   ON ob.BROKER_CD = p.BROKER_CD
# MAGIC   AND ob.CONTRACT = p.Contract
# MAGIC   LEFT JOIN MyMI_Abst_GXLP210.stg30_oriplacementbase op
# MAGIC   ON op.Contract = p.Contract
# MAGIC   AND p.BROKER_CD = op.BROKER_CD
# MAGIC   AND op.REIN_CD = p.REIN_CD
# MAGIC   FULL OUTER JOIN MyMI_Abst_GXLP210.stg40_oriexpectedpremiumincome oe
# MAGIC   ON oe.ContractReference = p.Contract
# MAGIC   AND oe.BrokerCode = p.BROKER_CD
# MAGIC   AND oe.ReinsurerCode = p.REIN_CD
# MAGIC   AND oe.currency = p.Currency
# MAGIC   LEFT JOIN MyMI_Abst_GXLP210.stg30_oricontractbase oc
# MAGIC   ON oc.ContractRef = COALESCE(p.Contract, oe.ContractReference)
# MAGIC   GROUP BY COALESCE(COALESCE(p.BROKER_CD, oe.BrokerCode), 'UNK')
# MAGIC     , COALESCE(COALESCE(p.Contract, oe.ContractReference), 'UNK')
# MAGIC     , COALESCE(COALESCE(p.Currency, oe.Currency), 'UNK')
# MAGIC     , COALESCE(COALESCE(p.REIN_CD, oe.ReinsurerCode), 'UNK')
# MAGIC     , oc.ContractType
# MAGIC     , oc.AdjustablePct
# MAGIC ) ap
# MAGIC 
# MAGIC where ap.ContractRef='EB501F17A0001' --and ap.BrokerCode='1338'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from MyMI_Abst_GXLP210.stg30_oricontractbase
# MAGIC where (ContractRef = 'EB501F17A0001')-- or ContractRef25 ='EB501F17A0001')

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select *
# MAGIC from MyMI_Abst_GXLP210.stg40_oriexpectedpremiumincome
# MAGIC where (ContractReference = 'EB501F17A0001')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT oe.ContractReference
# MAGIC   , opc.REIN_CD AS ReinsurerCode 
# MAGIC   , ob.BROKER_CD AS BrokerCode 
# MAGIC   , CASE WHEN oe.Ccy = '#N/A' THEN null ELSE oe.Ccy END AS Currency
# MAGIC   , EPI * ( COALESCE(ob.BROKER_PC, 100) / 100 ) * ( COALESCE(opc.ReinsurerPctOfOrder, 100) / 100 ) AS ORIEstimatedPremiumSettCcy
# MAGIC FROM MyMI_Pre_MDS.MDM_ORIEPI oe
# MAGIC LEFT JOIN MyMI_Abst_GXLP210.stg30_oriplacementbase opc
# MAGIC ON oe.ContractReference = opc.Contract
# MAGIC LEFT JOIN MyMI_Abst_GXLP210.stg20_oricontractbroker ob
# MAGIC ON ob.BROKER_CD = opc.BROKER_CD
# MAGIC AND ob.CONTRACT = opc.Contract
# MAGIC where (ContractReference = 'EB501F17A0001')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cs.CONTRACT 
# MAGIC   , cs.VERSION_NO
# MAGIC   , r.REIN_CD 
# MAGIC   , MAX(cs.COMMUTED) AS COMMUTED 
# MAGIC   , brk.BROKER_LINE
# MAGIC   , brk.BROKER_CD
# MAGIC   , COALESCE(BROKER_PC,0) AS BROKER_PC
# MAGIC   , cs.unique_key
# MAGIC   /* Revised logic for Reinsurer Pct of Order*/
# MAGIC   , COALESCE( CASE WHEN COALESCE(brk.SEC_PC_IND,true) THEN cs.REIN_PC
# MAGIC                      WHEN brk.SEC_PC_IND = 0 THEN ( cs.REIN_PC / ch.PC_PLACED ) * 100
# MAGIC                 END,0) AS ReinsurerPctOfOrder
# MAGIC   , COALESCE( CASE WHEN NOT COALESCE(brk.SEC_PC_IND,true) THEN cs.REIN_PC
# MAGIC                      WHEN brk.SEC_PC_IND = 1 THEN ( cs.REIN_PC * ch.PC_PLACED ) / 100
# MAGIC                 END,0) AS ReinsurerPctOfWhole
# MAGIC   /* Added Broker Reference */
# MAGIC   , brk.BROKER_RF
# MAGIC FROM MyMI_Pre_GXLP210.dbo_CTR_SEC cs
# MAGIC INNER JOIN MyMI_Abst_GXLP210.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = cs.CONTRACT
# MAGIC AND lac.VERSION_NO = cs.VERSION_NO
# MAGIC INNER JOIN MyMI_Pre_GXLP210.dbo_REINSURE r
# MAGIC ON r.REIN_CD = cs.REIN_CD
# MAGIC INNER JOIN MyMI_Abst_GXLP210.Stg20_ORIContractBroker brk
# MAGIC ON brk.CONTRACT = cs.CONTRACT
# MAGIC AND brk.VERSION_NO = cs.VERSION_NO
# MAGIC INNER JOIN MyMI_Pre_GXLP210.dbo_CTR_HDR ch
# MAGIC ON ch.CONTRACT = cs.CONTRACT
# MAGIC AND ch.VERSION_NO = lac.VERSION_NO
# MAGIC where cs.CONTRACT = 'EB501F17A0001'
# MAGIC GROUP BY cs.CONTRACT
# MAGIC   , cs.VERSION_NO
# MAGIC   , r.REIN_CD
# MAGIC   , brk.BROKER_LINE 
# MAGIC   , brk.BROKER_CD 
# MAGIC   , BROKER_PC
# MAGIC   , cs.REIN_PC
# MAGIC   , cs.unique_key 
# MAGIC   , COALESCE( CASE WHEN COALESCE(brk.SEC_PC_IND,true) THEN cs.REIN_PC
# MAGIC                      WHEN brk.SEC_PC_IND = 0 THEN ( cs.REIN_PC / ch.PC_PLACED ) * 100
# MAGIC                 END,0) 
# MAGIC   , COALESCE( CASE WHEN NOT COALESCE(brk.SEC_PC_IND,true) THEN cs.REIN_PC
# MAGIC                      WHEN brk.SEC_PC_IND = 1 THEN ( cs.REIN_PC * ch.PC_PLACED ) / 100
# MAGIC                 END,0) 
# MAGIC   , brk.BROKER_RF

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC FROM MyMI_Pre_MDS.MDM_ORIEPI
# MAGIC where ContractReference = 'EB501F17A0001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(EPI_CCY),sum(MIN_CCY),sum(MD_PM_CCY),sum(AD_PM_CCY)
# MAGIC FROM MyMI_Pre_gxlp.DBO_CTR_PM

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(EPI_CCY),sum(MIN_CCY),sum(MD_PM_CCY),sum(AD_PM_CCY) FROM(
# MAGIC SELECT  CPR.CONTRACT ,
# MAGIC         CPR.CURRENCY CURRENCY,
# MAGIC 		CPR.EST_AMT EPI_CCY,
# MAGIC 		CPR.MIN_CCY MIN_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY IN ( 'DEP','FLT') THEN AMT_CCY ELSE 0 END) AS MD_PM_CCY,
# MAGIC     SUM(CASE WHEN OPR.CATEGORY = 'ADJ' THEN AMT_CCY ELSE 0 END) AS AD_PM_CCY
# MAGIC FROM MyMI_Pre_gxlp210.dbo_out_prm  OPR
# MAGIC left join MyMI_Pre_gxlp210.dbo_CTR_PRM CPR
# MAGIC ON	CPR.CONTRACT = OPR.CONTRACT
# MAGIC AND		CPR.CURRENCY = OPR.CURRENCY
# MAGIC WHERE OPR.CATEGORY IN ('DEP','FLT','ADJ') AND  OPR.NOTE_TYPE IN ('A', 'F') AND OPR.PM_STAT = 0 --and ( opr.CONTRACT = 'ZA002U93A001' or cpr.CONTRACT = 'ZA002U93A001')
# MAGIC GROUP BY CPR.CONTRACT , CPR.CURRENCY,
# MAGIC CPR.EST_AMT,
# MAGIC CPR.MIN_CCY)a

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT --A.CONTRACT
# MAGIC     SUM(A.ADJ_RATE) AS ADJ_RATE
# MAGIC   -- , A.VERSION_NO
# MAGIC    FROM MyMI_pre_gxlp210.dbo_CTR_PM_ADJ A
# MAGIC    INNER JOIN MyMI_pre_gxlp210.dbo_CTR_PM_USAGE U
# MAGIC    ON A.CONTRACT = U.CONTRACT AND A.VERSION_NO = U.VERSION_NO
# MAGIC    WHERE (A.STAT = 0 or U.PM_STAT = 1) and (A.CEDED = 0 or U.PM_STAT = 0)
# MAGIC    AND PM_STAT = 0
# MAGIC  --  GROUP BY A.CONTRACT, A.VERSION_NO

# COMMAND ----------

# MAGIC %sql
# MAGIC select SUM(ADJ_RATE)
# MAGIC FROM MyMI_pre_gxlp.dbo_CTR_PM_ADJ
# MAGIC where pm_stat=0

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT A.CONTRACT,A.VERSION_NO ,A.ADJ_RATE, B.CONTRACT,B.VERSION_NO ,B.25ADJRATE, CASE WHEN A.CONTRACT = B.CONTRACT AND A.VERSION_NO = B.VERSION_NO AND A.ADJ_RATE = B.25ADJRATE THEN 'Matched' else 'Not Matched' end matchstatus
# MAGIC   FROM( 
# MAGIC   SELECT A.CONTRACT
# MAGIC    , SUM(A.ADJ_RATE) AS ADJ_RATE
# MAGIC    , A.VERSION_NO
# MAGIC    FROM MyMI_pre_gxlp210.dbo_CTR_PM_ADJ A
# MAGIC    INNER JOIN MyMI_pre_gxlp210.dbo_CTR_PM_USAGE U
# MAGIC    ON A.CONTRACT = U.CONTRACT AND A.VERSION_NO = U.VERSION_NO
# MAGIC    WHERE (A.STAT = 0 or U.PM_STAT = 1) and (A.CEDED = 0 or U.PM_STAT = 0)
# MAGIC    AND PM_STAT = 0
# MAGIC    GROUP BY A.CONTRACT, A.VERSION_NO
# MAGIC    )A
# MAGIC    FULL OUTER JOIN (select CONTRACT, SUM(ADJ_RATE) 25ADJRATE,VERSION_NO
# MAGIC FROM MyMI_pre_gxlp.dbo_CTR_PM_ADJ
# MAGIC where pm_stat=0
# MAGIC GROUP BY CONTRACT, VERSION_NO) B
# MAGIC ON A.CONTRACT = B.CONTRACT AND A.VERSION_NO =B.VERSION_NO

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM  MYMI_GXLP210_TESTING.Stg20_ORIPlacementAccrualLedger
# MAGIC WHERE CONTRACT25 ='ZP191Q15A001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT a.CONTRACT 
# MAGIC   , a.BROKER_CD 
# MAGIC   , a.REIN_CD
# MAGIC   , a.BROKER_RF
# MAGIC   ,a.period
# MAGIC FROM MyMI_Pre_GXLP.dbo_Accrual a
# MAGIC JOIN MyMI_Abst_GXLP.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = a.CONTRACT
# MAGIC WHERE a.AC_STAT10 NOT IN ( '8987', 'BIL', '4444') -- No reporting of Riverstone data in MyMI
# MAGIC AND a.AC_STAT1 NOT IN ( '8987', 'BIL', '4444') -- No reporting of Riverstone data in MyMI
# MAGIC UNION
# MAGIC SELECT a.CONTRACT 
# MAGIC   , a.BROKER_CD
# MAGIC   , a.REIN_CD
# MAGIC   , a.BROKER_RF
# MAGIC   ,a.period
# MAGIC FROM MyMI_Pre_GXLP.dbo_LEDGER a
# MAGIC JOIN MyMI_Abst_GXLP.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = a.CONTRACT
# MAGIC WHERE COALESCE (a.TRAN_TYPE, '') <> 'CSH'  -- No Cash transactions
# MAGIC AND COALESCE(a.AC_STAT1, '') NOT IN ( 'BIL', '8987', '4444' ) 
# MAGIC )A

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct * ,CASE WHEN GX25.CONTRACT = GX210.CONTRACT210
# MAGIC AND GX25.BROKER_CD = GX210.BROKER_CD210
# MAGIC AND GX25.REIN_CD = GX210.REIN_CD210
# MAGIC AND GX25.BROKER_RF =GX210.BROKER_RF210 THEN 'MATCHING' ELSE 'Not Matching' end MatchStatus FROM (
# MAGIC   SELECT a.CONTRACT 
# MAGIC   , a.BROKER_CD 
# MAGIC   , a.REIN_CD
# MAGIC   , a.BROKER_RF
# MAGIC   ,a.period
# MAGIC FROM MyMI_Pre_GXLP.dbo_Accrual a
# MAGIC JOIN MyMI_Abst_GXLP.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = a.CONTRACT
# MAGIC WHERE a.AC_STAT10 NOT IN ( '8987', 'BIL', '4444') -- No reporting of Riverstone data in MyMI
# MAGIC AND a.AC_STAT1 NOT IN ( '8987', 'BIL', '4444') -- No reporting of Riverstone data in MyMI
# MAGIC UNION
# MAGIC SELECT a.CONTRACT 
# MAGIC   , a.BROKER_CD
# MAGIC   , a.REIN_CD
# MAGIC   , a.BROKER_RF
# MAGIC   ,a.period
# MAGIC FROM MyMI_Pre_GXLP.dbo_LEDGER a
# MAGIC JOIN MyMI_Abst_GXLP.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = a.CONTRACT
# MAGIC WHERE COALESCE (a.TRAN_TYPE, '') <> 'CSH'  -- No Cash transactions
# MAGIC AND COALESCE(a.AC_STAT1, '') NOT IN ( 'BIL', '8987', '4444' )
# MAGIC ) GX25
# MAGIC left OUTER JOIN (
# MAGIC     SELECT a.CONTRACT AS CONTRACT210 
# MAGIC   , a.BROKER_CD AS BROKER_CD210
# MAGIC   , a.REIN_CD AS REIN_CD210
# MAGIC   , a.BROKER_RF AS BROKER_RF210
# MAGIC  ,A.PERIOD AS PERIOD210
# MAGIC FROM MyMI_Pre_GXLP210.dbo_Accrual a
# MAGIC JOIN MyMI_Abst_GXLP210.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = a.CONTRACT
# MAGIC WHERE a.AC_STAT10 NOT IN ( '8987', 'BIL', '4444') -- No reporting of Riverstone data in MyMI
# MAGIC AND a.ENTITY_CD NOT IN ( '8987', 'BIL', '4444') -- No reporting of Riverstone data in MyMI
# MAGIC UNION
# MAGIC SELECT a.CONTRACT AS CONTRACT210 
# MAGIC   , a.BROKER_CD AS BROKER_CD210
# MAGIC   , a.REIN_CD AS REIN_CD210
# MAGIC   , a.BROKER_RF AS BROKER_RF210
# MAGIC  ,A.PERIOD AS PERIOD210
# MAGIC FROM MyMI_Pre_GXLP210.dbo_LEDGER a
# MAGIC JOIN MyMI_Abst_GXLP210.Stg10_ORILatestActiveContract lac
# MAGIC ON lac.CONTRACT = a.CONTRACT
# MAGIC WHERE COALESCE (a.TRAN_TYPE, '') <> 'CSH'  -- No Cash transactions
# MAGIC AND COALESCE(a.ENTITY_CD, '') NOT IN ( 'BIL', '8987', '4444' )
# MAGIC 
# MAGIC )GX210
# MAGIC ON GX25.CONTRACT = GX210.CONTRACT210
# MAGIC AND GX25.BROKER_CD = GX210.BROKER_CD210
# MAGIC AND GX25.REIN_CD = GX210.REIN_CD210
# MAGIC AND GX25.BROKER_RF =GX210.BROKER_RF210
# MAGIC AND GX25.PERIOD = GX210.PERIOD210
# MAGIC where contract in ('ZM110N07A001',
# MAGIC 'ZM110N11A001',
# MAGIC 'ZP191Q15A001',
# MAGIC 'ZP201E17EH01',
# MAGIC 'ZP181M15A001',
# MAGIC 'ZP182M15A001',
# MAGIC 'ZM120W13A001',
# MAGIC 'KE550E16A0001',
# MAGIC 'ZA102Q15A001',
# MAGIC 'ZP201E06C002',
# MAGIC 'ZP201E09G001',
# MAGIC 'ZR201Q20E001',
# MAGIC 'ZP201E08D001',
# MAGIC 'ZM110N04A001',
# MAGIC 'ZQ700J12A001',
# MAGIC 'ZP150A19FV01',
# MAGIC 'ZP201E09B001',
# MAGIC 'ZP201E05A001',
# MAGIC 'ZM110N15A001',
# MAGIC 'ZP201E06A001',
# MAGIC 'JW033H14A001',
# MAGIC 'ZM120W10A001',
# MAGIC 'ZP160A15A001',
# MAGIC 'ZM120W14A001',
# MAGIC 'ZP201E13F001/P',
# MAGIC 'ZP201E17H001',
# MAGIC 'ZP201E13A001/P',
# MAGIC 'ZM110N10A001',
# MAGIC 'ZP899Q13A001',
# MAGIC 'ZP191Q14A001',
# MAGIC 'ZM130G16A001',
# MAGIC 'ZM110N06A001',
# MAGIC 'ZP201E06C001',
# MAGIC 'JH087H16B0001',
# MAGIC 'ZP161A15A001',
# MAGIC 'ZP201E08B001',
# MAGIC 'ZM120W15A001',
# MAGIC 'ZM360P17A001',
# MAGIC 'ZS102S16A001',
# MAGIC 'ZM110N02A001',
# MAGIC 'ZP201E07C002',
# MAGIC 'ZM120W11A001',
# MAGIC 'ZP201E09E001',
# MAGIC 'ZM130G17A001',
# MAGIC 'ZM130G13C001',
# MAGIC 'ZP201E17SH01',
# MAGIC 'ZP201E07B002',
# MAGIC 'ZP201E08C001',
# MAGIC 'ZA102Q17A001/P',
# MAGIC 'ZM110N03A001',
# MAGIC 'ZP193Q17R001',
# MAGIC 'ZP201E13H001/P',
# MAGIC 'ZR252S18A001',
# MAGIC 'ZM120W16A001',
# MAGIC 'ZM110N12A001',
# MAGIC 'ZM120W17A001',
# MAGIC 'KN081S17A0001',
# MAGIC 'ZP201E13B001/P',
# MAGIC 'ZP163A15A001',
# MAGIC 'EC385Z16A0001/1',
# MAGIC 'ZP161A14A001',
# MAGIC 'ZM110N05A001',
# MAGIC 'ZP300J16R001',
# MAGIC 'ZM110N08A001',
# MAGIC 'MB648H16A0001',
# MAGIC 'ZP201E17EI01',
# MAGIC 'ZP201E13D001/P',
# MAGIC 'ZP201E02A001',
# MAGIC 'ZM110N13A001',
# MAGIC 'ZA102Q14A001',
# MAGIC 'ZP201E05C001',
# MAGIC 'ZP201E07B001',
# MAGIC 'ZM110N14A001',
# MAGIC 'ZP201E12G001/P',
# MAGIC 'ZP201E09C001',
# MAGIC 'ZP192Q18A001',
# MAGIC 'ZP201E13C001/P',
# MAGIC 'ZP161A13A001',
# MAGIC 'ZP201E17EC01',
# MAGIC 'ZP201E04A001',
# MAGIC 'ZR201Q18E001',
# MAGIC 'ZM110N09A001',
# MAGIC 'ZP201E07E001',
# MAGIC 'ZP201E17EK01',
# MAGIC 'ZP201E08E001',
# MAGIC 'JW033H15A0001',
# MAGIC 'ZP201E17EG01',
# MAGIC 'ZP163A16A001',
# MAGIC 'JH087H16C0001',
# MAGIC 'ZP201E06B002',
# MAGIC 'KD999S15A0001',
# MAGIC 'ZP201E09A001',
# MAGIC 'ZP201E17SK01',
# MAGIC 'ZM130G15C001',
# MAGIC 'ZP201E13G001/P',
# MAGIC 'ZM130G15A001',
# MAGIC 'ZP201E13E001/P',
# MAGIC 'ZP201E17EF01',
# MAGIC 'ZP201E08A001',
# MAGIC 'ZP201E17ED01',
# MAGIC 'EC385Z16B0001/1',
# MAGIC 'ZP201E07C001',
# MAGIC 'ZP160A14A001',
# MAGIC 'ZP201E05B001',
# MAGIC 'ZP201E09D001',
# MAGIC 'CE524L13A001',
# MAGIC 'ZP201E08F001',
# MAGIC 'ZP150A18D001',
# MAGIC 'ZP201E05C002',
# MAGIC 'ZM120W12A001',
# MAGIC 'KD999S16A0001',
# MAGIC 'ZP201E05B002',
# MAGIC 'ZP201E17EE01',
# MAGIC 'ZP300J15R001',
# MAGIC 'ZP191Q18A001',
# MAGIC 'ZM130G14C001',
# MAGIC 'ZP201E08H001',
# MAGIC 'ZM120W18A001',
# MAGIC 'ZP150Q18A001',
# MAGIC 'ZP201E07A001',
# MAGIC 'ZP201E17EA01',
# MAGIC 'ZP201E09F001',
# MAGIC 'ZP201E12A001/P',
# MAGIC 'ZP201E06B001',
# MAGIC 'JH087H16A0001',
# MAGIC 'ZP800Q17A001',
# MAGIC 'ZA101Q17A001/P',
# MAGIC 'ZP201E17EB01',
# MAGIC 'KE550E17A0001'
# MAGIC )
