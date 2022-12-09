# Databricks notebook source
# MAGIC %run ../../../Datalib/Common/Master

# COMMAND ----------

#Get Entity MetaData Details from DataLib
str_GetEntitiesGXLP210 = """SELECT	S.name+'.'+E.Name EntityName
FROM	METADATA.ENTITY E
JOIN	METADATA.SUBJECT S
ON		E.SUBJECTID = S.SUBJECTID
WHERE	S.NAME LIKE '%GXLP210' AND SUBLAYER IN ('Abstract','Transformed') --and e.EntityID = 5483
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
    var_df = spark.sql("SELECT *,CASE WHEN GXLP25 IS NULL THEN 'Missing 2.5' WHEN GXLP210 IS NULL THEN 'Missing 2.10' ELSE 'MATCHED' END MatchStatus FROM GXLPJoinOutput")
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
    str_Query = f""" SELECT count(*) as cnt,MatchStatus FROM MYMI_GXLP210_TESTING.{str_Ename[1]} GROUP BY MatchStatus """
    df_Count = spark.sql(str_Query)
    df_Count.withColumn("TableName",lit(str_Ename[1])).show()
    #df_test.show()
    str_FilePath = f"dbfs:/mnt/main/Transformed/MyMI_GXLP210_Testing/Internal/Datacheckscount"
    #var_df.write.mode('overwrite').parquet(str_FilePath)
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
# MAGIC SELECT *
# MAGIC from  mymi_gxlp210_testing.Stg60_ORIContract
# MAGIC where matchstatus <> 'MATCHED'
# MAGIC order by GXLPContractReference,GXLPContractReference25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM  MYMI_ABST_GXLP.STG30_ORICONTRACTBASE
# MAGIC WHERE CONTRACTREF= 'EB501F17A0001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.CONTRACT
# MAGIC , SUM(A.ADJ_RATE) AS ADJ_RATE
# MAGIC , A.VERSION_NO
# MAGIC FROM MYMI_PRE_GXLP210.DBO_CTR_PM_ADJ A
# MAGIC INNER JOIN (SELECT  MAX(VERSION_NO) VERSION_NO,PM_STAT,CONTRACT
# MAGIC FROM		MYMI_PRE_GXLP210.DBO_CTR_PM_USAGE
# MAGIC GROUP BY CONTRACT,PM_STAT) U
# MAGIC ON A.CONTRACT = U.CONTRACT AND A.VERSION_NO = U.VERSION_NO
# MAGIC LEFT JOIN MYMI_ABST_GXLP210.Stg10_ORILatestActiveContract LAC
# MAGIC ON lac.CONTRACT = A.CONTRACT
# MAGIC AND lac.VERSION_NO = A.VERSION_NO
# MAGIC WHERE (A.STAT = 0 or U.PM_STAT = 1) and (A.CEDED = 0 or U.PM_STAT = 0)
# MAGIC AND PM_STAT = 0 AND A.CONTRACT= 'EB501F17A0001'
# MAGIC GROUP BY A.CONTRACT, A.VERSION_NO

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.CONTRACT
# MAGIC , SUM(A.ADJ_RATE) AS ADJ_RATE
# MAGIC , A.VERSION_NO
# MAGIC FROM MYMI_PRE_GXLP.DBO_CTR_PM_ADJ A
# MAGIC LEFT JOIN MYMI_ABST_GXLP210.Stg10_ORILatestActiveContract LAC
# MAGIC ON lac.CONTRACT = A.CONTRACT
# MAGIC AND lac.VERSION_NO = A.VERSION_NO
# MAGIC WHERE 1=1--(A.STAT = 0 or U.PM_STAT = 1) and (A.CEDED = 0 or U.PM_STAT = 0)
# MAGIC AND PM_STAT = 0 AND A.CONTRACT= 'EB501F17A0001'
# MAGIC GROUP BY A.CONTRACT, A.VERSION_NO

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM ( SELECT CONTRACT
# MAGIC   , ocp.CURRENCY
# MAGIC   , ocp.MD_PM_CCY AS ORIDepositPremiumSettCcy
# MAGIC   , ocp.EPI_CCY AS ORIEstimatedPremiumSettCcy
# MAGIC   , ocp.AD_PM_CCY AS ORIAdjustmentPremiumSettCcy
# MAGIC   , ocp.MIN_CCY AS ORIMinimumPremiumSettCcy
# MAGIC FROM mymi_pre_gxlp.DBO_CTR_PM OCP)A

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, CASE WHEN A.CONTRACT = B.CONTRACT AND A.CURRENCY = B.CURRENCY AND A.ORIDepositPremiumSettCcy = B.ORIDepositPremiumSettCcy AND A.ORIEstimatedPremiumSettCcy = B.ORIEstimatedPremiumSettCcy AND A.ORIAdjustmentPremiumSettCcy  = B.ORIAdjustmentPremiumSettCcy AND A.ORIMinimumPremiumSettCcy = B.ORIMinimumPremiumSettCcy THEN 'Matched' ELSE 'Not Matched' END MatchStatus
# MAGIC FROM (
# MAGIC         SELECT  COALESCE(CPR.CONTRACT,OPR.CONTRACT) Contract ,
# MAGIC                 COALESCE(CPR.CURRENCY,OPR.Currency) CURRENCY,
# MAGIC             COALESCE(SUM(CASE WHEN OPR.CATEGORY IN ('DEP','FLT') THEN AMT_CCY ELSE 0 ENd),0) AS ORIDepositPremiumSettCcy,
# MAGIC             COALESCE(CPR.EST_AMT,0)  ORIEstimatedPremiumSettCcy,
# MAGIC             COALESCE(SUM(CASE WHEN OPR.CATEGORY = 'ADJ' THEN AMT_CCY ELSE 0 END),0) AS ORIAdjustmentPremiumSettCcy,
# MAGIC             COALESCE(CPR.MIN_CCY,0) ORIMinimumPremiumSettCcy
# MAGIC         FROM MyMI_pre_gxlp210.dbo_out_prm  OPR
# MAGIC         FULL OUTER join MyMI_pre_gxlp210.dbo_CTR_PRM CPR
# MAGIC         ON	CPR.CONTRACT = OPR.CONTRACT
# MAGIC         AND		CPR.CURRENCY = OPR.CURRENCY
# MAGIC         WHERE OPR.CATEGORY IN ('DEP','FLT','ADJ') AND  OPR.NOTE_TYPE IN ('A', 'F') AND OPR.PM_STAT = 0 --and ( opr.CONTRACT = 'ZP300J20A001' or cpr.CONTRACT = 'ZP300J20A001')
# MAGIC         GROUP BY CPR.CONTRACT , CPR.CURRENCY,OPR.CONTRACT , OPR.CURRENCY,
# MAGIC         CPR.EST_AMT,
# MAGIC         CPR.MIN_CCY
# MAGIC ) A
# MAGIC FULL OUTER JOIN (
# MAGIC     SELECT CONTRACT
# MAGIC   , ocp.CURRENCY
# MAGIC   , COALESCE(ocp.MD_PM_CCY,0) AS ORIDepositPremiumSettCcy
# MAGIC   , COALESCE(ocp.EPI_CCY,0) AS ORIEstimatedPremiumSettCcy
# MAGIC   , COALESCE(ocp.AD_PM_CCY,0) AS ORIAdjustmentPremiumSettCcy
# MAGIC   ,COALESCE(ocp.MIN_CCY,0) AS ORIMinimumPremiumSettCcy
# MAGIC FROM mymi_pre_gxlp.DBO_CTR_PM OCP
# MAGIC ) B
# MAGIC ON A.CONTRACT = B.CONTRACT
# MAGIC AND A.CURRENCY = B.CURRENCY
# MAGIC ORDER BY A.CONTRACT,B.CONTRACT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_pre_gxlp210.DBO_CTR_PRM
# MAGIC where contract = 'ZV500A20B002/R'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COALESCE(EST_AMT,0)
# MAGIC FROM mymi_pre_gxlp210.DBO_CTR_PRM
# MAGIC where contract = 'X0001J011000'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_gxlp210_testing.Stg70_ORIApplicablePremium
# MAGIC WHERE MatchStatus <> 'MATCHED'
# MAGIC ORDER BY CONTRACTref,CONTRACTref25

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_pre_gxlp210.DBO_CTR_HDR
# MAGIC WHERE CONTRACT = 'EC770D10A001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct A.CONTRACT,A.CURRENCY,A.VERSION_NO,SUM(A.ORIAdjustmentPremiumSettCcy) AD_PM_CCY,SUM(A.ORIDepositPremiumSettCcy) MD_PM_CCY,SUM(A.EST_AMT) EPI_CCY,SUM(A.MIN_CCY) MIN_CCY
# MAGIC FROM (
# MAGIC SELECT CONTRACT,CURRENCY,VERSION_NO,SUM(CASE WHEN OPR.CATEGORY IN ('DEP','FLT') THEN AMT_CCY ELSE 0 END) AS ORIDepositPremiumSettCcy,SUM(CASE WHEN OPR.CATEGORY = 'ADJ' THEN AMT_CCY ELSE 0 END) AS ORIAdjustmentPremiumSettCcy,NULL EST_AMT,NULL MIN_CCY
# MAGIC FROM myMI_pre_gxlp210.dbo_OUT_PRM OPR
# MAGIC WHERE OPR.CATEGORY IN ('DEP','FLT','ADJ') AND OPR.NOTE_TYPE IN ('A', 'F') AND OPR.PM_STAT = 0
# MAGIC GROUP BY CONTRACT,CURRENCY,VERSION_NO
# MAGIC UNION
# MAGIC SELECT CONTRACT,CURRENCY,VERSION_NO,NULL MD_PM_CCY,NULL AD_PM_CCY,EST_AMT EPI_CCY,MIN_CCY
# MAGIC FROM myMI_pre_gxlp210.dbo_CTR_PRM
# MAGIC )A  WHERE CONTRACT = 'EC770D10A001'
# MAGIC GROUP BY A.CONTRACT,A.CURRENCY,A.VERSION_NO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM myMI_pre_gxlp210.dbo_out_PrM
# MAGIC WHERE CONTRACT = 'EC770D10A001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_abst_gxlp210.Stg20_ORIApplicablePremiumBase
# MAGIC WHERE CONTRACT = 'EC770D10A001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_abst_gxlp210.Stg10_ORILatestActiveContract
# MAGIC WHERE CONTRACT = 'EC770D10A001'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mymi_gxlp210_testing.DIMBROKER
# MAGIC --WHERE JOINFLAG <> 'MATCHED'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT PERIOD
# MAGIC FROM MYMI_PRE_GXLP.DBO_ACCRUAL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC from mymi_gxlp210_testing.stg40_ORIClaimSubevent
# MAGIC where matchstatus <> 'MATCHED'
