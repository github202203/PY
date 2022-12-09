# Databricks notebook source
# MAGIC %run ../../../Datalib/Common/Master

# COMMAND ----------

#Get Entity MetaData Details from DataLib
str_GetEntitiesGXLP210 = """SELECT	S.name+'.'+E.Name EntityName
FROM	METADATA.ENTITY E
JOIN	METADATA.SUBJECT S
ON		E.SUBJECTID = S.SUBJECTID
WHERE	S.NAME LIKE '%GXLP210' AND SUBLAYER IN ('Abstract') and e.EntityID = 5454
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
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM MYMI_PRE_GXLP210.DBO_ACCRUAL
# MAGIC WHERE CONTRACT = 'ZP094E16A001'
# MAGIC AND REIN_CD = 'A11114'
# MAGIC AND PERIOD = 202203
# MAGIC AND AC_STAT3 = 91085

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from mymi_gxlp210_testing.Stg20_ORIUSMSigningTransaction
# MAGIC where matchstatus <> 'MATCHED'
# MAGIC order by ORIContractReference,ORIContractReference25

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC from mymi_pre_gxlp.dbo_ledger
# MAGIC where contract = '60643G17A000'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     PolicyLineRef
# MAGIC    ,BrokerCode
# MAGIC    ,BrokerPseud
# MAGIC    ,BrokerRefOne
# MAGIC    ,OutRIRef
# MAGIC    ,SettCcyISO
# MAGIC    ,ShareNetAmtSettCcy
# MAGIC    ,ActualSettDate
# MAGIC    ,Synd
# MAGIC    ,TrustFundCode
# MAGIC    ,RiskCode
# MAGIC    ,FILCode
# MAGIC    ,FILCode1
# MAGIC    ,FILCode2
# MAGIC    ,FILCode1 AS FILCode4
# MAGIC    ,YOA
# MAGIC    ,SigningNum
# MAGIC    ,SigningDate
# MAGIC    ,SigningVersionNum
# MAGIC    ,CategoryCode
# MAGIC    ,ProcessDate
# MAGIC    ,QualCategory
# MAGIC    ,BusinessCategory
# MAGIC FROM MyMI_Pre_Eclipse.dbo_USMSigningTrans UST
# MAGIC WHERE   InwardsOutwardsInd = 'O'
# MAGIC     AND SigningDate       >= '2015-01-01 00:00:00.000'
# MAGIC     AND IsContra           = 'N'
# MAGIC     AND HasContra          = 'N'
# MAGIC     AND PolicyLineRef = '60643G17A000'
# MAGIC     and SigningNum =51304

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     LPSO_DATE
# MAGIC    ,LPSO_NO
# MAGIC    ,LPSO_VER
# MAGIC    ,MSG_REFNO
# MAGIC    ,PAY_DATE
# MAGIC    ,Entity_CD
# MAGIC    ,Unique_key
# MAGIC    ,LORS_YOA
# MAGIC FROM MyMI_Pre_GXLP210.dbo_Ledger AS GL 
# MAGIC WHERE GL.LPSO_DATE >= '2015-01-01'
# MAGIC and 
# MAGIC --lpso_no = 51304
# MAGIC --and lors_yoa = 2017
# MAGIC --order by 1 desc
# MAGIC /*LPSO_DATE  = UST.SigningDate								
# MAGIC 	   AND GL.LPSO_NO    = UST.SigningNum								
# MAGIC 	   AND GL.LPSO_VER   = UST.SigningVersionNum						
# MAGIC 	   AND GL.PAY_DATE   = UST.ActualSettDate							
# MAGIC 	   AND GL.Entity_CD  = UST.Synd*/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ETL_USMSigningTrans
# MAGIC AS
# MAGIC -- Description:	Get USM Signing Transaction from Eclipse
# MAGIC SELECT  
# MAGIC     PolicyLineRef
# MAGIC    ,BrokerCode
# MAGIC    ,BrokerPseud
# MAGIC    ,BrokerRefOne
# MAGIC    ,OutRIRef
# MAGIC    ,SettCcyISO
# MAGIC    ,ShareNetAmtSettCcy
# MAGIC    ,ActualSettDate
# MAGIC    ,Synd
# MAGIC    ,TrustFundCode
# MAGIC    ,RiskCode
# MAGIC    ,FILCode
# MAGIC    ,FILCode1
# MAGIC    ,FILCode2
# MAGIC    ,FILCode1 AS FILCode4
# MAGIC    ,YOA
# MAGIC    ,SigningNum
# MAGIC    ,SigningDate
# MAGIC    ,SigningVersionNum
# MAGIC    ,CategoryCode
# MAGIC    ,ProcessDate
# MAGIC    ,QualCategory
# MAGIC    ,BusinessCategory
# MAGIC FROM MyMI_Pre_Eclipse.dbo_USMSigningTrans UST
# MAGIC WHERE   InwardsOutwardsInd = 'O'
# MAGIC     AND SigningDate       >= '2015-01-01 00:00:00.000'
# MAGIC     AND IsContra           = 'N'
# MAGIC     AND HasContra          = 'N'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ETL_GXLPLedger
# MAGIC AS
# MAGIC -- Description:	Get Ledger from GXLP
# MAGIC SELECT
# MAGIC     LPSO_DATE
# MAGIC    ,LPSO_NO
# MAGIC    ,LPSO_VER
# MAGIC    ,MSG_REFNO
# MAGIC    ,PAY_DATE
# MAGIC    ,Entity_CD
# MAGIC    ,Unique_key
# MAGIC    ,LORS_YOA
# MAGIC FROM MyMI_Pre_GXLP210.dbo_Ledger AS GL 
# MAGIC WHERE GL.LPSO_DATE >= '2015-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_Transform_ORIUSMSigningTrans
# MAGIC AS
# MAGIC 
# MAGIC -- Description:	Fetches the ORI USM's data from Eclipse and GXLP Ledger
# MAGIC SELECT 
# MAGIC 	 ROW_NUMBER() OVER ( PARTITION BY UST.ActualSettDate, UST.SigningNum, UST.SigningDate, UST.SigningVersionNum,ust.synd 
# MAGIC 				 		 ORDER BY     UST.ActualSettDate, UST.SigningNum, UST.SigningDate, UST.SigningVersionNum,ust.synd ) AS RowNumber 
# MAGIC 	,UST.PolicyLineRef                                                              AS ORIContractReference
# MAGIC 	,UST.BrokerCode                                                                 AS BrokerCode
# MAGIC 	,CAST(IFNULL(UST.BrokerPseud, '') AS VARCHAR(10))                               AS BrokerPseudonym
# MAGIC     ,UST.BrokerRefOne                                                               AS ORIBrokerReference
# MAGIC 	,UST.OutRIRef                                                                   AS LORSORIReference
# MAGIC 	,UST.SettCcyISO                                                                 AS SettlementCurrencyISO
# MAGIC 	,CASE WHEN UST.CategoryCode IN ( 8, 9 ) THEN SUM(UST.ShareNetAmtSettCcy) END	AS ORIUSMPaidClaimSettCcy
# MAGIC 	,CASE WHEN UST.CategoryCode IN ( 6, 7 ) THEN SUM(UST.ShareNetAmtSettCcy) END	AS ORIUSMSignedPremiumSettCcy
# MAGIC 	,IFNULL(UST.ActualSettDate, '9999-12-31')										AS EclipseActualSettlementDate    
# MAGIC 	,UST.Synd 																 		AS Syndicate			    
# MAGIC 	,IFNULL(UST.TrustFundCode, '')													AS TrustFundCode 				    
# MAGIC 	,UST.RiskCode													 				AS RiskCode	    
# MAGIC 	,IFNULL(UST.FILCode,  '')														AS FILCode 								    
# MAGIC 	,IFNULL(UST.FILCode2, '')														AS FILCode2								 				    
# MAGIC 	,IFNULL(UST.FILCode4, '')														AS FILCode4 							 					    
# MAGIC 	,UST.YOA				                                                        AS YOA										 					    
# MAGIC 	,UST.SigningNum 		                                                        AS EclipseSigningNumber												 					    
# MAGIC 	,UST.SigningDate 		                                                        AS EclipseSigningDate												 					    
# MAGIC 	,UST.SigningVersionNum 	                                                        AS EclipseSigningVersionNumber												 					    
# MAGIC 	,UST.CategoryCode		                                                        AS USMCategoryCode 														 				    
# MAGIC 	,UCC.Dsc													                    AS USMCategoryCodeDsc 		    
# MAGIC 	,UST.QualCategory 														 		AS QualCategoryCode		    
# MAGIC 	,UQC.Dsc																		AS QualCategoryCodeDsc									    
# MAGIC 	,UST.BusinessCategory 													 	    AS BusinessCategoryCode
# MAGIC 	,UBC.Dsc																		AS BusinessCategoryCodeDsc 							    
# MAGIC 	,UST.ProcessDate 														 		AS ProcessDate			    								    
# MAGIC 	,2                                                                              AS DimSourceSystemID 
# MAGIC 	,CASE WHEN GL.LPSO_DATE  = UST.SigningDate					    
# MAGIC 		   AND GL.LPSO_NO    = UST.SigningNum						    
# MAGIC 		   AND GL.LPSO_VER   = UST.SigningVersionNum				    
# MAGIC 		   AND GL.PAY_DATE   = UST.ActualSettDate					    
# MAGIC 		   AND GL.Entity_CD  = UST.Synd		   THEN 1 ELSE 0 END					AS AcceptedByGXLPFlag
# MAGIC 
# MAGIC FROM	   vw_ETL_USMSigningTrans                    AS UST
# MAGIC INNER JOIN MyMI_Pre_Eclipse.dbo_USMCategoryCode		 AS UCC  									
# MAGIC 		ON UST.CategoryCode = UCC.CategoryCode				
# MAGIC LEFT JOIN  MyMI_Pre_Eclipse.dbo_USMQualCategoryCode  AS UQC 								
# MAGIC 		ON UQC.QualCategoryCodeID = UST.QualCategory				
# MAGIC LEFT JOIN  MyMI_Pre_Eclipse.dbo_USMBusinessCategory  AS UBC 									
# MAGIC 		ON UBC.Category  = UST.BusinessCategory					
# MAGIC LEFT JOIN  vw_ETL_GXLPLedger                         AS GL 												
# MAGIC 		ON GL.LPSO_DATE  = UST.SigningDate								
# MAGIC 	   AND GL.LPSO_NO    = UST.SigningNum								
# MAGIC 	   AND GL.LPSO_VER   = UST.SigningVersionNum						
# MAGIC 	   AND GL.PAY_DATE   = UST.ActualSettDate							
# MAGIC 	   AND GL.Entity_CD  = UST.Synd
# MAGIC 
# MAGIC WHERE   UST.CategoryCode IN ( 6, 7, 8, 9 )
# MAGIC 
# MAGIC GROUP BY UST.PolicyLineRef
# MAGIC 		,UST.BrokerCode
# MAGIC         ,IFNULL(UST.BrokerPseud, '')
# MAGIC 		,UST.BrokerRefOne
# MAGIC 		,UST.OutRIRef
# MAGIC 		,UST.SettCcyISO
# MAGIC 		,UST.ActualSettDate
# MAGIC 		,UST.Synd
# MAGIC 		,UST.TrustFundCode
# MAGIC 		,UST.RiskCode
# MAGIC 		,UST.FILCode
# MAGIC 		,IFNULL(UST.FILCode2, '')
# MAGIC 		,IFNULL(UST.FILCode4, '')
# MAGIC 		,UST.YOA
# MAGIC 		,UST.SigningNum
# MAGIC 		,UST.SigningDate
# MAGIC 		,UST.SigningVersionNum
# MAGIC 		,UST.CategoryCode
# MAGIC 		,UCC.Dsc
# MAGIC 		,UST.ProcessDate
# MAGIC 		,UST.QualCategory
# MAGIC 		,UQC.Dsc
# MAGIC 		,UST.BusinessCategory
# MAGIC 		,UBC.Dsc
# MAGIC 		,CASE WHEN GL.LPSO_DATE = UST.SigningDate
# MAGIC 			   AND GL.LPSO_NO   = UST.SigningNum
# MAGIC 			   AND GL.LPSO_VER  = UST.SigningVersionNum
# MAGIC 			   AND GL.PAY_DATE  = UST.ActualSettDate
# MAGIC 			   AND GL.Entity_CD = UST.Synd  THEN 1 ELSE 0 END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT 
# MAGIC     RowNumber
# MAGIC    ,ORIContractReference
# MAGIC    ,BrokerCode
# MAGIC    ,BrokerPseudonym
# MAGIC    ,ORIBrokerReference
# MAGIC    ,LORSORIReference
# MAGIC    ,SettlementCurrencyISO
# MAGIC    ,CAST(IFNULL(ORIUSMPaidClaimSettCcy, 0)     AS decimal(19,4)) AS ORIUSMPaidClaimSettCcy 
# MAGIC    ,CAST(IFNULL(ORIUSMSignedPremiumSettCcy, 0) AS decimal(19,4)) AS ORIUSMSignedPremiumSettCcy
# MAGIC    ,CAST(EclipseActualSettlementDate AS date ) AS EclipseActualSettlementDate
# MAGIC    ,Syndicate
# MAGIC    ,TrustFundCode
# MAGIC    ,RiskCode
# MAGIC    ,FILCode
# MAGIC    ,FILCode2
# MAGIC    ,FILCode4
# MAGIC    ,YOA
# MAGIC    ,EclipseSigningNumber
# MAGIC    ,CAST(EclipseSigningDate AS date ) AS EclipseSigningDate
# MAGIC    ,EclipseSigningVersionNumber
# MAGIC    ,CAST(USMCategoryCode AS int ) AS USMCategoryCode
# MAGIC    ,USMCategoryCodeDsc
# MAGIC    ,QualCategoryCode
# MAGIC    ,QualCategoryCodeDsc
# MAGIC    ,BusinessCategoryCode
# MAGIC    ,BusinessCategoryCodeDsc
# MAGIC    ,ProcessDate
# MAGIC    ,DimSourceSystemID
# MAGIC    ,CAST(AcceptedByGXLPFlag AS boolean ) AS AcceptedByGXLPFlag
# MAGIC FROM  vw_Transform_ORIUSMSigningTrans
# MAGIC WHERE RowNumber = 1
# MAGIC and ORIContractReference ='ZM500Z17A00A'
# MAGIC         and EclipseSigningNumber=51304

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Description:	Fetches the ORI USM's data from Eclipse and GXLP Ledger
# MAGIC SELECT 
# MAGIC 	 ROW_NUMBER() OVER ( PARTITION BY UST.ActualSettDate, UST.SigningNum, UST.SigningDate, UST.SigningVersionNum
# MAGIC 				 		 ORDER BY     UST.ActualSettDate, UST.SigningNum, UST.SigningDate, UST.SigningVersionNum) AS RowNumber 
# MAGIC 	,UST.PolicyLineRef                                                              AS ORIContractReference
# MAGIC 	,UST.BrokerCode                                                                 AS BrokerCode
# MAGIC 	,CAST(IFNULL(UST.BrokerPseud, '') AS VARCHAR(10))                               AS BrokerPseudonym
# MAGIC     ,UST.BrokerRefOne                                                               AS ORIBrokerReference
# MAGIC 	,UST.OutRIRef                                                                   AS LORSORIReference
# MAGIC 	,UST.SettCcyISO                                                                 AS SettlementCurrencyISO
# MAGIC 	,CASE WHEN UST.CategoryCode IN ( 8, 9 ) THEN SUM(UST.ShareNetAmtSettCcy) END	AS ORIUSMPaidClaimSettCcy
# MAGIC 	,CASE WHEN UST.CategoryCode IN ( 6, 7 ) THEN SUM(UST.ShareNetAmtSettCcy) END	AS ORIUSMSignedPremiumSettCcy
# MAGIC 	,IFNULL(UST.ActualSettDate, '9999-12-31')										AS EclipseActualSettlementDate    
# MAGIC 	,UST.Synd 																 		AS Syndicate			    
# MAGIC 	,IFNULL(UST.TrustFundCode, '')													AS TrustFundCode 				    
# MAGIC 	,UST.RiskCode													 				AS RiskCode	    
# MAGIC 	,IFNULL(UST.FILCode,  '')														AS FILCode 								    
# MAGIC 	,IFNULL(UST.FILCode2, '')														AS FILCode2								 				    
# MAGIC 	,IFNULL(UST.FILCode4, '')														AS FILCode4 							 					    
# MAGIC 	,UST.YOA				                                                        AS YOA										 					    
# MAGIC 	,UST.SigningNum 		                                                        AS EclipseSigningNumber												 					    
# MAGIC 	,UST.SigningDate 		                                                        AS EclipseSigningDate												 					    
# MAGIC 	,UST.SigningVersionNum 	                                                        AS EclipseSigningVersionNumber												 					    
# MAGIC 	,UST.CategoryCode		                                                        AS USMCategoryCode 														 				    
# MAGIC 	,UCC.Dsc													                    AS USMCategoryCodeDsc 		    
# MAGIC 	,UST.QualCategory 														 		AS QualCategoryCode		    
# MAGIC 	,UQC.Dsc																		AS QualCategoryCodeDsc									    
# MAGIC 	,UST.BusinessCategory 													 	    AS BusinessCategoryCode
# MAGIC 	,UBC.Dsc																		AS BusinessCategoryCodeDsc 							    
# MAGIC 	,UST.ProcessDate 														 		AS ProcessDate			    								    
# MAGIC 	,2                                                                              AS DimSourceSystemID 
# MAGIC 	,CASE WHEN GL.LPSO_DATE  = UST.SigningDate					    
# MAGIC 		   AND GL.LPSO_NO    = UST.SigningNum						    
# MAGIC 		   AND GL.LPSO_VER   = UST.SigningVersionNum				    
# MAGIC 		   AND GL.PAY_DATE   = UST.ActualSettDate					    
# MAGIC 		   AND GL.Entity_CD  = UST.Synd		   THEN 1 ELSE 0 END					AS AcceptedByGXLPFlag
# MAGIC 
# MAGIC FROM	   vw_ETL_USMSigningTrans                    AS UST
# MAGIC INNER JOIN MyMI_Pre_Eclipse.dbo_USMCategoryCode		 AS UCC  									
# MAGIC 		ON UST.CategoryCode = UCC.CategoryCode				
# MAGIC LEFT JOIN  MyMI_Pre_Eclipse.dbo_USMQualCategoryCode  AS UQC 								
# MAGIC 		ON UQC.QualCategoryCodeID = UST.QualCategory				
# MAGIC LEFT JOIN  MyMI_Pre_Eclipse.dbo_USMBusinessCategory  AS UBC 									
# MAGIC 		ON UBC.Category  = UST.BusinessCategory					
# MAGIC LEFT JOIN  vw_ETL_GXLPLedger                         AS GL 												
# MAGIC 		ON GL.LPSO_DATE  = cast(UST.SigningDate as date)								
# MAGIC 	   AND GL.LPSO_NO    = UST.SigningNum								
# MAGIC 	   AND GL.LPSO_VER   = UST.SigningVersionNum						
# MAGIC 	   AND GL.PAY_DATE   = UST.ActualSettDate							
# MAGIC 	   AND GL.Entity_CD  = UST.Synd
# MAGIC 
# MAGIC WHERE   UST.CategoryCode IN ( 6, 7, 8, 9 )
# MAGIC         and ust.PolicyLineRef ='60643G17A000'
# MAGIC        -- and SigningNum=51304
# MAGIC GROUP BY UST.PolicyLineRef
# MAGIC 		,UST.BrokerCode
# MAGIC         ,IFNULL(UST.BrokerPseud, '')
# MAGIC 		,UST.BrokerRefOne
# MAGIC 		,UST.OutRIRef
# MAGIC 		,UST.SettCcyISO
# MAGIC 		,UST.ActualSettDate
# MAGIC 		,UST.Synd
# MAGIC 		,UST.TrustFundCode
# MAGIC 		,UST.RiskCode
# MAGIC 		,UST.FILCode
# MAGIC 		,IFNULL(UST.FILCode2, '')
# MAGIC 		,IFNULL(UST.FILCode4, '')
# MAGIC 		,UST.YOA
# MAGIC 		,UST.SigningNum
# MAGIC 		,UST.SigningDate
# MAGIC 		,UST.SigningVersionNum
# MAGIC 		,UST.CategoryCode
# MAGIC 		,UCC.Dsc
# MAGIC 		,UST.ProcessDate
# MAGIC 		,UST.QualCategory
# MAGIC 		,UQC.Dsc
# MAGIC 		,UST.BusinessCategory
# MAGIC 		,UBC.Dsc
# MAGIC 		,CASE WHEN GL.LPSO_DATE = UST.SigningDate
# MAGIC 			   AND GL.LPSO_NO   = UST.SigningNum
# MAGIC 			   AND GL.LPSO_VER  = UST.SigningVersionNum
# MAGIC 			   AND GL.PAY_DATE  = UST.ActualSettDate
# MAGIC 			   AND GL.Entity_CD = UST.Synd  THEN 1 ELSE 0 END

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from  mymi_abst_gxlp210.Stg20_ORIUSMSigningTransaction
# MAGIC where ORIContractReference ='ZM500Z17A00A'--and EclipseSigningNumber = 51304

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from  mymi_abst_gxlp.Stg20_ORIUSMSigningTransaction
# MAGIC where ORIContractReference ='ZM500Z17A003'
