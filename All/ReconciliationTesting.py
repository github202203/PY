# Databricks notebook source
# MAGIC %sql
# MAGIC   SELECT
# MAGIC     SCF.VelocityClaimCaseNumber AS ClaimID,
# MAGIC     IFNULL(PL.EclipsePolicyLIneID, 0) + 1000000 AS ClaimLineID,
# MAGIC     CAST(
# MAGIC       CONCAT(
# MAGIC         CAST(SCF.VelocityClaimCaseNumber AS string),
# MAGIC         CAST(
# MAGIC           ROW_NUMBER() OVER (
# MAGIC             PARTITION BY IFNULL(PL.EclipsePolicyLIneID, 0)
# MAGIC             ORDER BY
# MAGIC               SCF.TransactionDate,
# MAGIC               SCF.ClaimRef,
# MAGIC               PL.EclipsePolicyID,
# MAGIC               PL.EclipsePolicyLIneID,
# MAGIC               movementTypeId,
# MAGIC               SCF.VelocityClaimTransactionSequence,
# MAGIC               SCF.SplitCategory_ID
# MAGIC           ) AS string
# MAGIC         )
# MAGIC       ) AS INT
# MAGIC     ) AS MovementID,
# MAGIC     IFNULL(PL.EclipsePolicyLIneID, 0) + 1000000 AS ClaimLineShareID,
# MAGIC     IFNULL(PL.EclipsePolicyID, 0) AS EclipsePolicyID,
# MAGIC     IFNULL(PL.EclipsePolicyLIneID, 0) AS EclipsePolicyLIneID,
# MAGIC     IFNULL(CAST(SCF.ClaimRef AS string), '') AS ClaimRef,
# MAGIC     CAST(MovementRef AS string) AS MovementRef,
# MAGIC     movementTypeId,
# MAGIC     VelocityClaimTransactionSequence,
# MAGIC     CAST(SCF.TransactionDate AS DATE) AS DimTransactionDateID,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY SCF.MovementRef, PL.EclipsePolicyLIneID, SCF.movementTypeId,
# MAGIC                                     SCF.VelocityClaimTransactionSequence, SCF.SplitCategory_ID ORDER BY SCF.TransactionDate, SCF.valueAtBroker_MVMT ) AS RowNumber,
# MAGIC     CASE WHEN PL.LineStatus = 'Signed' THEN PL.SignedLinePct / 100
# MAGIC          WHEN PL.LineStatus = 'Written' THEN PL.WrittenLine / 100
# MAGIC          ELSE 0
# MAGIC     END AS LinePct,    
# MAGIC     UPPER(CAST(IFNULL(SCF.Currency, '') AS string)) AS OriginalCcyISO,
# MAGIC     UPPER(CAST(IFNULL(SCF.SettlementCurrency, '') AS string)) AS SettlementCcyISO,
# MAGIC     IFNULL(CASE WHEN SCF.movementTypeId = 'PAYMENT' THEN SCF.valueAtBroker_MVMT ELSE 0 END, 0) AS PaidClaimOrigCcy,
# MAGIC     IFNULL(CASE WHEN SCF.movementTypeId = 'PAYMENT' THEN SCF.valueAtBrokerSett_MVMT ELSE 0 END, 0) AS PaidClaimSettCcy,    
# MAGIC     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker_MVMT ELSE 0 END, 0) AS OutstandingClaimOrigCcyMovement
# MAGIC   FROM
# MAGIC     MyMI_Abst_SequelClaims.stg10_ClaimTransactions SCF
# MAGIC     JOIN (
# MAGIC       SELECT
# MAGIC         *
# MAGIC       FROM
# MAGIC         MyMI_Abst_SequelClaims.stg20_ClaimDetails
# MAGIC       WHERE
# MAGIC         IsEclipse = 1
# MAGIC     ) SCD ON UPPER(SCD.Claimref) = UPPER(SCF.ClaimRef)
# MAGIC     JOIN MyMI_Abst_Eclipse.stg10_Policy P ON P.PolicyId = SCF.PolicySequence
# MAGIC     LEFT JOIN MyMI_Abst_Eclipse.stg20_PolicyLine PL ON PL.EclipsePolicyID = P.PolicyId
# MAGIC   WHERE
# MAGIC     UPPER(PL.LineStatus) IN ('SIGNED', 'WRITTEN')
# MAGIC     AND SCF.VelocityClaimCaseNumber = 1011243
# MAGIC --     AND MovementRef = 'AE0419'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC SELECT              SCF.VelocityClaimCaseNumber AS ClaimID ,
# MAGIC                     IFNULL(PL.EclipsePolicyLIneID, 0) + 1000000 AS ClaimLineID ,
# MAGIC                     CAST(CONCAT(CAST(SCF.VelocityClaimCaseNumber AS string)
# MAGIC                     , CAST(ROW_NUMBER() OVER ( PARTITION BY IFNULL(PL.EclipsePolicyLIneID, 0) ORDER BY SCF.TransactionDate, SCF.ClaimRef, PL.EclipsePolicyID, PL.EclipsePolicyLIneID, movementTypeId, SCF.VelocityClaimTransactionSequence,SCF.SplitCategory_ID ) AS string)) AS INT) AS MovementID  ,
# MAGIC                     IFNULL(PL.EclipsePolicyLIneID, 0) + 1000000 AS ClaimLineShareID ,
# MAGIC                     IFNULL(PL.EclipsePolicyID, 0) AS EclipsePolicyID ,
# MAGIC                     IFNULL(PL.EclipsePolicyLIneID, 0) AS EclipsePolicyLIneID ,
# MAGIC                     '' AS FILcode2 ,
# MAGIC                     '' AS FILCode4 ,
# MAGIC                     CAST(IFNULL(PL.ExternalSyndicate, 'UNK') AS string) AS Syndicate ,
# MAGIC                     CAST(SCF.TransactionDate AS DATE) AS DimTransactionDateID ,
# MAGIC                     CAST(IFNULL(SCF.EarlyRIIndicator, '') AS string) EarlyRIIndicator ,
# MAGIC                     '' AS ClaimTransactionCode ,
# MAGIC                     IFNULL(CAST(SCF.ClaimRef AS string), '') AS ClaimRef ,
# MAGIC                     IFNULL(CAST('' AS string), '') AS BureauClaimRef ,
# MAGIC                     IFNULL(LEFT(CAST(SCF.MovementRef AS string),2), '') AS ClaimTransactionReference ,
# MAGIC                     '' AS BureauClaimTransactionReference ,
# MAGIC                     CAST(MovementRef AS string) AS MovementRef ,
# MAGIC                     CAST(IFNULL(RiskCode, '') AS string) AS RiskCode ,
# MAGIC                     '' AS TrustFundCode ,
# MAGIC                     IFNULL(CAST(UPPER(LTRIM(RTRIM(PL.ExternalSyndicate))) AS string), 'UNK') AS LegalEntity ,
# MAGIC                     IFNULL(CAST(UPPER(LTRIM(RTRIM(PL.LineStatus))) AS string), 'UNK') AS PolicyStatus ,
# MAGIC                     '' AS LatestMovementIndicator ,
# MAGIC                     UPPER(CAST(IFNULL(SCF.Currency, '') AS string)) AS OriginalCcyISO ,
# MAGIC                     UPPER(CAST(IFNULL(SCF.SettlementCurrency, '') AS string)) AS SettlementCcyISO ,
# MAGIC                     IFNULL(SCF.OriginalToSettlementExchangeRate, 0) AS OriginalToSettlementExchangeRate ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'PAYMENT' THEN SCF.valueAtBroker_MVMT
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS PaidClaimOrigCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'PAYMENT' THEN SCF.valueAtBrokerSett_MVMT
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS PaidClaimSettCcy ,
# MAGIC                     0 AS PaidLossFundOrigCcy ,
# MAGIC                     0 AS PaidLossFundSettCcy ,
# MAGIC                     0 AS PendingLocOrigCcy ,
# MAGIC                     0 AS PendingLocSettCcy ,
# MAGIC                     0 AS PendingPaidClaimOrigCcy ,
# MAGIC                     0 AS PendingPaidSettCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS AdvisedOutstandingClaimOrigCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBrokerSett
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS OutstandingSettCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker_MVMT
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS OutstandingClaimOrigCcyMovement ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBrokerSett_MVMT
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS OutstandingSettCcyMovement ,
# MAGIC                     0 AS OurAdditionalOutstandingClaimOrigCcy ,
# MAGIC                     0 AS OurAdditionalOutstandingSettCcy ,
# MAGIC                     CAST(CASE WHEN SCF.VelocityClaimTransactionSequence = 1
# MAGIC                                    AND SCF.movementTypeId = 'Oustanding' THEN 'Y'
# MAGIC                               ELSE 'N'
# MAGIC                          END AS string) FirstOSMovementIndicator ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker_MVMT
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS AdvisedOutstandingClaimOrigCcyMovement ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker_MVMT
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS OurOutstandingClaimOrigCcyMovement ,
# MAGIC                     IFNULL(CASE WHEN SCF.movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker
# MAGIC                                 ELSE 0
# MAGIC                            END, 0) AS OurOutstandingClaimOrigCcy ,
# MAGIC                     0 AS OurAdditionalOutstandingOrigCcyMovement ,
# MAGIC                     0 AS OurAdditionalOutstandingSettCcyMovement ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 0
# MAGIC                                      AND SCF.ExpenseIndemnity = 'E'
# MAGIC                                      AND movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker
# MAGIC                            END, 0) AS ExpenseOutstandingClaimOrigCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 0
# MAGIC                                      AND SCF.ExpenseIndemnity = 'E'
# MAGIC                                      AND movementTypeId = 'PAYMENT' THEN SCF.valueAtBroker_MVMT
# MAGIC                            END, 0) AS ExpensePaidClaimOrigCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 0
# MAGIC                                      AND SCF.ExpenseIndemnity = 'E'
# MAGIC                                      AND movementTypeId = 'PAYMENT' THEN SCF.valueAtBrokerSett_MVMT
# MAGIC                            END, 0) AS ExpensePaidClaimSettCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 0
# MAGIC                                      AND SCF.ExpenseIndemnity = 'I'
# MAGIC                                      AND movementTypeId = 'PAYMENT' THEN SCF.valueAtBroker_MVMT
# MAGIC                            END, 0) AS IndemnityPaidClaimOrigCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 0
# MAGIC                                      AND SCF.ExpenseIndemnity = 'I'
# MAGIC                                      AND movementTypeId = 'PAYMENT' THEN SCF.valueAtBrokerSett_MVMT
# MAGIC                            END, 0) AS IndemnityPaidClaimSettCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 0
# MAGIC                                      AND SCF.ExpenseIndemnity = 'I'
# MAGIC                                      AND movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker
# MAGIC                            END, 0) AS IndemnityOutstandingClaimOrigCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 1
# MAGIC                                      AND SCF.ExpenseIndemnity IN ( 'E', 'I' )
# MAGIC                                      AND movementTypeId = 'PAYMENT' THEN SCF.valueAtBroker_MVMT
# MAGIC                            END, 0) AS RecoveryPaidClaimOrigCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 1
# MAGIC                                      AND SCF.ExpenseIndemnity IN ( 'E', 'I' )
# MAGIC                                      AND movementTypeId = 'PAYMENT' THEN SCF.valueAtBrokerSett_MVMT
# MAGIC                            END, 0) AS RecoveryPaidClaimSettCcy ,
# MAGIC                     IFNULL(CASE WHEN SCF.Recovery = 1
# MAGIC                                      AND SCF.ExpenseIndemnity IN ( 'E', 'I' )
# MAGIC                                      AND movementTypeId = 'OUTSTANDING' THEN SCF.valueAtBroker
# MAGIC                            END, 0) AS RecoveryOutstandingClaimOrigCcy ,
# MAGIC  
# MAGIC                     ROW_NUMBER() OVER ( PARTITION BY SCF.MovementRef, PL.EclipsePolicyLIneID, SCF.movementTypeId,
# MAGIC                                         SCF.VelocityClaimTransactionSequence, SCF.SplitCategory_ID ORDER BY SCF.TransactionDate ) AS RowNumber ,
# MAGIC                     IFNULL(P.YOA, 0) AS YOA ,
# MAGIC                     25 AS SourceSystemID ,
# MAGIC                     CAST('P' AS string) AS RowStatus ,
# MAGIC                     CAST('I' AS string) AS RowErrorCode ,
# MAGIC                     CASE WHEN PL.LineStatus = 'Signed' THEN PL.SignedLinePct / 100
# MAGIC                          WHEN PL.LineStatus = 'Written' THEN PL.WrittenLine / 100
# MAGIC                          ELSE 0
# MAGIC                     END AS LinePct ,
# MAGIC                     SCF.movementTypeId ,
# MAGIC                     SCF.VelocityClaimTransactionSequence
# MAGIC 
# MAGIC FROM                MyMI_Abst_SequelClaims.stg10_ClaimTransactions SCF
# MAGIC                     JOIN (SELECT * 
# MAGIC                               FROM MyMI_Abst_SequelClaims.stg20_ClaimDetails 
# MAGIC                               WHERE IsEclipse = 1 )SCD
# MAGIC                     ON UPPER(SCD.Claimref) = UPPER(SCF.ClaimRef)
# MAGIC                     JOIN MyMI_Abst_Eclipse.stg10_Policy P
# MAGIC                     ON P.PolicyId = SCF.PolicySequence
# MAGIC                     LEFT JOIN MyMI_Abst_Eclipse.stg20_PolicyLine PL
# MAGIC                     ON PL.EclipsePolicyID = P.PolicyId
# MAGIC 					WHERE   UPPER(PL.LineStatus) IN ( 'SIGNED','WRITTEN' )
# MAGIC                     )
# MAGIC SELECT 
# MAGIC ClaimID,
# MAGIC ClaimLineID,
# MAGIC MovementID,
# MAGIC ClaimLineShareID,
# MAGIC EclipsePolicyID,
# MAGIC EclipsePolicyLIneID,
# MAGIC FILcode2,
# MAGIC FILCode4,
# MAGIC Syndicate,
# MAGIC DimTransactionDateID,
# MAGIC cast(99991200 as string) + '00' as ProcessPeriod,
# MAGIC EarlyRIIndicator,
# MAGIC ClaimTransactionCode,
# MAGIC ClaimRef,
# MAGIC BureauClaimRef,
# MAGIC ClaimTransactionReference,
# MAGIC BureauClaimTransactionReference,
# MAGIC MovementRef,
# MAGIC RiskCode,
# MAGIC TrustFundCode,
# MAGIC LegalEntity,
# MAGIC PolicyStatus,
# MAGIC LatestMovementIndicator,
# MAGIC OriginalCcyISO,
# MAGIC SettlementCcyISO,
# MAGIC OriginalToSettlementExchangeRate,
# MAGIC PaidClaimOrigCcy,
# MAGIC PaidClaimSettCcy,
# MAGIC PaidLossFundOrigCcy,
# MAGIC PaidLossFundSettCcy,
# MAGIC PendingLocOrigCcy,
# MAGIC PendingLocSettCcy,
# MAGIC PendingPaidClaimOrigCcy,
# MAGIC PendingPaidSettCcy,
# MAGIC AdvisedOutstandingClaimOrigCcy,
# MAGIC OutstandingSettCcy,
# MAGIC OutstandingClaimOrigCcyMovement,
# MAGIC OutstandingSettCcyMovement,
# MAGIC OurAdditionalOutstandingClaimOrigCcy,
# MAGIC OurAdditionalOutstandingSettCcy,
# MAGIC FirstOSMovementIndicator,
# MAGIC AdvisedOutstandingClaimOrigCcyMovement,
# MAGIC OurOutstandingClaimOrigCcyMovement,
# MAGIC OurOutstandingClaimOrigCcy,
# MAGIC OurAdditionalOutstandingOrigCcyMovement,
# MAGIC OurAdditionalOutstandingSettCcyMovement,
# MAGIC ExpenseOutstandingClaimOrigCcy,
# MAGIC ExpensePaidClaimOrigCcy,
# MAGIC ExpensePaidClaimSettCcy,
# MAGIC IndemnityPaidClaimOrigCcy,
# MAGIC IndemnityPaidClaimSettCcy,
# MAGIC IndemnityOutstandingClaimOrigCcy,
# MAGIC RecoveryPaidClaimOrigCcy,
# MAGIC RecoveryPaidClaimSettCcy,
# MAGIC RecoveryOutstandingClaimOrigCcy,
# MAGIC RowNumber,
# MAGIC YOA,
# MAGIC SourceSystemID,
# MAGIC RowStatus,
# MAGIC RowErrorCode,
# MAGIC LinePct,
# MAGIC movementTypeId,
# MAGIC VelocityClaimTransactionSequence
# MAGIC 
# MAGIC FROM    CTE                 
# MAGIC WHERE ClaimID = 1011243
# MAGIC     AND MovementRef = 'AE0419'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ClaimID,
# MAGIC     ClaimLineID,
# MAGIC     MovementID,
# MAGIC     ClaimLineShareID,
# MAGIC     EclipsePolicyID,
# MAGIC     EclipsePolicyLIneID,
# MAGIC     MovementRef,
# MAGIC     movementTypeId,
# MAGIC     VelocityClaimTransactionSequence,    
# MAGIC     RowNumber,
# MAGIC     LinePct,
# MAGIC     OriginalCcyISO,
# MAGIC     SettlementCcyISO,
# MAGIC     CAST(PaidClaimOrigCcy * LinePct AS DECIMAL(19, 2)) AS PaidClaimOrigCcy,
# MAGIC     CAST(PaidClaimSettCcy * LinePct AS DECIMAL(19, 2)) AS PaidClaimSettCcy,
# MAGIC     CAST(OutstandingClaimOrigCcyMovement * LinePct AS DECIMAL(19, 2)) OutstandingClaimOrigCcyMovement
# MAGIC FROM 
# MAGIC   MyMI_Abst_Eclipse.Stg30_SequelClaimTransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_SequelClaims.Stg10_ClaimTransactions WHERE VelocityClaimCaseNumber = 1001185 AND MovementRef = 5077153

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT
# MAGIC     mc.ClaimID,
# MAGIC     m.movementCWTId,
# MAGIC     m.id AS MovementId,
# MAGIC     m.MovementRef,
# MAGIC     mc.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN m.id <= 5646 THEN m.dateMovement
# MAGIC       ELSE m.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     mcl.currencyIdOriginal AS OriginalCcyISO,
# MAGIC     mcl.currencyIdMain     AS SettlementCcyISO,
# MAGIC     mcl.movementTypeId,
# MAGIC     mcs.splitCategoryId,
# MAGIC     sc.name AS SplitCategoryName,
# MAGIC     IFNULL(mcl.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(mcs.valueAtBroker)      AS ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY mc.ClaimID, mcl.movementTypeId, sc.name ORDER BY mc.DateMovement, m.movementCWTId) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT                 mc
# MAGIC     INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement          m ON mc.id               = m.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine  mcl ON mc.id               = mcl.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit mcs ON mcl.id              = mcs.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory     sc ON mcs.splitCategoryId = sc.id
# MAGIC   WHERE 
# MAGIC     mc.ClaimID = 1001185
# MAGIC     AND m.MovementRef = 5077153
# MAGIC   GROUP BY
# MAGIC     1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM MyMI_Abst_Eclipse.Stg50_ClaimTransaction
# MAGIC WHERE OriginalCcyISO = 'OMR'
# MAGIC AND ClaimLineID = '1255511'
# MAGIC ORDER BY 
# MAGIC   MovementOrder

# COMMAND ----------

df = spark.createDataFrame(dbutils.fs.ls("/mnt/main/Raw/MDSProd/Internal"))
df.display()

# COMMAND ----------

# MAGIC %md ## 1. reconciliation.reconciliation_output

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %sql DESC mymi_snapshot_group.dwh_factclaimtransaction

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE
# MAGIC   DestinationName IN ('Eclipse', 'MDSProd', 'SequelClaims', 'Velocity', 'GXLP', 'MDSDev', 'Adhoc', 'FinancialPeriod')
# MAGIC   AND LevelID = 200
# MAGIC   AND DataLoadCode IN ('20220214_2200', '20220214_2000', '20220215_0000')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND LevelID = 500
# MAGIC AND DataLoadCode IN ('2022022401')
# MAGIC -- AND ToleranceFlag IN ('Red', 'Amber')
# MAGIC ORDER BY 
# MAGIC   ReconciliationDateTime

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND LevelID = 600
# MAGIC AND DataLoadCode IN ('2022031601')
# MAGIC AND DestinationSubjectName = 'MyMI_Trans_Eclipse'
# MAGIC AND DestinationEntityName IN ('FactClaimTransactionDWH', 'FactClaimTransactionEclipse')

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND LevelID = 600
# MAGIC AND DataLoadCode IN ('2022011701')
# MAGIC AND DestinationSubjectName = 'MyMI_Trans_Eclipse'

# COMMAND ----------

int_none = None
spark.sql(f"""SELECT 1 AS A, IF({ int_none } = 'None', null, 0) AS b""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND LevelID = 500
# MAGIC AND DataLoadCode = '2022012401'
# MAGIC -- AND ToleranceFlag IN ("Red", "Amber")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND LevelID = 500
# MAGIC AND DataLoadCode = '2021112201'
# MAGIC -- AND ControlID = 57
# MAGIC AND DestinationEntityName = 'MDM_BGSUReserveType'
# MAGIC ORDER BY Variance

# COMMAND ----------

# MAGIC %sql DESC reconciliation.reconciliation_output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT DataLoadCode, DataMartLoadID
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND DataLoadCode LIKE 'F2021%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DataLoadCode, DataMartLoadID, LevelID, COUNT(1)
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND DataLoadCode = 'F20211001'
# MAGIC GROUP BY DataLoadCode, DataMartLoadID, LevelID
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND DataLoadCode = 'F20211001'
# MAGIC AND DataMartLoadID != 2021113001

# COMMAND ----------

# MAGIC %md ## 2. reconciliation.vw_reconciliation_output

# COMMAND ----------

  spark.sql(f"""
    SELECT
      ro.*
    FROM
      reconciliation.reconciliation_output ro
      JOIN reconciliation.reconciliation_dataload rd ON ro.DestinationName = rd.DestinationName
      AND ro.DataLoadCode = rd.DataLoadCode
      AND ro.LevelID      = rd.LevelID
  """).explain()

# COMMAND ----------

spark.catalog.

# COMMAND ----------

spark.sql("SHOW PARTITIONS reconciliation.reconciliation_output").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(1)
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output ro
# MAGIC   JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT DestinationName,
# MAGIC       DataLoadCode
# MAGIC     FROM
# MAGIC       reconciliation.reconciliation_dataload
# MAGIC   ) rd ON ro.DestinationName = rd.DestinationName
# MAGIC   AND ro.DataLoadCode = rd.DataLoadCode 
# MAGIC   -- AND ro.LevelID      = rd.LevelID
# MAGIC   -- WHERE ro.DestinationName = 'MyMI'
# MAGIC   -- WHERE ro.LevelID = 800
# MAGIC   -- AND ro.DataLoadCode = 'C20211107'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DestinationName,
# MAGIC   DataLoadCode,
# MAGIC   LevelID,
# MAGIC   COUNT(1)
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output ro
# MAGIC GROUP BY 
# MAGIC   1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   reconciliation.vw_reconciliation_output

# COMMAND ----------

# MAGIC %md ## 3. reconciliation.reconciliation_dataload

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_dataload

# COMMAND ----------

# MAGIC %md ## 4. Reconciliation Control Tables

# COMMAND ----------

# MAGIC %run ../../Datalib/Common/Master

# COMMAND ----------

# MAGIC %md ## List Databases

# COMMAND ----------

df = spark.catalog.listDatabases()

# COMMAND ----------

df = spark.createDataFrame(spark.catalog.listDatabases())

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md ## Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_dataload VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_dataload TIMESTAMP AS OF '2021-11-10T14:52:48.000+0000'