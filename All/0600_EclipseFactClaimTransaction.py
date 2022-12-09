# Databricks notebook source
# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionEclipse a
# MAGIC   inner join mymi_trans_mdm.dimsourcesystem c on a.FK_DWH_DimSourceSystem = c.BusinessKey
# MAGIC WHERE c.SourceSystemName = 'Sequel Claims'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionDWH a
# MAGIC   inner join mymi_trans_mdm.dimsourcesystem c on a.FK_DWH_DimSourceSystem = c.BusinessKey
# MAGIC WHERE c.SourceSystemName = 'Sequel Claims'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   mymi_trans_mdm.dimsourcesystem

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OldClaimRef, * FROM MyMI_Pre_SequelClaims.dbo_Claim 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Pre_SequelClaims.dbo_Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Pre_SequelClaims.dbo_MovementCWTLine 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMi_Trans_MDM.DimClaimTransactionCategory 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   MyMI_Abst_SequelClaims.stg10_ClaimTransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   MyMI_Pre_SequelClaims.dbo_Claim
# MAGIC WHERE 
# MAGIC   id = 1015964

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   MyMI_Pre_Eclipse.dbo_PolicyLine
# MAGIC WHERE 
# MAGIC   policyLineID = 769484

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC MyMI_Pre_Eclipse.dbo_Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC MyMI_Pre_SequelClaims.dbo_Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  PolicyUniqueID, CAST(substring_index(PolicyUniqueID,'-',1) AS INT), Product, yoa, *
# MAGIC FROM
# MAGIC   MyMI_Pre_SequelClaims.dbo_Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT float('0'), float('a')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from MyMi_Pre_mds.mdm_sequelcob  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) From MyMI_Abst_Eclipse.stg30_SequelClaimTransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From MyMI_Pre_Eclipse.dbo_PolicyLine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From MyMI_Pre_SequelClaims.dbo_PolicyLine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_extract('EclipsePolicyID=858649;VelocityLevel1Org=0;VelocityPolicySequence=0', 'EclipsePolicyID=(\\w+)', 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Pre_SequelClaims.dbo_Movement

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Pre_SequelClaims.dbo_MovementCWT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Pre_SequelClaims.dbo_MovementCWTLine

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_SequelClaims_Movement1
# MAGIC AS 
# MAGIC WITH SequelClaims_Movement AS (
# MAGIC   SELECT
# MAGIC     MC.ClaimID,
# MAGIC     M.movementCWTId,
# MAGIC     M.id AS MovementId,
# MAGIC     M.MovementRef,
# MAGIC     MC.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC       ELSE M.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC     MCL.currencyIdMain SettlementCcyISO,
# MAGIC     MCL.movementTypeId,
# MAGIC     MCS.splitCategoryId,
# MAGIC     SC.name AS SplitCategoryName,
# MAGIC     IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY SCM.ClaimID, SCM.movementTypeId, SCM.SplitCategoryName ORDER BY SCM.dateMovement ASC, SCM.movementCWTId ASC) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC     INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4,
# MAGIC     5,
# MAGIC     6,
# MAGIC     7,
# MAGIC     8,
# MAGIC     9,
# MAGIC     10,
# MAGIC     11,
# MAGIC     12
# MAGIC )
# MAGIC SELECT
# MAGIC   SCM.*,
# MAGIC   DENSE_RANK() OVER (PARTITION BY SCM.ClaimID, SCM.movementTypeId, SCM.SplitCategoryName ORDER BY SCM.dateMovement ASC, SCM.movementCWTId ASC) AS VelocityClaimTransactionSequence
# MAGIC FROM
# MAGIC   SequelClaims_Movement SCM

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_SequelClaims_Movement
# MAGIC AS 
# MAGIC SELECT
# MAGIC   MC.ClaimID,
# MAGIC   M.movementCWTId,
# MAGIC   M.id AS MovementId,
# MAGIC   M.MovementRef,
# MAGIC   MC.DateMovement,
# MAGIC   CASE
# MAGIC     WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC     ELSE M.dateActualSettlement
# MAGIC   END AS TransactionDate,
# MAGIC   MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC   MCL.currencyIdMain SettlementCcyISO,
# MAGIC   MCL.movementTypeId,
# MAGIC   MCS.splitCategoryId,
# MAGIC   SC.name AS SplitCategoryName,
# MAGIC   IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC   sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC   DENSE_RANK() OVER (
# MAGIC     PARTITION BY MC.ClaimID,
# MAGIC     MCL.movementTypeId,
# MAGIC     SC.name
# MAGIC     ORDER BY
# MAGIC       MC.DateMovement ASC,
# MAGIC       M.movementCWTId ASC
# MAGIC   ) AS VelocityClaimTransactionSequence
# MAGIC FROM
# MAGIC   MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC   INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC   LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC   LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC   LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC GROUP BY
# MAGIC   1,
# MAGIC   2,
# MAGIC   3,
# MAGIC   4,
# MAGIC   5,
# MAGIC   6,
# MAGIC   7,
# MAGIC   8,
# MAGIC   9,
# MAGIC   10,
# MAGIC   11,
# MAGIC   12

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM vw_SequelClaims_Movement

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_SequelClaims.stg10_ClaimTransactions WHERE VelocityClaimCaseNumber IN (1011773, 1015519, 1015604)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_Eclipse.stg30_sequelclaimsdetails WHERE Claim_id IN (1673244, 1015604)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMi_Pre_mds.mdm_TPAVelocityProductsToExclude

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT IF(1=1, 0, 2)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_FactClaimTransactionEclipse_SC_Pre 
# MAGIC AS 
# MAGIC WITH SequelClaimsMovement AS (
# MAGIC   SELECT
# MAGIC     MC.ClaimID,
# MAGIC     M.movementCWTId,
# MAGIC     M.id AS MovementId,
# MAGIC     M.MovementRef,
# MAGIC     MC.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC       ELSE M.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC     MCL.currencyIdMain SettlementCcyISO,
# MAGIC     MCL.movementTypeId,
# MAGIC     MCS.splitCategoryId,
# MAGIC     SC.name AS SplitCategoryName,
# MAGIC     IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY MC.ClaimID, MCL.movementTypeId, SC.name ORDER BY MC.DateMovement, M.movementCWTId) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC     INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4,
# MAGIC     5,
# MAGIC     6,
# MAGIC     7,
# MAGIC     8,
# MAGIC     9,
# MAGIC     10,
# MAGIC     11,
# MAGIC     12
# MAGIC ),
# MAGIC SequelClaimTransactions AS (
# MAGIC   SELECT
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineShareID,
# MAGIC     SCL.id EclipseClaimID,
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineID,
# MAGIC     EPL.policyLineID AS EclipsePolicyLineID,
# MAGIC     EPL.PolicyId AS EclipsePolicyID,
# MAGIC     25 AS MDSSourceSystemCode,
# MAGIC     UPPER(CAST(IFNULL(SCM.OriginalCcyISO, 'USD') AS string)) AS OriginalCcyISO,
# MAGIC     UPPER(CAST(IFNULL(SCM.SettlementCcyISO, 'USD') AS string)) AS SettlementCcyISO,    
# MAGIC     IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker ELSE 0 END, 0) AS PaidClaimOrigCcy, 
# MAGIC     IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker * SCM.ExchangeRate ELSE 0 END, 0) AS PaidClaimSettCcy, 
# MAGIC     CASE WHEN EPL.LineStatus = 'Signed' THEN EPL.SignedLine / 100
# MAGIC          WHEN EPL.LineStatus = 'Written' THEN EPL.WrittenLine / 100
# MAGIC          ELSE 0
# MAGIC     END AS LinePct,     
# MAGIC     SCM.movementCWTId,
# MAGIC     SCM.MovementId,
# MAGIC     SCM.MovementRef,
# MAGIC     SCM.DateMovement,
# MAGIC     SCM.TransactionDate,
# MAGIC     SCM.movementTypeId,
# MAGIC     SCM.splitCategoryId,
# MAGIC     SCM.SplitCategoryName,
# MAGIC     SCM.VelocityClaimTransactionSequence,
# MAGIC     CASE WHEN SCM.movementCWTId IS NULL THEN ROW_NUMBER() OVER (PARTITION BY EPL.policyLineID, SCL.id ORDER BY SCL.id, EPL.policyLineID)  
# MAGIC          ELSE ROW_NUMBER() OVER (PARTITION BY SCM.MovementRef, EPL.policyLineID, SCM.movementTypeId, SCM.VelocityClaimTransactionSequence, SCM.splitCategoryId ORDER BY SCM.TransactionDate) 
# MAGIC     END AS RowNumber
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_Claim SCL
# MAGIC     JOIN MyMI_Pre_SequelClaims.dbo_Policy SPO ON SCL.policyId = SPO.id
# MAGIC     JOIN MyMi_Pre_mds.mdm_sequelcob MSC ON SPO.product = MSC.name
# MAGIC     JOIN MyMI_Pre_Eclipse.dbo_PolicyLine EPL ON CAST(substring_index(SPO.PolicyUniqueID, '-', 1) AS INT) = EPL.PolicyId
# MAGIC     LEFT JOIN SequelClaimsMovement SCM ON SCM.ClaimId = SCL.id
# MAGIC     LEFT JOIN MyMi_Pre_SequelClaims.dbo_Tag STG ON SCL.Id = STG.claimId
# MAGIC   WHERE
# MAGIC     int(MSC.code) IS NULL --IsEclipse is True
# MAGIC     AND IFNULL(UPPER(SCL.claimTitle),'') <> 'DO NOT USE' OR UPPER(STG.name) <> 'DUPLICATE'
# MAGIC     AND UPPER(EPL.LineStatus) IN ('SIGNED', 'WRITTEN')
# MAGIC )  
# MAGIC SELECT
# MAGIC     SCT.EclipseClaimLineShareID,
# MAGIC     SCT.EclipseClaimID,
# MAGIC     SCT.EclipseClaimLineID,
# MAGIC     SCT.EclipsePolicyLineID,
# MAGIC     SCT.EclipsePolicyID,
# MAGIC     SCT.MDSSourceSystemCode,
# MAGIC     SCT.OriginalCcyISO, 
# MAGIC     SCT.SettlementCcyISO,
# MAGIC     CAST(SCT.PaidClaimOrigCcy * (SCT.LinePct) AS DECIMAL(19, 2)) AS PaidClaimOrigCcy,
# MAGIC     CAST(SCT.PaidClaimSettCcy * (SCT.LinePct) AS DECIMAL(19, 2)) AS PaidClaimSettCcy,
# MAGIC     SCT.movementCWTId,
# MAGIC     SCT.MovementId,
# MAGIC     SCT.MovementRef,
# MAGIC     SCT.DateMovement,
# MAGIC     SCT.TransactionDate,
# MAGIC     SCT.movementTypeId,
# MAGIC     SCT.splitCategoryId,
# MAGIC     SCT.SplitCategoryName,  
# MAGIC     SCT.VelocityClaimTransactionSequence,
# MAGIC     SCT.RowNumber
# MAGIC FROM
# MAGIC   SequelClaimTransactions SCT;
# MAGIC   
# MAGIC SELECT * FROM vw_FactClaimTransactionEclipse_SC_Pre;  

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_FactClaimTransactionEclipse_SC_Pre 
# MAGIC AS 
# MAGIC WITH SequelClaimsMovement AS (
# MAGIC   SELECT
# MAGIC     MC.ClaimID,
# MAGIC     M.movementCWTId,
# MAGIC     M.id AS MovementId,
# MAGIC     M.MovementRef,
# MAGIC     MC.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC       ELSE M.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC     MCL.currencyIdMain SettlementCcyISO,
# MAGIC     MCL.movementTypeId,
# MAGIC     MCS.splitCategoryId,
# MAGIC     SC.name AS SplitCategoryName,
# MAGIC     IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY MC.ClaimID, MCL.movementTypeId, SC.name ORDER BY MC.DateMovement, M.movementCWTId) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC     INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4,
# MAGIC     5,
# MAGIC     6,
# MAGIC     7,
# MAGIC     8,
# MAGIC     9,
# MAGIC     10,
# MAGIC     11,
# MAGIC     12
# MAGIC ),
# MAGIC SequelClaimsPolicyLine AS (
# MAGIC   SELECT
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineShareID,
# MAGIC     SCL.id AS EclipseClaimID,
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineID,
# MAGIC     EPL.policyLineID AS EclipsePolicyLineID,
# MAGIC     EPL.PolicyId AS EclipsePolicyID,
# MAGIC     25 AS MDSSourceSystemCode,
# MAGIC     CASE WHEN EPL.LineStatus = 'Signed' THEN EPL.SignedLine / 100
# MAGIC          WHEN EPL.LineStatus = 'Written' THEN EPL.WrittenLine / 100
# MAGIC          ELSE 0
# MAGIC     END AS LinePct,
# MAGIC     IFNULL(CASE WHEN length(SCL.OldClaimRef) = 0 THEN SCL.id ELSE SCL.OldClaimRef END, SCL.id) AS ClaimRef
# MAGIC   FROM    
# MAGIC     MyMI_Pre_SequelClaims.dbo_Claim SCL
# MAGIC     JOIN MyMI_Pre_SequelClaims.dbo_Policy SPO ON SCL.policyId = SPO.id
# MAGIC     JOIN MyMi_Pre_mds.mdm_sequelcob MSC ON SPO.product = MSC.name
# MAGIC     JOIN MyMI_Pre_Eclipse.dbo_PolicyLine EPL ON CAST(substring_index(SPO.PolicyUniqueID, '-', 1) AS INT) = EPL.PolicyId
# MAGIC     JOIN MyMI_Abst_SequelClaims.stg20_claimdetails SCD ON SCL.id = SCD.Claimref
# MAGIC   WHERE
# MAGIC     int(MSC.code) IS NULL --IsEclipse is True
# MAGIC     AND UPPER(EPL.LineStatus) IN ('SIGNED', 'WRITTEN')    
# MAGIC ),
# MAGIC SequelClaimPart1 AS (
# MAGIC   SELECT
# MAGIC     SCP.*,
# MAGIC     CAST(CONCAT(CAST(SCP.EclipseClaimID AS string), CAST(ROW_NUMBER() OVER (PARTITION BY IFNULL(SCP.EclipsePolicyLIneID, 0) ORDER BY SCM.TransactionDate, SCP.ClaimRef, SCP.EclipsePolicyID, SCP.EclipsePolicyLIneID, SCM.movementTypeId, SCM.VelocityClaimTransactionSequence, SCM.splitCategoryId) AS string)) AS INT) AS EclipseMovementID,
# MAGIC     UPPER(CAST(IFNULL(SCM.OriginalCcyISO, 'USD') AS string)) AS OriginalCcyISO,
# MAGIC     UPPER(CAST(IFNULL(SCM.SettlementCcyISO, 'USD') AS string)) AS SettlementCcyISO,    
# MAGIC     CAST(IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker * SCP.LinePct ELSE 0 END, 0)  AS DECIMAL(19, 2)) AS PaidClaimOrigCcy, 
# MAGIC     CAST(IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker * SCP.LinePct * SCM.ExchangeRate ELSE 0 END, 0) AS DECIMAL(19, 2)) AS PaidClaimSettCcy,   
# MAGIC     SCM.movementCWTId,
# MAGIC     SCM.MovementId,
# MAGIC     SCM.MovementRef,
# MAGIC     SCM.DateMovement,
# MAGIC     SCM.TransactionDate,
# MAGIC     SCM.movementTypeId,
# MAGIC     SCM.splitCategoryId,
# MAGIC     SCM.SplitCategoryName,
# MAGIC     SCM.VelocityClaimTransactionSequence,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY SCM.MovementRef, SCP.EclipsePolicyLineID, SCM.movementTypeId, SCM.VelocityClaimTransactionSequence, SCM.splitCategoryId ORDER BY SCM.TransactionDate) AS RowNumber
# MAGIC   FROM 
# MAGIC     SequelClaimsPolicyLine SCP
# MAGIC     JOIN SequelClaimsMovement SCM ON SCM.ClaimId = SCP.EclipseClaimID
# MAGIC ), 
# MAGIC SequelClaimPart2 AS (
# MAGIC   SELECT
# MAGIC     SCP.*,
# MAGIC     CAST(CONCAT(CAST(SCP.EclipseClaimID AS string), CAST(ROW_NUMBER() OVER (PARTITION BY IFNULL(SCP.EclipsePolicyLIneID, 0) ORDER BY SCP.EclipseClaimID, SCP.EclipsePolicyID, SCP.EclipsePolicyLIneID) AS string)) AS INT) AS EclipseMovementID,        
# MAGIC     UPPER(CAST(IFNULL('USD', '') AS string)) AS OriginalCcyISO,
# MAGIC     UPPER(CAST(IFNULL('USD', '') AS string)) AS SettlementCcyISO,    
# MAGIC     CAST(0 AS DECIMAL(19, 2)) AS PaidClaimOrigCcy, 
# MAGIC     CAST(0 AS DECIMAL(19, 2)) AS PaidClaimSettCcy,   
# MAGIC     null movementCWTId,
# MAGIC     null MovementId,
# MAGIC     null MovementRef,
# MAGIC     null DateMovement,
# MAGIC     null TransactionDate,
# MAGIC     '0'  movementTypeId,
# MAGIC     null splitCategoryId,
# MAGIC     null SplitCategoryName,
# MAGIC     '0'  VelocityClaimTransactionSequence,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY SCP.EclipsePolicyLineID, SCP.EclipseClaimID ORDER BY SCP.EclipseClaimID, SCP.EclipsePolicyLineID) AS RowNumber
# MAGIC   FROM 
# MAGIC     SequelClaimsPolicyLine SCP
# MAGIC     LEFT JOIN SequelClaimPart1 SCM ON UPPER(SCM.ClaimRef) = UPPER(SCP.Claimref)
# MAGIC   WHERE 
# MAGIC     SCM.Claimref IS NULL
# MAGIC )
# MAGIC SELECT * FROM SequelClaimPart1 P1
# MAGIC UNION  
# MAGIC SELECT * FROM SequelClaimPart2 P2;
# MAGIC   
# MAGIC SELECT * FROM vw_FactClaimTransactionEclipse_SC_Pre;  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Pre;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   EclipseClaimLineShareID,
# MAGIC   EclipseMovementID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   PaidClaimOrigCcy,
# MAGIC   PaidClaimSettCcy
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Pre
# MAGIC WHERE
# MAGIC   RowNumber = 1 
# MAGIC MINUS
# MAGIC SELECT
# MAGIC   DISTINCT EclipseClaimLineShareID,
# MAGIC   EclipseMovementID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   PaidClaimOrigCcy,
# MAGIC   PaidClaimSettCcy  
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Tran

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_SequelClaims.stg20_claimdetails WHERE Claim_id = 1014076

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_Eclipse.stg30_SequelClaimTransactions WHERE ClaimLineShareID = 1678452  AND ClaimID = 1014076

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)  
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Tran  
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   COUNT(*) 
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Pre
# MAGIC -- WHERE
# MAGIC --   RowNumber = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT EclipseClaimLineShareID,
# MAGIC   EclipseMovementID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   PaidClaimOrigCcy,
# MAGIC   PaidClaimSettCcy  
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Tran  
# MAGIC MINUS
# MAGIC SELECT
# MAGIC   DISTINCT EclipseClaimLineShareID,
# MAGIC   EclipseMovementID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   PaidClaimOrigCcy,
# MAGIC   PaidClaimSettCcy  
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Pre
# MAGIC -- WHERE
# MAGIC --   RowNumber = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_FactClaimTransactionEclipse_SC_Tran
# MAGIC AS
# MAGIC SELECT
# MAGIC   regexp_extract(FK_Eclipse_DimClaimTransaction, 'EclipseClaimLineShareID=(\\w+)', 1) AS EclipseClaimLineShareID,
# MAGIC --   regexp_extract(FK_Eclipse_DimClaimTransaction, 'EclipseMovementID=(\\w+)', 1) AS EclipseMovementID,
# MAGIC   regexp_extract(FK_DWH_DimClaim, 'EclipseClaimID=(\\w+)', 1) AS EclipseClaimID,
# MAGIC   regexp_extract(FK_DWH_DimClaim, 'EclipseClaimLineID=(\\w+)', 1) AS EclipseClaimLineID,
# MAGIC   regexp_extract(FK_DWH_DimPolicyGranularDetail, 'EclipsePolicyLineID=(\\w+)', 1) AS EclipsePolicyLineID,
# MAGIC   regexp_extract(FK_DWH_DimPolicy, 'EclipsePolicyID=(\\w+)', 1) AS EclipsePolicyID,
# MAGIC   regexp_extract(FK_DWH_DimSourceSystem, 'MDSSourceSystemCode=(\\w+)', 1) AS MDSSourceSystemCode,
# MAGIC   regexp_extract(`FK_DWH_DimCurrency:DWH_DimOriginalCurrency`, 'MDSCurrencyISOCode=(\\w+)', 1) AS OriginalCcyISO, 
# MAGIC   regexp_extract(`FK_DWH_DimCurrency:DWH_DimSettlementCurrency`, 'MDSCurrencyISOCode=(\\w+)', 1) AS SettlementCcyISO,     
# MAGIC --   PaidClaimOrigCcy,
# MAGIC --   PaidClaimSettCcy
# MAGIC   SUM(PaidClaimOrigCcy) AS PaidClaimOrigCcy,
# MAGIC   SUM(PaidClaimSettCcy) AS PaidClaimSettCcy
# MAGIC from
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionEclipse a
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem c ON a.FK_DWH_DimSourceSystem = c.BusinessKey
# MAGIC WHERE c.SourceSystemName = 'Sequel Claims'
# MAGIC AND `FK_DWH_DimMonth:DWH_DimProcessPeriod` != 'MonthID=999912'
# MAGIC GROUP BY
# MAGIC   1,
# MAGIC   2,
# MAGIC   3,
# MAGIC   4,
# MAGIC   5,
# MAGIC   6,
# MAGIC   7,
# MAGIC   8;
# MAGIC 
# MAGIC SELECT * FROM vw_FactClaimTransactionEclipse_SC_Tran;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_FactClaimTransactionEclipse_SC_Pre 
# MAGIC AS 
# MAGIC WITH SequelClaimsMovement AS (
# MAGIC   SELECT
# MAGIC     MC.ClaimID,
# MAGIC     M.movementCWTId,
# MAGIC     M.id AS MovementId,
# MAGIC     M.MovementRef,
# MAGIC     MC.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC       ELSE M.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC     MCL.currencyIdMain SettlementCcyISO,
# MAGIC     MCL.movementTypeId,
# MAGIC     MCS.splitCategoryId,
# MAGIC     SC.name AS SplitCategoryName,
# MAGIC     IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY MC.ClaimID, MCL.movementTypeId, SC.name ORDER BY MC.DateMovement, M.movementCWTId) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC     INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4,
# MAGIC     5,
# MAGIC     6,
# MAGIC     7,
# MAGIC     8,
# MAGIC     9,
# MAGIC     10,
# MAGIC     11,
# MAGIC     12
# MAGIC ),
# MAGIC SequelClaimsPolicyLine AS (
# MAGIC   SELECT
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineShareID,
# MAGIC     SCL.id AS EclipseClaimID,
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineID,
# MAGIC     EPL.policyLineID AS EclipsePolicyLineID,
# MAGIC     EPL.PolicyId AS EclipsePolicyID,
# MAGIC     25 AS MDSSourceSystemCode,
# MAGIC     CASE WHEN EPL.LineStatus = 'Signed' THEN EPL.SignedLine / 100
# MAGIC          WHEN EPL.LineStatus = 'Written' THEN EPL.WrittenLine / 100
# MAGIC          ELSE 0
# MAGIC     END AS LinePct,
# MAGIC     IFNULL(CASE WHEN length(SCL.OldClaimRef) = 0 THEN SCL.id ELSE SCL.OldClaimRef END, SCL.id) AS ClaimRef
# MAGIC   FROM    
# MAGIC     MyMI_Pre_SequelClaims.dbo_Claim SCL
# MAGIC     JOIN MyMI_Pre_SequelClaims.dbo_Policy SPO ON SCL.policyId = SPO.id
# MAGIC     JOIN MyMi_Pre_mds.mdm_sequelcob MSC ON SPO.product = MSC.name
# MAGIC     JOIN MyMI_Pre_Eclipse.dbo_PolicyLine EPL ON CAST(substring_index(SPO.PolicyUniqueID, '-', 1) AS INT) = EPL.PolicyId
# MAGIC     JOIN MyMI_Abst_SequelClaims.stg20_claimdetails SCD ON SCL.id = SCD.Claim_id
# MAGIC   WHERE
# MAGIC     int(MSC.code) IS NULL --IsEclipse is True
# MAGIC     AND UPPER(EPL.LineStatus) IN ('SIGNED', 'WRITTEN')    
# MAGIC ),
# MAGIC SequelClaimPart1 AS (
# MAGIC   SELECT
# MAGIC     SCP.*,
# MAGIC     CAST(CONCAT(CAST(SCP.EclipseClaimID AS string), CAST(ROW_NUMBER() OVER (PARTITION BY IFNULL(SCP.EclipsePolicyLIneID, 0) ORDER BY SCM.TransactionDate, SCP.ClaimRef, SCP.EclipsePolicyID, SCP.EclipsePolicyLIneID, SCM.movementTypeId, SCM.VelocityClaimTransactionSequence, SCM.splitCategoryId) AS string)) AS INT) AS EclipseMovementID,
# MAGIC     UPPER(CAST(IFNULL(SCM.OriginalCcyISO, 'USD') AS string)) AS OriginalCcyISO,
# MAGIC     UPPER(CAST(IFNULL(SCM.SettlementCcyISO, 'USD') AS string)) AS SettlementCcyISO,    
# MAGIC     CAST(IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker * SCP.LinePct ELSE 0 END, 0)  AS DECIMAL(19, 2)) AS PaidClaimOrigCcy, 
# MAGIC     CAST(IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker * SCP.LinePct * SCM.ExchangeRate ELSE 0 END, 0) AS DECIMAL(19, 2)) AS PaidClaimSettCcy,   
# MAGIC     SCM.movementCWTId,
# MAGIC     SCM.MovementId,
# MAGIC     SCM.MovementRef,
# MAGIC     SCM.DateMovement,
# MAGIC     SCM.TransactionDate,
# MAGIC     SCM.movementTypeId,
# MAGIC     SCM.splitCategoryId,
# MAGIC     SCM.SplitCategoryName,
# MAGIC     SCM.VelocityClaimTransactionSequence,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY SCM.MovementRef, SCP.EclipsePolicyLineID, SCM.movementTypeId, SCM.VelocityClaimTransactionSequence, SCM.splitCategoryId ORDER BY SCM.TransactionDate) AS RowNumber
# MAGIC   FROM 
# MAGIC     SequelClaimsPolicyLine SCP
# MAGIC     JOIN SequelClaimsMovement SCM ON SCM.ClaimId = SCP.EclipseClaimID
# MAGIC )
# MAGIC SELECT * FROM SequelClaimPart1 P1;
# MAGIC   
# MAGIC SELECT * FROM vw_FactClaimTransactionEclipse_SC_Pre;  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Pre' AS Table,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   SUM(PaidClaimOrigCcy) AS PaidClaimOrigCcy,
# MAGIC   SUM(PaidClaimSettCcy) AS PaidClaimSettCcy
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Pre
# MAGIC WHERE
# MAGIC   RowNumber = 1  
# MAGIC GROUP BY 1, 2, 3
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Tran' AS Table,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   SUM(PaidClaimOrigCcy) AS PaidClaimOrigCcy,
# MAGIC   SUM(PaidClaimSettCcy) AS PaidClaimSettCcy  
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Tran
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC --   'Tran' AS Table,
# MAGIC   EclipseClaimLineShareID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   SUM(PaidClaimOrigCcy) AS PaidClaimOrigCcy,
# MAGIC   SUM(PaidClaimSettCcy) AS PaidClaimSettCcy
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Tran
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
# MAGIC MINUS
# MAGIC SELECT
# MAGIC --   'Pre' AS Table,
# MAGIC   EclipseClaimLineShareID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   SUM(PaidClaimOrigCcy) AS PaidClaimOrigCcy,
# MAGIC   SUM(PaidClaimSettCcy) AS PaidClaimSettCcy  
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Pre
# MAGIC WHERE
# MAGIC   RowNumber = 1
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Tran' AS Table,
# MAGIC   EclipseClaimLineShareID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   SUM(PaidClaimOrigCcy) AS PaidClaimOrigCcy,
# MAGIC   SUM(PaidClaimSettCcy) AS PaidClaimSettCcy
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Pre
# MAGIC WHERE
# MAGIC   RowNumber = 1  
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Pre' AS Table,
# MAGIC   EclipseClaimLineShareID,
# MAGIC   EclipseClaimID,
# MAGIC   EclipseClaimLineID,
# MAGIC   EclipsePolicyLineID,
# MAGIC   EclipsePolicyID,
# MAGIC   MDSSourceSystemCode,
# MAGIC   OriginalCcyISO, 
# MAGIC   SettlementCcyISO,
# MAGIC   SUM(PaidClaimOrigCcy) AS PaidClaimOrigCcy,
# MAGIC   SUM(PaidClaimSettCcy) AS PaidClaimSettCcy  
# MAGIC FROM
# MAGIC   vw_FactClaimTransactionEclipse_SC_Tran
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT
# MAGIC     MC.ClaimID,
# MAGIC     M.movementCWTId,
# MAGIC     M.id AS MovementId,
# MAGIC     M.MovementRef,
# MAGIC     MC.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC       ELSE M.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC     MCL.currencyIdMain SettlementCcyISO,
# MAGIC     MCL.movementTypeId,
# MAGIC     MCS.splitCategoryId,
# MAGIC     SC.name AS SplitCategoryName,
# MAGIC     IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY MC.ClaimID, MCL.movementTypeId, SC.name ORDER BY MC.DateMovement, M.movementCWTId) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC     INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC   WHERE 
# MAGIC     MC.ClaimID = 1011773
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4,
# MAGIC     5,
# MAGIC     6,
# MAGIC     7,
# MAGIC     8,
# MAGIC     9,
# MAGIC     10,
# MAGIC     11,
# MAGIC     12

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Pre_SequelClaims.dbo_MovementCWT  WHERE ClaimID = 1011773

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Pre_SequelClaims.dbo_Movement  WHERE ClaimID = 1011773

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_Eclipse.stg30_SequelClaimTransactions WHERE MovementRef IN ('AB0919', 'AC1219') ORDER BY MovementRef, EclipsePolicyLIneID, movementTypeId, VelocityClaimTransactionSequence

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_Eclipse.Stg50_ClaimTransaction  WHERE ClaimID = 1011773

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_Eclipse.Stg50_ClaimTransaction WHERE ClaimID = 1011773 AND RowNumber = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_Eclipse.Stg60_CommonClaimTransactions 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_SequelClaimsTransaction
# MAGIC AS 
# MAGIC WITH SequelClaimsMovement AS (
# MAGIC   SELECT
# MAGIC     MC.ClaimID,
# MAGIC     M.movementCWTId,
# MAGIC     M.id AS MovementId,
# MAGIC     M.MovementRef,
# MAGIC     MC.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC       ELSE M.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC     MCL.currencyIdMain SettlementCcyISO,
# MAGIC     MCL.movementTypeId,
# MAGIC     MCS.splitCategoryId,
# MAGIC     SC.name AS SplitCategoryName,
# MAGIC     IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY MC.ClaimID, MCL.movementTypeId, SC.name ORDER BY MC.DateMovement, M.movementCWTId) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC     INNER JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4,
# MAGIC     5,
# MAGIC     6,
# MAGIC     7,
# MAGIC     8,
# MAGIC     9,
# MAGIC     10,
# MAGIC     11,
# MAGIC     12
# MAGIC ),
# MAGIC SequelClaimsPolicyLine AS (
# MAGIC   SELECT
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineShareID,
# MAGIC     SCL.id AS EclipseClaimID,
# MAGIC     IFNULL(EPL.policyLineID, 0) + 1000000 AS EclipseClaimLineID,
# MAGIC     EPL.policyLineID AS EclipsePolicyLineID,
# MAGIC     EPL.PolicyId AS EclipsePolicyID,
# MAGIC     25 AS MDSSourceSystemCode,
# MAGIC     CASE WHEN EPL.LineStatus = 'Signed' THEN EPL.SignedLine / 100
# MAGIC          WHEN EPL.LineStatus = 'Written' THEN EPL.WrittenLine / 100
# MAGIC          ELSE 0
# MAGIC     END AS LinePct,
# MAGIC     CASE WHEN length(IFNULL(OldClaimRef, '')) = 0 THEN SCL.id ELSE SCL.OldClaimRef END AS ClaimRef
# MAGIC   FROM    
# MAGIC     MyMI_Pre_SequelClaims.dbo_Claim SCL
# MAGIC     JOIN MyMI_Pre_SequelClaims.dbo_Policy SPO ON SCL.policyId = SPO.id
# MAGIC     JOIN MyMi_Pre_mds.mdm_sequelcob MSC ON SPO.product = MSC.name
# MAGIC     LEFT JOIN MyMI_Pre_Eclipse.dbo_PolicyLine EPL ON CAST(substring_index(SPO.PolicyUniqueID, '-', 1) AS INT) = EPL.PolicyId
# MAGIC   WHERE
# MAGIC     int(MSC.code) IS NULL --IsEclipse is True
# MAGIC     AND UPPER(EPL.LineStatus) IN ('SIGNED', 'WRITTEN')
# MAGIC ),
# MAGIC SequelClaimsTransaction AS (
# MAGIC   SELECT
# MAGIC     SCP.*,   
# MAGIC     UPPER(CAST(IFNULL(SCM.OriginalCcyISO, 'USD') AS string)) AS OriginalCcyISO,
# MAGIC     UPPER(CAST(IFNULL(SCM.SettlementCcyISO, 'USD') AS string)) AS SettlementCcyISO,    
# MAGIC     CAST(IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker * SCP.LinePct ELSE 0 END, 0)  AS DECIMAL(19, 2)) AS PaidClaimOrigCcy, 
# MAGIC     CAST(IFNULL(CASE WHEN SCM.movementTypeId = 'PAYMENT' THEN SCM.valueAtBroker * SCP.LinePct * SCM.ExchangeRate ELSE 0 END, 0) AS DECIMAL(19, 2)) AS PaidClaimSettCcy, 
# MAGIC     SCM.movementCWTId,
# MAGIC     SCM.MovementId,
# MAGIC     SCM.MovementRef,
# MAGIC     SCM.DateMovement,
# MAGIC     SCM.TransactionDate,
# MAGIC     SCM.movementTypeId,
# MAGIC     SCM.splitCategoryId,
# MAGIC     SCM.SplitCategoryName,
# MAGIC     SCM.VelocityClaimTransactionSequence,
# MAGIC     CAST(CONCAT(CAST(SCP.EclipseClaimID AS string), CAST(ROW_NUMBER() OVER (PARTITION BY IFNULL(SCP.EclipsePolicyLIneID, 0) ORDER BY SCM.TransactionDate, SCP.ClaimRef, SCP.EclipsePolicyID, SCP.EclipsePolicyLIneID, SCM.movementTypeId, SCM.VelocityClaimTransactionSequence, SCM.splitCategoryId) AS string)) AS INT) AS EclipseMovementID,    
# MAGIC     ROW_NUMBER() OVER (PARTITION BY SCM.MovementRef, SCP.EclipsePolicyLineID, SCM.movementTypeId, SCM.VelocityClaimTransactionSequence, SCM.splitCategoryId ORDER BY SCM.TransactionDate) AS RowNumber
# MAGIC   FROM 
# MAGIC     SequelClaimsPolicyLine SCP
# MAGIC     LEFT JOIN SequelClaimsMovement SCM ON SCM.ClaimId = SCP.EclipseClaimID
# MAGIC     JOIN MyMI_Abst_SequelClaims.stg20_claimdetails SCD ON SCP.ClaimRef = SCD.ClaimRef
# MAGIC )
# MAGIC SELECT 
# MAGIC   SCT.*
# MAGIC FROM SequelClaimsTransaction SCT
# MAGIC WHERE RowNumber = 1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_SequelClaimsTransaction

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyMI_Abst_Eclipse.stg30_SequelClaimTransactions 

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH SequelClaimsMovement AS (
# MAGIC   SELECT
# MAGIC     MC.ClaimID,
# MAGIC     M.movementCWTId,
# MAGIC     M.id AS MovementId,
# MAGIC     M.MovementRef,
# MAGIC     MC.DateMovement,
# MAGIC     CASE
# MAGIC       WHEN M.id <= 5646 THEN M.dateMovement
# MAGIC       ELSE M.dateActualSettlement
# MAGIC     END AS TransactionDate,
# MAGIC     MCL.currencyIdOriginal OriginalCcyISO,
# MAGIC     MCL.currencyIdMain SettlementCcyISO,
# MAGIC     MCL.movementTypeId,
# MAGIC     MCS.splitCategoryId,
# MAGIC     SC.name AS SplitCategoryName,
# MAGIC     IFNULL(MCL.exchangeRate, 1) AS ExchangeRate,
# MAGIC     sum(MCS.valueAtBroker) as ValueAtBroker,
# MAGIC     DENSE_RANK() OVER (PARTITION BY MC.ClaimID, MCL.movementTypeId, SC.name ORDER BY MC.DateMovement, M.movementCWTId) AS VelocityClaimTransactionSequence
# MAGIC   FROM
# MAGIC     MyMI_Pre_SequelClaims.dbo_MovementCWT MC
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_Movement M ON MC.id = M.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTLine MCL ON MC.id = MCL.movementCWTId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_MovementCWTSplit MCS ON MCL.id = MCS.movementCWTLineId
# MAGIC     LEFT JOIN MyMI_Pre_SequelClaims.dbo_SplitCategory SC ON MCS.splitCategoryId = SC.id
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4,
# MAGIC     5,
# MAGIC     6,
# MAGIC     7,
# MAGIC     8,
# MAGIC     9,
# MAGIC     10,
# MAGIC     11,
# MAGIC     12
# MAGIC )
# MAGIC SELECT * 
# MAGIC FROM SequelClaimsMovement
# MAGIC WHERE ClaimID = 1011243
# MAGIC -- AND MovementRef = 'AE0419'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   ClaimID,
# MAGIC   MovementRef,
# MAGIC   COUNT(*)
# MAGIC FROM MyMI_Abst_Eclipse.stg30_SequelClaimTransactions 
# MAGIC GROUP BY 1, 2
# MAGIC MINUS
# MAGIC SELECT 
# MAGIC   EclipseClaimID AS ClaimID, 
# MAGIC   IFNULL(MovementRef, 0) AS MovementRef,
# MAGIC   COUNT(*)
# MAGIC FROM vw_SequelClaimsTransaction
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   EclipsePolicyLIneID, 
# MAGIC   EclipsePolicyID, 
# MAGIC   ClaimID,
# MAGIC   ClaimRef,
# MAGIC   LinePct,
# MAGIC   MovementID
# MAGIC FROM MyMI_Abst_Eclipse.stg30_SequelClaimTransactions 
# MAGIC MINUS
# MAGIC SELECT 
# MAGIC   EclipsePolicyLineID, 
# MAGIC   EclipsePolicyID, 
# MAGIC   EclipseClaimID AS ClaimID, 
# MAGIC   ClaimRef,
# MAGIC   LinePct,
# MAGIC   EclipseMovementID as MovementID
# MAGIC FROM vw_SequelClaimsTransaction

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   EclipsePolicyLineID, 
# MAGIC   EclipsePolicyID, 
# MAGIC   EclipseClaimID AS ClaimID, 
# MAGIC   ClaimRef,
# MAGIC   LinePct,
# MAGIC   EclipseMovementID as MovementID
# MAGIC FROM vw_SequelClaimsTransaction
# MAGIC MINUS
# MAGIC SELECT 
# MAGIC   EclipsePolicyLIneID, 
# MAGIC   EclipsePolicyID, 
# MAGIC   ClaimID,
# MAGIC   ClaimRef,
# MAGIC   LinePct,
# MAGIC   MovementID
# MAGIC FROM MyMI_Abst_Eclipse.stg30_SequelClaimTransactions 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM MyMI_Abst_SequelClaims.stg20_claimdetails 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM MyMI_Abst_SequelClaims.Stg10_ClaimTransactions 

# COMMAND ----------

# MAGIC %sql 
# MAGIC WITH 
# MAGIC SequelClaimExclusion AS (
# MAGIC   SELECT
# MAGIC     DISTINCT PL.eclipsePolicyLineID
# MAGIC   FROM
# MAGIC     MyMI_Abst_SequelClaims.stg20_ClaimDetails SCD
# MAGIC     INNER JOIN MyMI_Abst_Eclipse.stg20_PolicyLine PL ON PL.EclipsePolicyID = SCD.PolicyID
# MAGIC     INNER JOIN MyMI_Abst_MDM.stg10_MDSProduct MP ON MP.ProductCode = SCD.ProductCode
# MAGIC     AND MP.GroupClass = PL.ReportingClass2
# MAGIC ),
# MAGIC MVExclusion as (
# MAGIC   SELECT
# MAGIC     PL.eclipsePolicyLineID
# MAGIC   FROM
# MAGIC     MyMI_Abst_Eclipse.stg20_PolicyLine PL
# MAGIC     INNER JOIN MYMI_Pre_MDS.mdm_eclipseexclusion MEE ON MEE.GroupClass = PL.ReportingClass2
# MAGIC     AND CASE
# MAGIC       WHEN MEE.SubClassCode = '*' THEN PL.Class3
# MAGIC       ELSE MEE.SubClassCode
# MAGIC     END = PL.Class3
# MAGIC     AND CASE
# MAGIC       WHEN MEE.YOA = '*' THEN PL.YOA
# MAGIC       ELSE MEE.YOA
# MAGIC     END = PL.YOA
# MAGIC     AND CASE
# MAGIC       WHEN MEE.Syndicate = '*' THEN PL.Synd
# MAGIC       ELSE MEE.Syndicate
# MAGIC     END = PL.Synd
# MAGIC )
# MAGIC select
# MAGIC   2 AS SourceSystemID,
# MAGIC   cls.SettCcyISO,
# MAGIC   cls.OsCcyISO,
# MAGIC   cls.SharePaidThisTimeOrigCcy,
# MAGIC   cls.SharePaidThisTimeSettCcy
# MAGIC FROM
# MAGIC   MyMI_Pre_Eclipse.dbo_Policy pol
# MAGIC   INNER JOIN MyMI_Pre_Eclipse.dbo_PolicyLine pl ON pol.PolicyId = pl.PolicyId
# MAGIC   INNER JOIN MyMI_Pre_Eclipse.dbo_ClaimLine cl -- Joins Claim with line % of policy
# MAGIC   ON cl.PolicyLineId = pl.PolicyLineId
# MAGIC   INNER JOIN MyMI_Pre_Eclipse.dbo_ClaimLineShare cls ON cls.ClaimLineID = cl.ClaimLineID
# MAGIC   INNER JOIN MyMI_Pre_Eclipse.dbo_Movement mvmt ON mvmt.MovementID = cls.MovementID
# MAGIC WHERE
# MAGIC   pol.DelDate IS NULL
# MAGIC   AND pl.DelDate IS NULL
# MAGIC   AND cl.DelDate IS NULL
# MAGIC   AND cls.DelDate IS NULL
# MAGIC   AND mvmt.DelDate IS NULL
# MAGIC   AND cls.EarlyRIInd <> 'Y'
# MAGIC   AND pol.PolicyID NOT IN (
# MAGIC     SELECT
# MAGIC       Policyid
# MAGIC     FROM
# MAGIC       MyMI_Abst_Eclipse.Stg10_ExcludedPolicy
# MAGIC   )
# MAGIC   AND 1 = CASE
# MAGIC     WHEN pl.PolicyLineID IN (
# MAGIC       SELECT
# MAGIC         EclipsePolicyLineID
# MAGIC       FROM
# MAGIC         SequelClaimExclusion
# MAGIC     )
# MAGIC     AND IFNULL(
# MAGIC       CAST(UPPER(LTRIM(RTRIM(pl.Synd))) AS string),
# MAGIC       ''
# MAGIC     ) IN ('BGSU', 'BGU8')
# MAGIC     OR pl.PolicyLineID IN (
# MAGIC       SELECT
# MAGIC         EclipsePolicyLineID
# MAGIC       FROM
# MAGIC         MVExclusion
# MAGIC     ) THEN 0
# MAGIC     ELSE 1
# MAGIC   END

# COMMAND ----------

# MAGIC %md # 1.Crwaford Analysis