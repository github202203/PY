# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationProcess_0600_EclipseFactClaimTransaction
# MAGIC * **Description :** Reconciliation process for Pre-Transformed to Transformed Output - Eclipse Fact Claim Transaction
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 25/01/2022 | Raja Murugan | Reconciliation process for Pre-Transformed to Transformed Output - Eclipse Fact Claim Transaction |
# MAGIC | 16/03/2022 | Raja Murugan | Added OutstandingClaimOrigCcyMovement |

# COMMAND ----------

# MAGIC %md # 1.Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# DBTITLE 0,Reconciliation Libs
# MAGIC %run /Datalib/Common/Master

# COMMAND ----------

# MAGIC %md ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## 1.3 Set Parameters

# COMMAND ----------

# DBTITLE 0,Declare Properties
dbutils.widgets.text("DebugRun"               , "True"                                 , "DebugRun")
dbutils.widgets.text("DestinationID"          , "1"                                    , "DestinationID")
dbutils.widgets.text("DestinationName"        , "MyMI"                                 , "DestinationName")
dbutils.widgets.text("LevelID"                , "0600"                                 , "LevelID")
dbutils.widgets.text("LevelName"              , "Pre-Transformed to Transformed Output", "LevelName")
dbutils.widgets.text("DataLoadCode"           , "2022030901"                           , "DataLoadCode")
dbutils.widgets.text("FileDateTime"           , ""                                     , "FileDateTime")
dbutils.widgets.text("DataMartLoadID"         , "2022030901"                           , "DataMartLoadID")
dbutils.widgets.text("DataHistoryCode"        , ""                                     , "DataHistoryCode")
dbutils.widgets.text("ReconciliationDateTime" , "20220310_1100"                        , "ReconciliationDateTime")
dbutils.widgets.text("JobRunID"               , "7001"                                 , "JobRunID")
dbutils.widgets.text("PipelineRunID"          , "7001"                                 , "PipelineRunID")
dbutils.widgets.text("ControlID"              , ""                                     , "ControlID")
dbutils.widgets.text("DataMartID"             , "1"                                    , "DataMartID")
dbutils.widgets.text("DataMartName"           , "MyMI"                                 , "DataMartName")
dbutils.widgets.text("SourceSubjectID"        , ""                                     , "SourceSubjectID")
dbutils.widgets.text("SourceSubjectName"      , ""                                     , "SourceSubjectName")
dbutils.widgets.text("SourceEntityID"         , ""                                     , "SourceEntityID")
dbutils.widgets.text("SourceEntityName"       , ""                                     , "SourceEntityName")
dbutils.widgets.text("DestinationSubjectID"   , ""                                     , "DestinationSubjectID")
dbutils.widgets.text("DestinationSubjectName" , ""                                     , "DestinationSubjectName")
dbutils.widgets.text("DestinationEntityID"    , ""                                     , "DestinationEntityID")
dbutils.widgets.text("DestinationEntityName"  , ""                                     , "DestinationEntityName")
dbutils.widgets.text("LoadType"               , ""                                     , "LoadType")
dbutils.widgets.text("SourceOrigCcy"          , ""                                     , "SourceOrigCcy")
dbutils.widgets.text("SourceSettCcy"          , ""                                     , "SourceSettCcy")
dbutils.widgets.text("SourceLimitCcy"         , ""                                     , "SourceLimitCcy")
dbutils.widgets.text("DestinationOrigCcy"     , ""                                     , "DestinationOrigCcy")
dbutils.widgets.text("DestinationSettCcy"     , ""                                     , "DestinationSettCcy")
dbutils.widgets.text("DestinationLimitCcy"    , ""                                     , "DestinationLimitCcy")
dbutils.widgets.text("SourceSystemID"         , ""                                     , "SourceSystemID")
dbutils.widgets.text("DestinationSystemID"    , ""                                     , "DestinationSystemID")
dbutils.widgets.text("GreenCountTolerance"    , "0.0000"                               , "GreenCountTolerance")
dbutils.widgets.text("GreenSumTolerance"      , "0.0100"                               , "GreenSumTolerance")
dbutils.widgets.text("AmberCountTolerance"    , "0.0050"                               , "AmberCountTolerance")
dbutils.widgets.text("AmberSumTolerance"      , "0.0500"                               , "AmberSumTolerance")
dbutils.widgets.text("IsToleranceRatio"       , "1"                                    , "IsToleranceRatio")

# COMMAND ----------

# MAGIC %md ## 1.4 Set Variables

# COMMAND ----------

bol_debugRun               = eval(dbutils.widgets.get("DebugRun"))
int_destinationID          = int(dbutils.widgets.get("DestinationID"))
str_destinationName        = dbutils.widgets.get("DestinationName")
int_levelID                = int(dbutils.widgets.get("LevelID"))
str_levelName              = dbutils.widgets.get("LevelName")
str_dataLoadCode           = dbutils.widgets.get("DataLoadCode")
str_fileDateTime           = dbutils.widgets.get("FileDateTime")        if dbutils.widgets.get("FileDateTime")    else None
int_dataMartLoadID         = int(dbutils.widgets.get("DataMartLoadID")) if dbutils.widgets.get("DataMartLoadID")  else None
str_dataHistoryCode        = dbutils.widgets.get("DataHistoryCode")     if dbutils.widgets.get("DataHistoryCode") else None
str_reconciliationDateTime = dbutils.widgets.get("ReconciliationDateTime")
int_jobRunID               = int(dbutils.widgets.get("JobRunID"))
str_pipelineRunID          = dbutils.widgets.get("PipelineRunID")
int_controlID              = int(dbutils.widgets.get("ControlID"))      if dbutils.widgets.get("ControlID")       else None
int_dataMartID             = int(dbutils.widgets.get("DataMartID"))
str_dataMartName           = dbutils.widgets.get("DataMartName")
int_sourceSubjectID        = 0
str_sourceSubjectName      = "'MyMI_Pre_Eclipse', 'MyMI_Pre_Crawford', 'MyMI_Pre_MountainView', 'MyMI_Pre_SequelClaims'"
int_sourceEntityID         = 0
str_sourceEntityName       = "'dbo_ClaimLineShare', 'Crawford_Movement', 'MV_ClaimFinancials', 'dbo_MovementCWTSplit'"
int_destinationSubjectID   = 0
str_destinationSubjectName = "'MyMI_Trans_Eclipse'"
int_destinationEntityID    = 0
str_destinationEntityName  = "'FactClaimTransactionEclipse', 'FactClaimTransactionDWH'"
str_loadType               = dbutils.widgets.get("LoadType")
str_sourceOrigCcy          = dbutils.widgets.get("SourceOrigCcy")
str_sourceSettCcy          = dbutils.widgets.get("SourceSettCcy")
str_sourceLimitCcy         = dbutils.widgets.get("SourceLimitCcy")
str_sourceSystemID         = dbutils.widgets.get("SourceSystemID")
str_destinationOrigCcy     = dbutils.widgets.get("DestinationOrigCcy")
str_destinationSettCcy     = dbutils.widgets.get("DestinationSettCcy")
str_destinationLimitCcy    = dbutils.widgets.get("DestinationLimitCcy")
str_destinationSystemID    = dbutils.widgets.get("DestinationSystemID")
dct_tolerance              = {
  "GreenCount": Decimal(dbutils.widgets.get("GreenCountTolerance")),
  "GreenSum"  : Decimal(dbutils.widgets.get("GreenSumTolerance")),
  "AmberCount": Decimal(dbutils.widgets.get("AmberCountTolerance")),
  "AmberSum"  : Decimal(dbutils.widgets.get("AmberSumTolerance"))
}
bol_isToleranceRatio       = bool(dbutils.widgets.get("IsToleranceRatio"))
dt_currentDateTime         = datetime.now()
str_reconciliationPath     = "dbfs:/mnt/reconciliation"
str_filePath               = f"{str_reconciliationPath}/datalake/temp/{str_destinationName}/{int_levelID}/{str_dataLoadCode}/MyMI_Trans_Eclipse_FactClaimTransaction"

# COMMAND ----------

# MAGIC %md # 2. Process

# COMMAND ----------

# MAGIC %md ## 2.1 Source

# COMMAND ----------

# MAGIC %md ### 2.1.1 Eclipse

# COMMAND ----------

# MAGIC %md #### 2.1.1.1 Create vw_eclipseClaimTransaction

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_eclipseClaimTransaction AS 
# MAGIC WITH 
# MAGIC SequelClaimExclusion AS (
# MAGIC   SELECT
# MAGIC     DISTINCT pl.eclipsePolicyLineID
# MAGIC   FROM
# MAGIC     MyMI_Abst_SequelClaims.stg20_ClaimDetails scd
# MAGIC     JOIN MyMI_Abst_Eclipse.stg20_PolicyLine    pl ON pl.EclipsePolicyID = scd.PolicyID
# MAGIC     JOIN MyMI_Abst_MDM.stg10_MDSProduct        mp ON mp.ProductCode     = scd.ProductCode
# MAGIC       AND mp.GroupClass = pl.ReportingClass2
# MAGIC ),
# MAGIC MountainViewExclusion AS (
# MAGIC   SELECT
# MAGIC     DISTINCT pl.eclipsePolicyLineID
# MAGIC   FROM
# MAGIC     MyMI_Abst_Eclipse.stg20_PolicyLine pl
# MAGIC     JOIN MYMI_Pre_MDS.mdm_eclipseexclusion mee ON mee.GroupClass   = pl.ReportingClass2
# MAGIC       AND IF(mee.SubClassCode = '*' , pl.Class3, mee.SubClassCode) = pl.Class3
# MAGIC       AND IF(mee.YOA          = '*' , pl.YOA   , mee.YOA)          = pl.YOA
# MAGIC       AND IF(mee.Syndicate    = '*' , pl.Synd  , mee.Syndicate)    = pl.Synd
# MAGIC ),
# MAGIC ClaimTransaction AS (
# MAGIC   SELECT 
# MAGIC     cls.SettCcyISO               AS SettlementCcyISO,
# MAGIC     cls.OsCcyISO                 AS OriginalCcyISO,
# MAGIC     cls.SharePaidThisTimeOrigCcy AS PaidClaimOrigCcy,
# MAGIC     cls.SharePaidThisTimeSettCcy AS PaidClaimSettCcy,
# MAGIC     (IFNULL(cls.SharePendingThisTimeIndemOrigCcy, 0) + IFNULL(cls.SharePendingThisTimeFeesOrigCcy, 0)) * -1 AS PendingPaidClaimOrigCcy,
# MAGIC     IFNULL(cls.ShareOSOrigCcy, 0) * -1 AS AdvisedOutstandingClaimOrigCcy,
# MAGIC     IFNULL(CASE WHEN (IFNULL(ShareUWViewOSIndemnityOrigCcy, 0) + IFNULL(ShareUWViewOSFeeOrigCcy, 0) <> IFNULL(ShareOSIndemOrigCcy,  0) + IFNULL(ShareOSFeeOrigCcy, 0))
# MAGIC                  AND (ShareUWViewOSIndemnityOrigCcy IS NOT NULL OR ShareUWViewOSFeeOrigCcy IS NOT NULL) THEN ShareUWAddOSOrigCcy * -1
# MAGIC                 ELSE 0
# MAGIC            END, 0) AS OurAdditionalOutstandingClaimOrigCcy,
# MAGIC     cls.ClaimLineID,
# MAGIC     ROW_NUMBER() OVER(PARTITION BY cl.ClaimLineID, cls.OsCcyISO, cls.SettCcyISO  ORDER BY m.MovementDate, m.MovementID, cls.ClaimLineShareID ASC) As MovementOrder
# MAGIC   FROM
# MAGIC     MyMI_Pre_Eclipse.dbo_Policy                            p
# MAGIC     INNER JOIN MyMI_Pre_Eclipse.dbo_PolicyLine            pl ON p.PolicyId      = pl.PolicyId
# MAGIC     INNER JOIN MyMI_Pre_Eclipse.dbo_ClaimLine             cl ON pl.PolicyLineId = cl.PolicyLineId
# MAGIC     INNER JOIN MyMI_Pre_Eclipse.dbo_ClaimLineShare       cls ON cl.ClaimLineID  = cls.ClaimLineID
# MAGIC     INNER JOIN MyMI_Pre_Eclipse.dbo_Movement               m ON cls.MovementID  = m.MovementID
# MAGIC     INNER JOIN MyMI_Pre_Eclipse.dbo_EclipseClaim          ec ON cl.ClaimID      = ec.ClaimId  
# MAGIC     LEFT ANTI JOIN SequelClaimExclusion                  sce ON pl.PolicyLineID = sce.EclipsePolicyLineID 
# MAGIC       AND IFNULL(UPPER(TRIM(pl.Synd)), '') IN ('BGSU', 'BGU8')
# MAGIC     LEFT ANTI JOIN MountainViewExclusion                 mve ON pl.PolicyLineID = mve.EclipsePolicyLineID
# MAGIC     LEFT ANTI JOIN MyMI_Abst_Eclipse.Stg10_ExcludedPolicy ep ON p.PolicyID      = ep.Policyid
# MAGIC   WHERE
# MAGIC     p.DelDate          IS NULL
# MAGIC     AND pl.DelDate     IS NULL
# MAGIC     AND cl.DelDate     IS NULL
# MAGIC     AND cls.DelDate    IS NULL
# MAGIC     AND m.DelDate      IS NULL
# MAGIC     AND cls.EarlyRIInd <> 'Y'
# MAGIC ),
# MAGIC EclipseClaimTransaction AS (
# MAGIC   SELECT 
# MAGIC     SettlementCcyISO,
# MAGIC     OriginalCcyISO,
# MAGIC     PaidClaimOrigCcy,
# MAGIC     PaidClaimSettCcy,
# MAGIC     PendingPaidClaimOrigCcy,
# MAGIC     AdvisedOutstandingClaimOrigCcy,
# MAGIC     IFNULL(LAG(AdvisedOutstandingClaimOrigCcy) OVER (PARTITION BY ClaimLineID,OriginalCcyISO,SettlementCcyISO ORDER BY MovementOrder), 0) AS PrevAdvisedOutstandingClaimOrigCcy,
# MAGIC     OurAdditionalOutstandingClaimOrigCcy,
# MAGIC     IFNULL(LAG(OurAdditionalOutstandingClaimOrigCcy) OVER (PARTITION BY ClaimLineID,OriginalCcyISO,SettlementCcyISO ORDER BY MovementOrder), 0) AS PrevOurAdditionalOutstandingClaimOrigCcy,
# MAGIC     ClaimLineID,
# MAGIC     MovementOrder
# MAGIC   FROM
# MAGIC     ClaimTransaction
# MAGIC )  
# MAGIC SELECT 
# MAGIC   'Eclipse'                    AS SourceSystemName,
# MAGIC   'MyMI_Pre_Eclipse'           AS SourceSubjectName,
# MAGIC   'dbo_ClaimLineShare'         AS SourceEntityName,  
# MAGIC   SettlementCcyISO,
# MAGIC   OriginalCcyISO,
# MAGIC   PaidClaimOrigCcy,
# MAGIC   PaidClaimSettCcy,
# MAGIC   (PendingPaidClaimOrigCcy + 
# MAGIC    AdvisedOutstandingClaimOrigCcy - PrevAdvisedOutstandingClaimOrigCcy + 
# MAGIC    OurAdditionalOutstandingClaimOrigCcy - PrevOurAdditionalOutstandingClaimOrigCcy) AS OutstandingClaimOrigCcyMovement,
# MAGIC   PendingPaidClaimOrigCcy,
# MAGIC   AdvisedOutstandingClaimOrigCcy,
# MAGIC   PrevAdvisedOutstandingClaimOrigCcy,
# MAGIC   OurAdditionalOutstandingClaimOrigCcy,
# MAGIC   PrevOurAdditionalOutstandingClaimOrigCcy,
# MAGIC   ClaimLineID,
# MAGIC   MovementOrder
# MAGIC FROM
# MAGIC   EclipseClaimTransaction

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_eclipseClaimTransaction").display()

# COMMAND ----------

# MAGIC %md #### 2.1.1.2 Create vw_reconciliationEclipse

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationEclipse AS
# MAGIC 
# MAGIC /* Get RecordCount */
# MAGIC SELECT 
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Count'       AS ReconciliationType,
# MAGIC   'RecordCount' AS ReconciliationColumn,
# MAGIC   'N/A'         AS CurrencyType,
# MAGIC   'N/A'         AS CurrencyCode,
# MAGIC   COUNT(1)      AS SourceValue
# MAGIC FROM
# MAGIC   vw_eclipseClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimOrigCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                        AS ReconciliationType,
# MAGIC   'PaidClaimOrigCcy'           AS ReconciliationColumn,
# MAGIC   'Original Currency'          AS CurrencyType,
# MAGIC   ct.OriginalCcyISO            AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimOrigCcy) *-1 AS SourceValue
# MAGIC FROM
# MAGIC   vw_eclipseClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimSettCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                         AS ReconciliationType,
# MAGIC   'PaidClaimSettCcy'            AS ReconciliationColumn,
# MAGIC   'Settlement Currency'         AS CurrencyType,
# MAGIC   ct.SettlementCcyISO           AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimSettCcy) * -1 AS SourceValue
# MAGIC FROM
# MAGIC   vw_eclipseClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get OutstandingClaimOrigCcyMovement */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                                   AS ReconciliationType,
# MAGIC   'OutstandingClaimOrigCcyMovement'       AS ReconciliationColumn,
# MAGIC   'Original Currency'                     AS CurrencyType,
# MAGIC   ct.OriginalCcyISO                       AS CurrencyCode,
# MAGIC   SUM(ct.OutstandingClaimOrigCcyMovement) AS SourceValue
# MAGIC FROM
# MAGIC   vw_eclipseClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7    

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationEclipse").display()

# COMMAND ----------

# MAGIC %md ### 2.1.2 Crawford

# COMMAND ----------

# MAGIC %md #### 2.1.2.1 Create vw_crawfordClaimTransaction

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_crawfordClaimTransaction AS
# MAGIC SELECT
# MAGIC   'Crawford'          AS SourceSystemName,
# MAGIC   'MyMI_Pre_Crawford' AS SourceSubjectName,
# MAGIC   'Crawford_Movement' AS SourceEntityName,  
# MAGIC   i.IncidentID,
# MAGIC   m.MovementID,
# MAGIC   i.currency AS OriginalCcyISO,
# MAGIC   CASE
# MAGIC     WHEN m.Type IN ('Expense Payment', 'Indemnity Payment') THEN m.Amount
# MAGIC     WHEN m.Type IN ('Recovered') THEN m.Amount * -1
# MAGIC     ELSE 0
# MAGIC   End AS PaidClaimOrigCcy,
# MAGIC   CASE
# MAGIC     WHEN m.Type IN ('Expense Reserve','Indemnity Reserve') THEN m.Amount
# MAGIC     WHEN m.Type IN ('Recovery Reserve') THEN m.Amount * -1
# MAGIC     ELSE 0
# MAGIC   End AS OutstandingClaimOrigCcyMovement  
# MAGIC FROM
# MAGIC   MyMI_Pre_Crawford.Crawford_Incident       i
# MAGIC   JOIN MyMI_Pre_Crawford.Crawford_Claimant cm ON i.IncidentID  = cm.IncidentID
# MAGIC   JOIN MyMI_Pre_Crawford.Crawford_Claim     c ON cm.ClaimantID = c.ClaimantID
# MAGIC   JOIN MyMI_Pre_Crawford.Crawford_Movement  m ON m.ClaimID     = c.ClaimID
# MAGIC   JOIN MyMI_Pre_Eclipse.dbo_Policy          p ON p.PolicyId    = IFNULL(i.PolicyID, i.BinderPolicyID)

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_crawfordClaimTransaction").display()

# COMMAND ----------

# MAGIC %md #### 2.1.2.1 Create vw_reconciliationCrawford

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationCrawford AS
# MAGIC 
# MAGIC /* Get RecordCount */
# MAGIC SELECT 
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Count'       AS ReconciliationType,
# MAGIC   'RecordCount' AS ReconciliationColumn,
# MAGIC   'N/A'         AS CurrencyType,
# MAGIC   'N/A'         AS CurrencyCode,
# MAGIC   COUNT(1)      AS SourceValue
# MAGIC FROM
# MAGIC   vw_crawfordClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimOrigCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                    AS ReconciliationType,
# MAGIC   'PaidClaimOrigCcy'       AS ReconciliationColumn,
# MAGIC   'Original Currency'      AS CurrencyType,
# MAGIC   ct.OriginalCcyISO        AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimOrigCcy) AS SourceValue
# MAGIC FROM
# MAGIC   vw_crawfordClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimSettCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                    AS ReconciliationType,
# MAGIC   'PaidClaimSettCcy'       AS ReconciliationColumn,
# MAGIC   'Settlement Currency'    AS CurrencyType,
# MAGIC   ct.OriginalCcyISO        AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimOrigCcy) AS SourceValue
# MAGIC FROM
# MAGIC   vw_crawfordClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get OutstandingClaimOrigCcyMovement */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                                   AS ReconciliationType,
# MAGIC   'OutstandingClaimOrigCcyMovement'       AS ReconciliationColumn,
# MAGIC   'Original Currency'                     AS CurrencyType,
# MAGIC   ct.OriginalCcyISO                       AS CurrencyCode,
# MAGIC   SUM(ct.OutstandingClaimOrigCcyMovement) AS SourceValue
# MAGIC FROM
# MAGIC   vw_crawfordClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7  

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationCrawford").display()

# COMMAND ----------

# MAGIC %md ### 2.1.3 Mountain View

# COMMAND ----------

# MAGIC %md #### 2.1.3.1 Create vw_mountainViewClaimTransaction

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_mountainViewClaimTransaction AS
# MAGIC SELECT
# MAGIC   'Mountain View'         AS SourceSystemName,
# MAGIC   'MyMI_Pre_MountainView' AS SourceSubjectName,
# MAGIC   'MV_ClaimFinancials'    AS SourceEntityName,
# MAGIC   'USD'                   AS OriginalCcyISO,
# MAGIC   cf.PaidClaim            AS PaidClaimOrigCcy,
# MAGIC   (cf.IndemnityOutstandingClaim - cf.IndemnityPaidClaim) + (cf.ExpenseOutstandingClaim - cf.ExpensePaidClaim) AS OutstandingClaimOrigCcyMovement
# MAGIC FROM
# MAGIC   MyMI_Pre_MountainView.MV_ClaimFinancials   cf
# MAGIC   JOIN MyMI_Pre_MountainView.MV_claimdetails cd ON cd.claimRef = cf.claimref
# MAGIC     AND cf.BatchID = cd.InsertedBatchID
# MAGIC WHERE
# MAGIC   UPPER(cd.BritSourceSystem) = 'ECLIPSE'
# MAGIC   AND UPPER(cd.claimstatus) <> 'DELETED'
# MAGIC   AND cf.BatchID            <> 13

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_mountainViewClaimTransaction").display()

# COMMAND ----------

# MAGIC %md #### 2.1.3.2 Create vw_reconciliationMountainView

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationMountainView AS
# MAGIC 
# MAGIC /* Get RecordCount */
# MAGIC SELECT 
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Count'       AS ReconciliationType,
# MAGIC   'RecordCount' AS ReconciliationColumn,
# MAGIC   'N/A'         AS CurrencyType,
# MAGIC   'N/A'         AS CurrencyCode,
# MAGIC   COUNT(1)      AS SourceValue
# MAGIC FROM
# MAGIC   vw_mountainViewClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimOrigCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                    AS ReconciliationType,
# MAGIC   'PaidClaimOrigCcy'       AS ReconciliationColumn,
# MAGIC   'Original Currency'      AS CurrencyType,
# MAGIC   ct.OriginalCcyISO        AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimOrigCcy) AS SourceValue
# MAGIC FROM
# MAGIC   vw_mountainViewClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimSettCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                    AS ReconciliationType,
# MAGIC   'PaidClaimSettCcy'       AS ReconciliationColumn,
# MAGIC   'Settlement Currency'    AS CurrencyType,
# MAGIC   ct.OriginalCcyISO        AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimOrigCcy) AS SourceValue
# MAGIC FROM
# MAGIC   vw_mountainViewClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get OutstandingClaimOrigCcyMovement */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                                   AS ReconciliationType,
# MAGIC   'OutstandingClaimOrigCcyMovement'       AS ReconciliationColumn,
# MAGIC   'Original Currency'                     AS CurrencyType,
# MAGIC   ct.OriginalCcyISO                       AS CurrencyCode,
# MAGIC   SUM(ct.OutstandingClaimOrigCcyMovement) AS SourceValue
# MAGIC FROM
# MAGIC   vw_mountainViewClaimTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7  

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationMountainView").display()

# COMMAND ----------

# MAGIC %md ### 2.1.4 Sequel Claims

# COMMAND ----------

# MAGIC %md #### 2.1.4.1 Create vw_SequelClaimsTransaction

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_SequelClaimsTransaction AS 
# MAGIC WITH 
# MAGIC SequelClaims1 AS (
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
# MAGIC     ROW_NUMBER() OVER (PARTITION BY SCF.MovementRef, PL.EclipsePolicyLIneID, SCF.movementTypeId,
# MAGIC                                     SCF.VelocityClaimTransactionSequence, SCF.SplitCategory_ID ORDER BY SCF.TransactionDate, SCF.valueAtBroker_MVMT) AS RowNumber,
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
# MAGIC ),
# MAGIC SequelClaims2 AS (
# MAGIC   SELECT
# MAGIC     SCF.Claim_id AS ClaimID,
# MAGIC     IFNULL(PL.EclipsePolicyLIneID, 0) + 1000000 AS ClaimLineID,
# MAGIC     CAST(CONCAT(CAST(SCF.Claim_id AS string),
# MAGIC                 CAST(ROW_NUMBER() OVER (PARTITION BY IFNULL(PL.EclipsePolicyLIneID, 0) ORDER BY Claim_id, PL.EclipsePolicyID, PL.EclipsePolicyLIneID) AS string)) AS INT) AS MovementID,
# MAGIC     IFNULL(PL.EclipsePolicyLIneID, 0) + 1000000 AS ClaimLineShareID,
# MAGIC     IFNULL(PL.EclipsePolicyID, 0) AS EclipsePolicyID,
# MAGIC     IFNULL(PL.EclipsePolicyLIneID, 0) AS EclipsePolicyLIneID,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY PL.EclipsePolicyLIneID, Claim_id ORDER BY Claim_id, PL.EclipsePolicyLIneID) AS RowNumber,    
# MAGIC     CASE WHEN PL.LineStatus = 'Signed' THEN PL.SignedLinePct / 100
# MAGIC          WHEN PL.LineStatus = 'Written' THEN PL.WrittenLine / 100
# MAGIC          ELSE 0
# MAGIC     END AS LinePct,    
# MAGIC     'USD' AS OriginalCcyISO,
# MAGIC     'USD' AS SettlementCcyISO,
# MAGIC     0 AS PaidClaimOrigCcy,
# MAGIC     0 AS PaidClaimSettCcy,
# MAGIC     0 AS OutstandingClaimOrigCcyMovement
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         DISTINCT Claim_id,
# MAGIC         PolicyID,
# MAGIC         ClaimRef
# MAGIC       FROM
# MAGIC         MyMI_Abst_Eclipse.stg30_sequelclaimsdetails
# MAGIC     ) SCF
# MAGIC     JOIN MyMI_Abst_Eclipse.stg10_Policy P ON P.PolicyId = SCF.PolicyID
# MAGIC     LEFT JOIN MyMI_Abst_Eclipse.stg20_PolicyLine PL ON PL.EclipsePolicyID = P.PolicyId
# MAGIC     LEFT JOIN MyMI_Abst_SequelClaims.Stg10_ClaimTransactions scf2 ON UPPER(scf2.ClaimRef) = UPPER(SCF.Claimref)
# MAGIC   WHERE
# MAGIC     UPPER(PL.LineStatus) IN ('SIGNED', 'WRITTEN')
# MAGIC     AND scf2.ClaimRef IS NULL
# MAGIC ),
# MAGIC SequelClaimsTransaction AS (
# MAGIC   SELECT 
# MAGIC     ClaimID,
# MAGIC     ClaimLineID,
# MAGIC     MovementID,
# MAGIC     ClaimLineShareID,
# MAGIC     EclipsePolicyID,
# MAGIC     EclipsePolicyLIneID,
# MAGIC     RowNumber,
# MAGIC     LinePct,
# MAGIC     OriginalCcyISO,
# MAGIC     SettlementCcyISO,
# MAGIC     CAST(PaidClaimOrigCcy * LinePct AS DECIMAL(19, 2)) AS PaidClaimOrigCcy,
# MAGIC     CAST(PaidClaimSettCcy * LinePct AS DECIMAL(19, 2)) AS PaidClaimSettCcy,
# MAGIC     CAST(OutstandingClaimOrigCcyMovement * LinePct AS DECIMAL(19, 2)) OutstandingClaimOrigCcyMovement
# MAGIC   FROM SequelClaims1
# MAGIC   UNION 
# MAGIC   SELECT * FROM SequelClaims2
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Sequel Claims'         AS SourceSystemName,
# MAGIC   'MyMI_Pre_SequelClaims' AS SourceSubjectName,
# MAGIC   'dbo_MovementCWTSplit'  AS SourceEntityName,
# MAGIC   sct.*
# MAGIC FROM SequelClaimsTransaction sct
# MAGIC -- WHERE RowNumber = 1
# MAGIC ;

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_SequelClaimsTransaction").display()

# COMMAND ----------

# MAGIC %md #### 2.1.4.2 Create vw_reconciliationSequelClaims

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationSequelClaims AS
# MAGIC 
# MAGIC /* Get RecordCount */
# MAGIC SELECT 
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Count'       AS ReconciliationType,
# MAGIC   'RecordCount' AS ReconciliationColumn,
# MAGIC   'N/A'         AS CurrencyType,
# MAGIC   'N/A'         AS CurrencyCode,
# MAGIC   COUNT(1)      AS SourceValue
# MAGIC FROM
# MAGIC   vw_SequelClaimsTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimOrigCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                    AS ReconciliationType,
# MAGIC   'PaidClaimOrigCcy'       AS ReconciliationColumn,
# MAGIC   'Original Currency'      AS CurrencyType,
# MAGIC   ct.OriginalCcyISO        AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimOrigCcy) AS SourceValue
# MAGIC FROM
# MAGIC   vw_SequelClaimsTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimSettCcy */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                    AS ReconciliationType,
# MAGIC   'PaidClaimSettCcy'       AS ReconciliationColumn,
# MAGIC   'Settlement Currency'    AS CurrencyType,
# MAGIC   ct.SettlementCcyISO      AS CurrencyCode,
# MAGIC   SUM(ct.PaidClaimSettCcy) AS SourceValue
# MAGIC FROM
# MAGIC   vw_SequelClaimsTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get OutstandingClaimOrigCcyMovement */
# MAGIC SELECT
# MAGIC   ct.SourceSystemName,
# MAGIC   ct.SourceSubjectName,
# MAGIC   ct.SourceEntityName,
# MAGIC   'Sum'                                   AS ReconciliationType,
# MAGIC   'OutstandingClaimOrigCcyMovement'       AS ReconciliationColumn,
# MAGIC   'Original Currency'                     AS CurrencyType,
# MAGIC   ct.OriginalCcyISO                       AS CurrencyCode,
# MAGIC   SUM(ct.OutstandingClaimOrigCcyMovement) AS SourceValue
# MAGIC FROM
# MAGIC   vw_SequelClaimsTransaction ct
# MAGIC GROUP BY
# MAGIC   1, 2, 3, 7    

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationSequelClaims").display()

# COMMAND ----------

# MAGIC %md ### 2.1.5 Create vw_reconciliationSource

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationSource AS
# MAGIC 
# MAGIC SELECT * FROM vw_reconciliationEclipse      UNION ALL
# MAGIC SELECT * FROM vw_reconciliationCrawford     UNION ALL
# MAGIC SELECT * FROM vw_reconciliationMountainView UNION ALL
# MAGIC SELECT * FROM vw_reconciliationSequelClaims

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationSource").display()

# COMMAND ----------

# MAGIC %md ## 2.2 Destination

# COMMAND ----------

# MAGIC %md ### 2.2.1 Create vw_reconciliationFactClaimTransactionEclipse

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationFactClaimTransactionEclipse AS
# MAGIC 
# MAGIC /* Get RecordCount */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'          AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionEclipse' AS DestinationEntityName,
# MAGIC   'Count'                       AS ReconciliationType,
# MAGIC   'RecordCount'                 AS ReconciliationColumn,
# MAGIC   'N/A'                         AS CurrencyType,
# MAGIC   'N/A'                         AS CurrencyCode,
# MAGIC   COUNT(1)                      AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionEclipse ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimOrigCcy */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'          AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionEclipse' AS DestinationEntityName,
# MAGIC   'Sum'                         AS ReconciliationType,  
# MAGIC   'PaidClaimOrigCcy'            AS ReconciliationColumn,
# MAGIC   'Original Currency'           AS CurrencyType,
# MAGIC   REPLACE(`FK_DWH_DimCurrency:DWH_DimOriginalCurrency`, 'MDSCurrencyISOCode=', '') CurrencyCode,
# MAGIC   SUM(PaidClaimOrigCcy)         AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionEclipse ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimSettCcy */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'          AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionEclipse' AS DestinationEntityName,
# MAGIC   'Sum'                         AS ReconciliationType,
# MAGIC   'PaidClaimSettCcy'            AS ReconciliationColumn,
# MAGIC   'Settlement Currency'         AS CurrencyType,
# MAGIC   REPLACE(`FK_DWH_DimCurrency:DWH_DimSettlementCurrency`, 'MDSCurrencyISOCode=', '') CurrencyCode,
# MAGIC   SUM(PaidClaimSettCcy)         AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionEclipse ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7
# MAGIC   
# MAGIC UNION ALL  
# MAGIC   
# MAGIC /* Get OutstandingClaimOrigCcyMovement */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'                 AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionEclipse'        AS DestinationEntityName,
# MAGIC   'Sum'                                AS ReconciliationType,
# MAGIC   'OutstandingClaimOrigCcyMovement'    AS ReconciliationColumn,
# MAGIC   'Original Currency'                  AS CurrencyType,
# MAGIC   REPLACE(`FK_DWH_DimCurrency:DWH_DimOriginalCurrency`, 'MDSCurrencyISOCode=', '') CurrencyCode,
# MAGIC   SUM(OutstandingClaimOrigCcyMovement) AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionEclipse ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationFactClaimTransactionEclipse").display()

# COMMAND ----------

# MAGIC %md ### 2.2.2 Create vw_reconciliationFactClaimTransactionDWH

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationFactClaimTransactionDWH AS
# MAGIC 
# MAGIC /* Get RecordCount */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'      AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionDWH' AS DestinationEntityName,
# MAGIC   'Count'                   AS ReconciliationType,
# MAGIC   'RecordCount'             AS ReconciliationColumn,
# MAGIC   'N/A'                     AS CurrencyType,
# MAGIC   'N/A'                     AS CurrencyCode,
# MAGIC   COUNT(1)                  AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionDWH ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7
# MAGIC   
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimOrigCcy */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'      AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionDWH' AS DestinationEntityName,
# MAGIC   'Sum'                     AS ReconciliationType,  
# MAGIC   'PaidClaimOrigCcy'        AS ReconciliationColumn,
# MAGIC   'Original Currency'       AS CurrencyType,
# MAGIC   REPLACE(`FK_DWH_DimCurrency:DWH_DimOriginalCurrency`, 'MDSCurrencyISOCode=', '') CurrencyCode,
# MAGIC   SUM(PaidClaimOrigCcy)     AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionDWH ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC /* Get PaidClaimSettCcy */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'      AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionDWH' AS DestinationEntityName,
# MAGIC   'Sum'                     AS ReconciliationType,
# MAGIC   'PaidClaimSettCcy'        AS ReconciliationColumn,
# MAGIC   'Settlement Currency'     AS CurrencyType,
# MAGIC   REPLACE(`FK_DWH_DimCurrency:DWH_DimSettlementCurrency`, 'MDSCurrencyISOCode=', '') CurrencyCode,
# MAGIC   SUM(PaidClaimSettCcy)     AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionDWH ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7
# MAGIC   
# MAGIC UNION ALL  
# MAGIC   
# MAGIC /* Get OutstandingClaimOrigCcyMovement */
# MAGIC SELECT
# MAGIC   ss.SourceSystemName,
# MAGIC   'MyMI_Trans_Eclipse'                 AS DestinationSubjectName,
# MAGIC   'FactClaimTransactionDWH'            AS DestinationEntityName,
# MAGIC   'Sum'                                AS ReconciliationType,
# MAGIC   'OutstandingClaimOrigCcyMovement'    AS ReconciliationColumn,
# MAGIC   'Original Currency'                  AS CurrencyType,
# MAGIC   REPLACE(`FK_DWH_DimCurrency:DWH_DimOriginalCurrency`, 'MDSCurrencyISOCode=', '') CurrencyCode,
# MAGIC   SUM(OutstandingClaimOrigCcyMovement) AS DestinationValue
# MAGIC FROM
# MAGIC   MyMI_Trans_Eclipse.FactClaimTransactionDWH ct
# MAGIC   JOIN mymi_trans_mdm.dimsourcesystem ss ON ct.FK_DWH_DimSourceSystem = ss.BusinessKey
# MAGIC GROUP BY
# MAGIC   1, 7   

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationFactClaimTransactionDWH").display()

# COMMAND ----------

# MAGIC %md ### 2.2.3 Create vw_reconciliationDestination

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reconciliationDestination AS
# MAGIC 
# MAGIC SELECT * FROM vw_reconciliationFactClaimTransactionEclipse UNION ALL
# MAGIC SELECT * FROM vw_reconciliationFactClaimTransactionDWH

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationDestination").display()

# COMMAND ----------

# MAGIC %md ## 2.3 Reconciliation

# COMMAND ----------

# MAGIC %md ### 2.3.1 Create Entity/Subject View

# COMMAND ----------

str_sqlQuery = f"""
  SELECT
    s.SubjectID,
    s.Name AS SubjectName,
    e.EntityID,
    e.Name AS EntityName
  FROM
    Metadata.Entity       e
    JOIN Metadata.Subject s ON e.SubjectID = s.SubjectID
  WHERE
    s.Name     IN ({str_sourceSubjectName}, {str_destinationSubjectName})
    AND e.Name IN ({str_sourceEntityName} , {str_destinationEntityName})
"""

# Create Entity View
fn_getSqlDataset(str_sqlQuery).createOrReplaceTempView("vw_entity")

# Create Subject View
spark.sql("SELECT DISTINCT SubjectID, SubjectName FROM vw_entity").createOrReplaceTempView("vw_subject")

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_entity").display()

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_subject").display()

# COMMAND ----------

# MAGIC %md ### 2.3.2 Create Reconciliation Output

# COMMAND ----------

# Create Reconciliation DataFrame
df_reconciliation = spark.sql(f"""
  SELECT
    { int_destinationID }                                      AS DestinationID,
    '{ str_destinationName }'                                  AS DestinationName,
    { int_levelID }                                            AS LevelID,
    '{ str_levelName }'                                        AS LevelName,
    '{ str_dataLoadCode }'                                     AS DataLoadCode,    
    '{ str_fileDateTime }'                                     AS FileDateTime,    
    '{int_dataMartLoadID}'                                     AS DataMartLoadID, 
    '{str_dataHistoryCode}'                                    AS DataHistoryCode,
    '{ str_reconciliationDateTime }'                           AS ReconciliationDateTime,
    { int_jobRunID }                                           AS JobRunID,
    '{ str_pipelineRunID }'                                    AS PipelineRunID, 
    '{ int_controlID }'                                        AS ControlID,
    {int_dataMartID}                                           AS DataMartID,
    '{ str_dataMartName }'                                     AS DataMartName,    
    ss.SubjectID                                               AS SourceSubjectID,
    rs.SourceSubjectName,
    es.EntityID                                                AS SourceEntityID,
    rs.SourceEntityName,
    COALESCE(rs.SourceSystemName, rd.SourceSystemName)         AS SourceSystemName,
    sd.SubjectID                                               AS DestinationSubjectID,
    rd.DestinationSubjectName,
    ed.EntityID                                                AS DestinationEntityID,
    rd.DestinationEntityName,
    COALESCE(rs.ReconciliationType  , rd.ReconciliationType)   AS ReconciliationType,
    COALESCE(rs.ReconciliationColumn, rd.ReconciliationColumn) AS ReconciliationColumn,
    COALESCE(rs.CurrencyType, rd.CurrencyType)                 AS CurrencyType,
    COALESCE(rs.CurrencyCode, rd.CurrencyCode)                 AS CurrencyCode,
    CAST(COALESCE(rs.SourceValue, 0) AS DECIMAL(38, 4))        AS SourceValue,
    CAST(COALESCE(rd.DestinationValue, 0) AS DECIMAL(38, 4))   AS DestinationValue,
    { bol_isToleranceRatio }                                   AS IsToleranceRatio
  FROM
    vw_reconciliationSource rs 
    FULL JOIN vw_reconciliationDestination rd ON rs.SourceSystemName = rd.SourceSystemName
      AND rs.ReconciliationColumn = rd.ReconciliationColumn
      AND rs.CurrencyType         = rd.CurrencyType
      AND rs.CurrencyCode         = rd.CurrencyCode
    LEFT JOIN vw_subject ss ON ss.SubjectName = rs.SourceSubjectName
    LEFT JOIN vw_subject sd ON sd.SubjectName = rd.DestinationSubjectName
    LEFT JOIN vw_entity  es ON es.EntityName  = rs.SourceEntityName
    LEFT JOIN vw_entity  ed ON ed.EntityName  = rd.DestinationEntityName
  """)

# Create Reconciliation Output DataFrame 
df_reconciliationOutput = fn_deriveReconciliationOutput(df_reconciliation, dct_tolerance) 

# COMMAND ----------

if bol_debugRun: 
  df_reconciliationOutput.display()

# COMMAND ----------

# MAGIC %md # 3. Post process

# COMMAND ----------

# MAGIC %md ## 3.1 Write Reconciliation Output

# COMMAND ----------

# df_reconciliationOutput.coalesce(1).write.mode("overwrite").parquet(str_filePath)