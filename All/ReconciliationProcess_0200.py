# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationProcess_0200
# MAGIC * **Description :** Reconciliation process for Raw to Cleansed.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 11/02/2022 | Raja Murugan | Reconciliation process for Raw to Cleansed. |

# COMMAND ----------

# MAGIC %md # 1. Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# MAGIC %run /Datalib/Common/Master

# COMMAND ----------

# MAGIC %md ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("DebugRun"               , "True"                                 , "DebugRun")
dbutils.widgets.text("DestinationID"          , "2"                                    , "DestinationID")
dbutils.widgets.text("DestinationName"        , "Eclipse"                              , "DestinationName")
dbutils.widgets.text("LevelID"                , "0200"                                 , "LevelID")
dbutils.widgets.text("LevelName"              , "Raw to Cleansed"                      , "LevelName")
dbutils.widgets.text("DataLoadCode"           , "20220210_2200"                        , "DataLoadCode")
dbutils.widgets.text("FileDateTime"           , "20220210_2200"                        , "FileDateTime")
dbutils.widgets.text("DataMartLoadID"         , ""                                     , "DataMartLoadID")
dbutils.widgets.text("DataHistoryCode"        , ""                                     , "DataHistoryCode")
dbutils.widgets.text("ReconciliationDateTime" , "20220211_0900"                        , "ReconciliationDateTime")
dbutils.widgets.text("JobRunID"               , "14521"                                , "JobRunID")
dbutils.widgets.text("PipelineRunID"          , "8a5323bf-66d4-4801-9ed0-544b432fbe68" , "PipelineRunID")
dbutils.widgets.text("ControlID"              , "900"                                  , "ControlID")
dbutils.widgets.text("DataMartID"             , ""                                     , "DataMartID")
dbutils.widgets.text("DataMartName"           , ""                                     , "DataMartName")
dbutils.widgets.text("SourceSubjectID"        , "8"                                    , "SourceSubjectID")
dbutils.widgets.text("SourceSubjectName"      , "Eclipse"                              , "SourceSubjectName")
dbutils.widgets.text("SourceEntityID"         , "539"                                  , "SourceEntityID")
dbutils.widgets.text("SourceEntityName"       , "dbo_ClaimLine"                        , "SourceEntityName")
dbutils.widgets.text("DestinationSubjectID"   , "7"                                    , "DestinationSubjectID")
dbutils.widgets.text("DestinationSubjectName" , "Eclipse"                              , "DestinationSubjectName")
dbutils.widgets.text("DestinationEntityID"    , "1606"                                 , "DestinationEntityID")
dbutils.widgets.text("DestinationEntityName"  , "dbo_ClaimLine"                        , "DestinationEntityName")
dbutils.widgets.text("LoadType"               , "Incremental"                          , "LoadType")
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
int_controlID              = int(dbutils.widgets.get("ControlID"))
int_dataMartID             = int(dbutils.widgets.get("DataMartID"))     if dbutils.widgets.get("DataMartID")   else None
str_dataMartName           = dbutils.widgets.get("DataMartName")        if dbutils.widgets.get("DataMartName") else 'N/A'
int_sourceSubjectID        = int(dbutils.widgets.get("SourceSubjectID"))
str_sourceSubjectName      = dbutils.widgets.get("SourceSubjectName")
int_sourceEntityID         = int(dbutils.widgets.get("SourceEntityID"))
str_sourceEntityName       = dbutils.widgets.get("SourceEntityName")
int_destinationSubjectID   = int(dbutils.widgets.get("DestinationSubjectID"))
str_destinationSubjectName = dbutils.widgets.get("DestinationSubjectName")
int_destinationEntityID    = int(dbutils.widgets.get("DestinationEntityID"))
str_destinationEntityName  = dbutils.widgets.get("DestinationEntityName")
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
str_filePath               = f"{str_reconciliationPath}/datalake/temp/{str_destinationName}/{int_levelID}/{str_dataLoadCode}/{str_destinationSubjectName}_{str_destinationEntityName}"

# COMMAND ----------

# MAGIC %md # 2. Process

# COMMAND ----------

# MAGIC %md ## 2.1 Source

# COMMAND ----------

# Create vw_reconciliationRaw
fn_getTimestampedFileDataFrame("Raw", str_sourceSubjectName, "Internal", str_sourceEntityName, str_fileDateTime, str_fileDateTime).createOrReplaceTempView("vw_reconciliationRaw")

# Create vw_reconciliationSource
spark.sql(f"SELECT COUNT(1) AS SourceValue FROM vw_reconciliationRaw").createOrReplaceTempView("vw_reconciliationSource")

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationSource").display()

# COMMAND ----------

# MAGIC %md ## 2.2 Destination

# COMMAND ----------

# Create vw_reconciliationCleansed
fn_getTimestampedFileDataFrame("Cleansed", str_destinationSubjectName, "Internal", str_destinationEntityName, str_fileDateTime, str_fileDateTime).createOrReplaceTempView("vw_reconciliationCleansed")

# Create vw_reconciliationDestination
spark.sql(f"SELECT COUNT(1) AS DestinationValue FROM vw_reconciliationCleansed").createOrReplaceTempView("vw_reconciliationDestination")

# COMMAND ----------

if bol_debugRun:
  spark.sql("SELECT * FROM vw_reconciliationDestination").display()

# COMMAND ----------

# MAGIC %md ## 2.3 Reconciliation

# COMMAND ----------

# Create Reconciliation DataFrame
df_reconciliation = spark.sql(f"""
  SELECT
    { int_destinationID }                                    AS DestinationID,
    '{ str_destinationName }'                                AS DestinationName,
    { int_levelID }                                          AS LevelID,
    '{ str_levelName }'                                      AS LevelName,
    '{ str_dataLoadCode }'                                   AS DataLoadCode,
    '{ str_fileDateTime }'                                   AS FileDateTime,
    '{int_dataMartLoadID}'                                   AS DataMartLoadID,
    '{str_dataHistoryCode}'                                  AS DataHistoryCode,
    '{ str_reconciliationDateTime }'                         AS ReconciliationDateTime,
    { int_jobRunID }                                         AS JobRunID,
    '{ str_pipelineRunID }'                                  AS PipelineRunID,
    '{ int_controlID }'                                      AS ControlID,
    '{ int_dataMartID }'                                     AS DataMartID,
    '{ str_dataMartName }'                                   AS DataMartName,
    { int_sourceSubjectID }                                  AS SourceSubjectID,
    '{ str_sourceSubjectName }'                              AS SourceSubjectName,
    { int_sourceEntityID }                                   AS SourceEntityID,
    '{ str_sourceEntityName }'                               AS SourceEntityName,
    'N/A'                                                    AS SourceSystemName,
    { int_destinationSubjectID }                             AS DestinationSubjectID,
    '{ str_destinationSubjectName }'                         AS DestinationSubjectName,
    { int_destinationEntityID }                              AS DestinationEntityID,
    '{ str_destinationEntityName }'                          AS DestinationEntityName,
    'Count'                                                  AS ReconciliationType,
    'RecordCount'                                            AS ReconciliationColumn,
    'N/A'                                                    AS CurrencyType,
    'N/A'                                                    AS CurrencyCode,
    CAST(COALESCE(rs.SourceValue, 0) AS DECIMAL(38, 4))      AS SourceValue,
    CAST(COALESCE(rd.DestinationValue, 0) AS DECIMAL(38, 4)) AS DestinationValue,
    { bol_isToleranceRatio }                                 AS IsToleranceRatio
  FROM
    vw_reconciliationSource rs 
    FULL JOIN vw_reconciliationDestination rd
  """)

# Create Reconciliation Output DataFrame 
df_reconciliationOutput = fn_deriveReconciliationOutput(df_reconciliation, dct_tolerance) 

# COMMAND ----------

if bol_debugRun: 
  df_reconciliationOutput.display()

# COMMAND ----------

# MAGIC %md # 3. Postprocess

# COMMAND ----------

# MAGIC %md ## 3.1 Write Reconciliation DataFrame

# COMMAND ----------

df_reconciliationOutput.coalesce(1).write.mode("overwrite").parquet(str_filePath)