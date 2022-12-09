# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** Ingestion Delta
# MAGIC * **Description :** Ingest Staging (Source) Data to Delta Lake table (Target).
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 31/05/2022 | Raja Murugan | Ingest Staging (Source) Data to Delta Lake table (Target). |
# MAGIC | 27/06/2022 | Raja Murugan | Framework Improvement |
# MAGIC | 14/07/2022 | Anna Cummins | Refactor to increase Pyspark usage and reduce to single notebook for all x3 delta layer processes |
# MAGIC | 25/08/2022 | Anna Cummins | Handling for record deletions for incremental loads |
# MAGIC | 03/11/2022 | Tahir Khan   | Extend to SCD2 logic for incremental loads in Bronze layer |

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Preprocess

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Include Common

# COMMAND ----------

from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from delta.tables import *
from pyspark.sql.functions import desc, row_number, dense_rank, col, lit, lag, expr,explode, count, when,coalesce
from ast import literal_eval

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("DebugRun"                        , "True"                                            , "DebugRun")
dbutils.widgets.text("SystemLoadID"                    , "1032022061701"                                   , "SystemLoadID")
dbutils.widgets.text("SourcePath"                      , "/bronze/underwriting/Internal/Eclipse/Staging"   , "SourcePath")
dbutils.widgets.text("TargetPath"                      , "/bronze/underwriting/Internal/Eclipse/DeltaLake" , "TargetPath")
dbutils.widgets.text("TargetDatabaseName"              , "EclipseBronze"                                   , "TargetDatabaseName")
dbutils.widgets.text("ObjectRunID"                     , "1"                                               , "ObjectRunID")
dbutils.widgets.text("ObjectID"                        , "1010001"                                         , "ObjectID")
dbutils.widgets.text("ObjectName"                      , "dbo_Policy"                                      , "ObjectName")
dbutils.widgets.text("LoadType"                        , "Incremental"                                     , "LoadType")
dbutils.widgets.text("UniqueColumn"                    , "PolicyID"                                        , "UniqueColumn")
dbutils.widgets.text("PartitionColumn"                 , ""                                                , "PartitionColumn")
dbutils.widgets.text("WatermarkValue"                  , "1022022061701"                                   , "WatermarkValue")
dbutils.widgets.text("BronzeKeepHistoryDateColumn"     , "LastUpd,InsDate"                                 , "BronzeKeepHistoryDateColumn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Set Variables

# COMMAND ----------

# Parameter Variables
bol_debugRun                    = eval(dbutils.widgets.get("DebugRun"))
int_systemLoadID                = int(dbutils.widgets.get("SystemLoadID"))
str_sourcePath                  = dbutils.widgets.get("SourcePath")
str_targetPath                  = dbutils.widgets.get("TargetPath")
str_targetDatabaseName          = dbutils.widgets.get("TargetDatabaseName")
int_objectRunID                 = int(dbutils.widgets.get("ObjectRunID"))
int_objectID                    = int(dbutils.widgets.get("ObjectID"))
str_objectName                  = dbutils.widgets.get("ObjectName")
str_loadType                    = dbutils.widgets.get("LoadType")
str_uniqueColumn                = dbutils.widgets.get("UniqueColumn")
str_BronzeKeepHistoryDateColumn = dbutils.widgets.get("BronzeKeepHistoryDateColumn")
str_partitionColumn             = dbutils.widgets.get("PartitionColumn")
int_watermarkValue              = int(dbutils.widgets.get("WatermarkValue"))

# Process Variables
str_mountPath            = "/mnt"
str_sourceFilePath       = f"{str_mountPath}{str_sourcePath}/{str_objectName}"
str_targetTablePath      = f"{str_mountPath}{str_targetPath}/{str_objectName}"
str_targetTableName      = str_objectName
str_sourceViewName       = f"vw_{str_objectName}"
str_mergeViewName        =  f"vw_{str_objectName}_merge"
str_load                 = "NotStarted"
str_layer                = (str_targetPath.split("/")[1]).split("-")[-1].title() 
dt_validTo               = datetime.strptime("9999-12-31 23:59:59.000", "%Y-%m-%d %H:%M:%S.%f")
dt_defaultValidFrom      = datetime.strptime("1900-01-01 00:00:00.000", "%Y-%m-%d %H:%M:%S.%f")

# COMMAND ----------

x = dbutils.fs.ls(str_sourceFilePath)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Process

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Load latest data

# COMMAND ----------

try:
      df = (spark
      .read
      .option("mergeSchema","true") 
      .parquet(str_sourceFilePath) 
      .filter(col("SystemLoadID") > int_watermarkValue)
      #.filter(col("SystemLoadID") == int_watermarkValue)
      .select("*",
              lit(int_systemLoadID).alias(str_layer+"SystemLoadID")
             )
      .withColumnRenamed("SystemLoadID", str_layer+"StagingSystemLoadID")
     )
except: 
    # for JSON file types we need to explode and select the data
    df = (spark
          .read
          .option("mergeSchema","true") 
          .json(str_sourceFilePath) 
          .filter(col("SystemLoadID") > int_watermarkValue)
          .withColumn("data", explode("data"))
          .select("data.*", "SystemLoadID")
         )
    
if bol_debugRun:
    display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Prep the process data
# get a list of system load ids to be processed - this will be executed chronologically
arr_systemLoadIDs = [int(x.name.split("=")[1].split("/")[0]) for x in dbutils.fs.ls(str_sourceFilePath)]
arr_unprocessedSystemLoadIDs = [x for x in arr_systemLoadIDs if x > int_watermarkValue]
arr_unprocessedSystemLoadIDs.sort()

# identify the latest data per unique column
arr_uniqueColumn = [c for c in str_uniqueColumn.split(",") if c != '']

# identify the change tracking date cols
arr_BronzeKeepHistoryDateColumns = [c for c in str_BronzeKeepHistoryDateColumn.split(",")]

# create window for identifying record EndDate
window_SCD2 = Window.partitionBy(arr_uniqueColumn).orderBy(desc("DP_ValidFromDateUTC"))

# COMMAND ----------

# DBTITLE 1,Finalise merge df
# identify the latest record, and update EndDates

if(str_BronzeKeepHistoryDateColumn):
    arr_BronzeKeepHistoryDateColumns = arr_BronzeKeepHistoryDateColumns + ["tmp_DefaultValidFromDate"]
    df_merge = (df
                .withColumn("tmp_DefaultValidFromDate", lit(dt_defaultValidFrom).cast(StringType()))
                .withColumn("DP_ValidFromDateUTC", coalesce(*arr_BronzeKeepHistoryDateColumns).cast(TimestampType()))    
                .withColumn("DP_ValidToDateUTC", coalesce(lag("DP_ValidFromDateUTC", 1).over(window_SCD2), lit(dt_validTo).cast(TimestampType())))   
                .drop("tmp_DefaultValidFromDate")
               )
else:
    df_merge = (df
                .filter(col(str_layer+"StagingSystemLoadID")==arr_unprocessedSystemLoadIDs[-1])
               )
    
if(bol_debugRun):
    display(df_merge.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Initial Load

# COMMAND ----------

# Check if Delta Lake table exists
bol_dltExists = DeltaTable.isDeltaTable(spark, str_targetTablePath)

if(bol_dltExists == False):
# Create Database if not exist, for Delta Lake tables
    spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {str_targetDatabaseName}  
    COMMENT '{str_layer} database {str_targetDatabaseName}'
  """).display()

# Drop table from hive store if exists
    spark.sql(f"""
       DROP TABLE IF EXISTS {str_targetDatabaseName}.{str_targetTableName}
    """).display()
    
# Write the table to ADLS and save in database
    (df_merge
     .write
     .format("delta")
     .option("path",str_targetTablePath)
     .option("comment", lit(f"{str_layer} table {str_targetTableName}")) \
     .saveAsTable(f"{str_targetDatabaseName}.{str_targetTableName}")
    )
    
    str_load = "Initial"
    print("Initial Load Completed")

# COMMAND ----------

if bol_debugRun and str_load == 'Initial':
  spark.sql(f"DESCRIBE DATABASE {str_targetDatabaseName}").display()
  spark.sql(f"DESCRIBE TABLE {str_targetDatabaseName}.{str_targetTableName}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Full Load

# COMMAND ----------

if str_load == "NotStarted" and str_loadType == "FullRefresh":
  
  (df_merge
   .write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", True)
   .option("path",str_targetTablePath)
   .saveAsTable(f"{str_targetDatabaseName}.{str_targetTableName}")
  )
  
  str_load = "Full"
  print("Full Load Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Incremental Load

# COMMAND ----------

# Useful ref doc: https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables

# COMMAND ----------

# create join condition for identifying updated records
str_mergeCondition = " AND ".join([ "Base." + str + " = mergeKey" + str for str in arr_uniqueColumn])

print(str_mergeCondition)

# COMMAND ----------

# create condition for identifying values which differ
arr_noneSCDColumns = ["BronzeStagingSystemLoadID", "BronzeSystemLoadID", "SYS_CHANGE_OPERATION", "DP_ValidFromDateUTC", "DP_ValidToDateUTC"] + arr_uniqueColumn
arr_comparisonColumns = df.drop(*arr_noneSCDColumns).columns
str_mergeValues = "Base.DP_ValidToDateUTC = '9999-12-31T23:59:59.000+0000' AND (" + " OR ".join(["((Base." + str + " <> Updates." + str + ") OR (Base." + str + " IS NULL AND Updates." + str + " IS NOT NULL) OR (Base." + str + " IS NOT NULL AND Updates." + str + " IS NULL))" for str in arr_comparisonColumns]) + ")"

print(arr_noneSCDColumns)
print('')
print(arr_comparisonColumns)
print('')
print(str_mergeValues)
print('')

# COMMAND ----------

# now create the insert when not matched dict
arr_insertValues = df_merge.drop("DP_ValidToDateUTC").columns
str_insertValues = "{" + ", ".join(["'" + str + "': 'Updates." + str + "'" for str in arr_insertValues]) + ", 'DP_ValidToDateUTC' : 'Updates.DP_ValidToDateUTC'}"
dict_insertValues = literal_eval(str_insertValues)  

print(arr_insertValues)
print('')
print(str_insertValues)
print('')
print(dict_insertValues)
print('')

# COMMAND ----------

arr_insertMergeKeys = ["NULL AS mergeKey" + str for str in arr_uniqueColumn] + ["Updates.*"]
arr_updateMergeKeys = ["Updates." + str + " AS mergeKey" + str for str in arr_uniqueColumn] + ["*"]

print(arr_insertMergeKeys)
print('')
print(arr_updateMergeKeys)
print('')

# COMMAND ----------

int_processLoadID = 1022022101002

# load current version of delta
dlt_base = DeltaTable.forPath(spark, str_targetTablePath)

df_base = dlt_base.toDF()
df_updates = df_merge.filter(col(str_layer+"StagingSystemLoadID") == int_processLoadID)

# get rows for updates to existing data
dlt_recordsToInsert = (df_updates.alias("Updates")
                       .join(df_base.alias("Base"), arr_uniqueColumn)
                       .where(str_mergeValues)
                       .selectExpr(arr_insertMergeKeys)
                      )

display(df_updates)
display(dlt_recordsToInsert)

# COMMAND ----------

# stage these alongside new data
arr_updateColumns = dlt_recordsToInsert.columns
dlt_stagedUpdates = (dlt_recordsToInsert
                     .union(df_updates.alias("Updates")
                            .selectExpr(arr_updateMergeKeys)
                            .select(*arr_updateColumns)
                           )
                    )

print(arr_updateColumns)
display(dlt_stagedUpdates)

# COMMAND ----------

# Now upsert into the delta table
(dlt_base.alias("Base")
 .merge(dlt_stagedUpdates.alias("Updates"), str_mergeCondition)
 .whenMatchedUpdate(
     condition=str_mergeValues,
     set={
          "DP_ValidToDateUTC": expr("Updates.DP_ValidFromDateUTC")
         }
 )
 .whenNotMatchedInsert(
     values = dict_insertValues
 )
 .execute()
)

# COMMAND ----------

if str_load == "NotStarted" and str_loadType == 'Incremental':
    
    # identify deleted records
    #df_merge = df_merge.withColumn("IsDeleted", col("SYS_CHANGE_OPERATION")=="D")
    
    # create join condition for identifying updated records
    str_mergeCondition = " AND ".join([ "Base." + str + " = mergeKey" + str for str in arr_uniqueColumn])
    
    # create condition for identifying values which differ
    arr_noneSCDColumns = ["BronzeStagingSystemLoadID", "BronzeSystemLoadID", "SYS_CHANGE_OPERATION", "DP_ValidFromDateUTC", "DP_ValidToDateUTC"] + arr_uniqueColumn
    arr_comparisonColumns = df.drop(*arr_noneSCDColumns).columns
    str_mergeValues = "Base.DP_ValidToDateUTC = '9999-12-31T23:59:59.000+0000' AND (" + " OR ".join(["((Base." + str + " <> Updates." + str + ") OR (Base." + str + " IS NULL AND Updates." + str + " IS NOT NULL) OR (Base." + str + " IS NOT NULL AND Updates." + str + " IS NULL))" for str in arr_comparisonColumns]) + ")"
    
    # now create the insert when not matched dict
    arr_insertValues = df_merge.drop("DP_ValidToDateUTC").columns
    str_insertValues = "{" + ", ".join(["'" + str + "': 'Updates." + str + "'" for str in arr_insertValues]) + ", 'DP_ValidToDateUTC' : 'Updates.DP_ValidToDateUTC'}"
    dict_insertValues = literal_eval(str_insertValues)        
    
    arr_insertMergeKeys = ["NULL AS mergeKey" + str for str in arr_uniqueColumn] + ["Updates.*"]
    arr_updateMergeKeys = ["Updates." + str + " AS mergeKey" + str for str in arr_uniqueColumn] + ["*"]
    
    # iterate through the unprocessed system load ids 
    for int_processLoadID in arr_unprocessedSystemLoadIDs:
        
        # load current version of delta
        dlt_base = DeltaTable.forPath(spark, str_targetTablePath)
        
        df_base = dlt_base.toDF()
        df_updates = df_merge.filter(col(str_layer+"StagingSystemLoadID") == int_processLoadID)
        
        # get rows for updates to existing data
        dlt_recordsToInsert = (df_updates.alias("Updates")
                               .join(df_base.alias("Base"), arr_uniqueColumn)
                               .where(str_mergeValues)
                               .selectExpr(arr_insertMergeKeys)
                              )
        
        # stage these alongside new data
        arr_updateColumns = dlt_recordsToInsert.columns
        dlt_stagedUpdates = (dlt_recordsToInsert
                             .union(df_updates.alias("Updates")
                                    .selectExpr(arr_updateMergeKeys)
                                    .select(*arr_updateColumns)
                                   )
                            )
        
        # Now upsert into the delta table
        (dlt_base.alias("Base")
         .merge(dlt_stagedUpdates.alias("Updates"), str_mergeCondition)
         .whenMatchedUpdate(
             condition=str_mergeValues,
             set={
                  "DP_ValidToDateUTC": expr("Updates.DP_ValidFromDateUTC")
                 }
         )
         .whenNotMatchedInsert(
             values = dict_insertValues
         )
         .execute()
        )

    str_load = "Merge"  
    print("Merge Load Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Check Load

# COMMAND ----------

if bol_debugRun:
    df_updated = DeltaTable.forPath(spark, str_targetTablePath).toDF()
    display(df_updated.filter(col(str_layer+"SystemLoadID") == int_systemLoadID))
