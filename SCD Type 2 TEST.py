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
# MAGIC | 24/10/2022 | Tahir Khan   | Adding SCD Type 2 Columns and modification of script |

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Preprocess

# COMMAND ----------

#%run ../Common/fn_checkSCDValidity

# COMMAND ----------

# MAGIC %md
# MAGIC # # 1.1 Load Functions and Libraries

# COMMAND ----------

from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from delta.tables import *
from pyspark.sql.functions import desc, row_number, dense_rank, col, lit, lag, expr, count, when, coalesce, date_format
from ast import literal_eval

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.2 Reset Parameters

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("DebugRun"           , "False"                                           , "DebugRun")
dbutils.widgets.text("SystemLoadID"       , "1032022102601"                                   , "SystemLoadID")
dbutils.widgets.text("SourcePath"         , "/bronze/underwriting/Internal/Eclipse/Staging"   , "SourcePath")
dbutils.widgets.text("TargetPath"         , "/bronze/underwriting/Internal/Eclipse/DeltaLake" , "TargetPath")
dbutils.widgets.text("TargetDatabaseName" , "EclipseBronze"                                   , "TargetDatabaseName")
dbutils.widgets.text("ObjectRunID"        , "1"                                               , "ObjectRunID")
dbutils.widgets.text("ObjectID"           , "1010001"                                         , "ObjectID")
dbutils.widgets.text("ObjectName"         , "dbo_Ac"                                          , "ObjectName")
dbutils.widgets.text("LoadType"           , "Incremental"                                     , "LoadType")
dbutils.widgets.text("UniqueColumn"       , "ClaimStatusID"                                   , "UniqueColumn")
dbutils.widgets.text("PartitionColumn"    , ""                                                , "PartitionColumn")
dbutils.widgets.text("WatermarkValue"     , "0"                                               , "WatermarkValue")

# COMMAND ----------

# MAGIC  %md
# MAGIC # 1.4 Set Variables

# COMMAND ----------

# Parameter Variables
bol_debugRun           = eval(dbutils.widgets.get("DebugRun"))
int_systemLoadID       = int(dbutils.widgets.get("SystemLoadID"))
str_sourcePath         = dbutils.widgets.get("SourcePath")
str_targetPath         = dbutils.widgets.get("TargetPath")
str_targetDatabaseName = dbutils.widgets.get("TargetDatabaseName")
int_objectRunID        = int(dbutils.widgets.get("ObjectRunID"))
int_objectID           = int(dbutils.widgets.get("ObjectID"))
str_objectName         = dbutils.widgets.get("ObjectName")
str_loadType           = dbutils.widgets.get("LoadType")
str_uniqueColumn       = dbutils.widgets.get("UniqueColumn")
str_partitionColumn    = dbutils.widgets.get("PartitionColumn")
int_watermarkValue     = int(dbutils.widgets.get("WatermarkValue"))

# Process Variables
str_mountPath          = "/mnt"
str_sourceFilePath     = f"{str_mountPath}{str_sourcePath}/{str_objectName}"
str_targetTablePath    = f"{str_mountPath}{str_targetPath}/{str_objectName}"
str_targetTableName    = str_objectName
str_sourceViewName     = f"vw_{str_objectName}"
str_mergeViewName      = f"vw_{str_objectName}_merge"
str_load               = "NotStarted"
str_layer              = (str_targetPath.split("/")[1]).split("-")[-1].title() 
dt_ValidTo             = datetime.strptime("9999-12-31 23:59:59.999", "%Y-%m-%d %H:%M:%S.%f")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Process

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.1 Load latest data

# COMMAND ----------

# Load the latest data 
df = (spark
      .read
      .option("mergeSchema","true") 
      .parquet(str_sourceFilePath) 
      .filter(col("SystemLoadID") > int_watermarkValue)
      #.filter(col("SystemLoadID") == 1022022102501)
      .select("*",
              lit(int_systemLoadID).alias(str_layer+"SystemLoadID"),
              col("LastUpd").alias("DP_ValidFromDateUTC").cast(TimestampType())
             )
      .withColumnRenamed("SystemLoadID", str_layer+"StagingSystemLoadID")
     )

# COMMAND ----------

# get a list of system load ids to be processed - this will be executed chronologically
arr_systemLoadIDs = [int(x.name.split("=")[1].split("/")[0]) for x in dbutils.fs.ls(str_sourceFilePath)]
arr_unprocessedSystemLoadIDs = [x for x in arr_systemLoadIDs if x > int_watermarkValue]
arr_unprocessedSystemLoadIDs.sort()

# identify the latest data per unique column
arr_uniqueColumn = [c for c in str_uniqueColumn.split(",") if c != '']

# create window for identifying record EndDate
window_SCD2 = Window.partitionBy(arr_uniqueColumn).orderBy(desc("DP_ValidFromDateUTC"))

# COMMAND ----------

# identify the latest record, and update EndDates
if(str_uniqueColumn):
    df_SCD2 = (df
               .withColumn("DP_ValidToDateUTC", coalesce(lag("DP_ValidFromDateUTC", 1).over(window_SCD2), lit(dt_ValidTo)))
              )
else:
    df_SCD2 = (df
               .withColumn("DP_ValidFromDateUTC", lit(dt_ValidTo))
              )
    
if bol_debugRun:
    display(df_SCD2.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.2 Initial Load

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
    (df_SCD2
     .write
     .format("delta")
     .option("path", str_targetTablePath)
     .option("comment", lit(f"{str_layer} table {str_targetTableName}"))
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
# MAGIC # # 2.3 Full Load

# COMMAND ----------

if str_load == "NotStarted" and str_loadType == "FullRefresh":
  
  (df_SCD2
   .filter(col(str_layer+"StagingSystemLoadID")==arr_unprocessedSystemLoadIDs[-1])
   .write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", True)
   .saveAsTable(f"{str_targetDatabaseName}.{str_targetTableName}")
  )
  
  str_load = "Full"
  print("Full Load Completed")


# COMMAND ----------

# MAGIC %md
# MAGIC # 2.4 Incremental Load

# COMMAND ----------

if str_load == "NotStarted" and str_loadType == 'Incremental':
    
    # identify deleted records
    #df_SCD2 = df_SCD2.withColumn("IsDeleted", col("SYS_CHANGE_OPERATION")=="D")
    
    # create join condition for identifying updated records
    str_mergeCondition = " AND ".join([ "Base." + str + " = mergeKey" + str for str in arr_uniqueColumn])
    
    # create condition for identifying values which differ
    arr_noneSCDColumns = ["BronzeStagingSystemLoadID", "BronzeSystemLoadID", "SYS_CHANGE_OPERATION"] + arr_uniqueColumn
    arr_comparisonColumns = df.drop(*arr_noneSCDColumns).columns
    str_mergeValues = "Base.DP_ValidToDateUTC = '9999-12-31T23:59:59.999+0000' AND (" + " OR ".join(["Base." + str + " <> Updates." + str for str in arr_comparisonColumns]) +")"
    
    # now create the insert when not matched dict
    arr_insertValues = df_SCD2.drop("DP_ValidToDateUTC").columns
    str_insertValues = "{" + ", ".join(["'" + str + "': 'Updates." + str + "'" for str in arr_insertValues]) + ", 'DP_ValidToDateUTC' : 'Updates.DP_ValidToDateUTC'}"
    dict_insertValues = literal_eval(str_insertValues)        
    
    arr_insertMergeKeys = ["NULL AS mergeKey" + str for str in arr_uniqueColumn] + ["Updates.*"]
    arr_updateMergeKeys = ["Updates." + str + " AS mergeKey" + str for str in arr_uniqueColumn] + ["*"]
    
    # iterate through the unprocessed system load ids 
    for int_processLoadID in arr_unprocessedSystemLoadIDs:
        
        # load current version of delta
        dlt_base = DeltaTable.forPath(spark, str_targetTablePath)
        
        df_base = dlt_base.toDF()
        df_updates = df_SCD2.filter(col(str_layer+"StagingSystemLoadID") == int_processLoadID)
        
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

# MAGIC  %md
# MAGIC # 2.5 Check Load

# COMMAND ----------

if bol_debugRun:
    df_updated = DeltaTable.forPath(spark, str_targetTablePath).toDF()
    display(df_updated.filter(col(str_layer+"SystemLoadID") == int_systemLoadID))

# COMMAND ----------

display(df_updated)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *from eclipsebronze.dbo_Ac
