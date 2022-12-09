# Databricks notebook source
#    Purpose of this notebook:

#    This is a custom Pre Transform notebook for GXLP.Accrual table to handle hard deletes from the source. Currently this table is enabled for change tracking but hard deletes are #    breaking the feed and hence making the table out of synce. This note book will perform an additional check to this perticular feed as we are getting unique ids for this table in a raw file    seperately. 

# COMMAND ----------

# MAGIC %run /Datalib/Common/Master

# COMMAND ----------

## set parameter
dbutils.widgets.text("EntityName","dbo_ARCUST","EntityName")
dbutils.widgets.text("Subject","MyMI_Pre_Velocity","Subject")
dbutils.widgets.text("Security","Internal","Security")
dbutils.widgets.text("DataMart","MyMI","DataMart")
dbutils.widgets.text("DataMartLoadID","1","DataMartLoadID")
dbutils.widgets.text("InitialDateTime","00000000_0000","InitialDateTime")
dbutils.widgets.text("EndDateTime","99999999_9999","EndDateTime")
dbutils.widgets.text("LoadType","FullRefresh","LoadType")

## set variables
str_entityName      = dbutils.widgets.get("EntityName")
str_subject         = dbutils.widgets.get("Subject")
str_security        = dbutils.widgets.get("Security")
str_dataMart        = dbutils.widgets.get("DataMart")
str_dataMartLoadID  = dbutils.widgets.get("DataMartLoadID")
str_initialDateTime   = dbutils.widgets.get("InitialDateTime")
str_endDateTime     = dbutils.widgets.get("EndDateTime")
str_loadType        = dbutils.widgets.get("LoadType")
## Fix vars
str_pattern = 'Pre'

# COMMAND ----------

# DBTITLE 1,1.1 Set Variables
str_layer         = "Cleansed"
str_databaseName  =  str_subject
str_deltaFilePath = 'dbfs:/mnt/main/Transformed' + "/" + str_subject + "/" + str_security + "/" + str_entityName


# COMMAND ----------

# DBTITLE 1,1.2 Get Entity Details
str_sqlTable = """
  SELECT e.PreTransFilter,SourcePath,IdentifierField,ChangeTrackingJoin
  FROM Metadata.Entity e
  INNER JOIN  Metadata.Subject s
      ON  s.SubjectID = e.SubjectID
  WHERE s.Name = '""" + str_databaseName + """'
  AND e.Enable = 1 
  AND s.Enable = 1
  AND e.Name = '""" + str_entityName + """'
  """

lst_details = fn_queryControlDatabase(str_sqlTable)

# COMMAND ----------

# DBTITLE 1,1.3 Calculating variables
str_predicate = " "

for str_item in lst_details:
  # If Pre Trans filter does not exits 
  if str_item[0] is not None:
    str_predicate     = "WHERE {}".format(str_item[0][:-1])
  # if Source path is empty  
  # Deriving Subject and layer based on the SourcePath
  if str_item[1] is not None:
    source_path_parts = str_item[1].split("\\")
    str_layer         = source_path_parts[0]
    str_subject       = source_path_parts[1]
  else:
    print ("Warning: Entity '" + str_entityName + "' has incorrect metadata i.e. sourcePath")
  # Basic calculations
  str_identifier      = str_item[2]
  str_join            = str_item[3]

# COMMAND ----------

# DBTITLE 1,2 Path Variables for Full Refresh
str_year     = str_endDateTime[0:4]
str_month    = str_endDateTime[0:6]
str_day      = str_endDateTime[0:8]
str_folter   = str_endDateTime[0:11]
str_filename = str_entityName + '_' + str_endDateTime
str_hour     = str(int(str_endDateTime[-4:]))
str_endRange  = str_day + '_' + str_hour

# COMMAND ----------

# DBTITLE 1,Preparing Data 
############################################
## Loading Current Period Full Refresh File
############################################
str_year = str_endDateTime[0:4] 
str_entityNameFile = str_entityName + 'CurrentPeriod'
str_filename = str_entityNameFile + '_' + str_endDateTime
str_filePath = 'dbfs:/mnt/main/' + str_layer + '/' + str_subject + '/' + str_security + '/' + str_entityNameFile +  '/' + str_year + '/' + str_month + '/' + str_day + '/' + str_folter + '/' + str_filename + '.parquet'
df_currentData = spark.read.format('parquet').option("header", "true").load(str_filePath)

############################################
## Loading Historical Static File
############################################
str_year = str_endDateTime[0:4] 
str_entityNameFile = str_entityName + 'Historical'
str_filename = str_entityName + '_' + str_endDateTime
df_historicalData = fn_getTimestampedFileDataFrame(str_layer, str_subject, str_security, str_entityNameFile, str_initialDateTime, str_endRange)

############################################
## Loading Closed Period  Files
############################################
str_year = str_endDateTime[0:4] 
str_entityNameFile = str_entityName + 'ClosedPeriod'
str_filename = str_entityName + '_' + str_endDateTime
df_closedData = fn_getTimestampedFileDataFrame(str_layer, str_subject, str_security, str_entityNameFile, str_initialDateTime, str_endRange)


# COMMAND ----------

print(str_filePath)

# COMMAND ----------

df_currentData.createOrReplaceTempView("vw_currentData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_currentData a
# MAGIC -- WHERE 
# MAGIC --     a.period = 201601
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;

# COMMAND ----------

df_historicalData.createOrReplaceTempView("vw_historicalData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_historicalData a
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;

# COMMAND ----------

df_closedData.createOrReplaceTempView("vw_closedData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_closedData a
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Applying Data Validations
try:
  fn_transformedValidateDatasetsSchema(df_closedData,df_currentData) 
except:
  print('Info: Data Validations Failed between AccrualClosedPeriod and AccrualCurrentPeriod')
# merge Closed Period data with Current
df_latestData = df_currentData.unionAll(df_closedData)

try:
  fn_transformedValidateDatasetsSchema(df_historicalData,df_latestData) 
except:
  print('Info: Data Validations Failed between Current/Closed Period data and AccrualHistorical data')  
  


# COMMAND ----------

# DBTITLE 1,Merging Data
df_unfilteredData = df_latestData.unionAll(df_historicalData)
print(df_unfilteredData.count())

# COMMAND ----------

# DBTITLE 1,Add DataMartLoadID to the file
df_unfilteredData = df_unfilteredData.withColumn('DataMartLoadID', lit(str_dataMartLoadID))
df_unfilteredData.createOrReplaceTempView("EntityTemp")


# COMMAND ----------

# DBTITLE 1,1,7. Apply common filter
str_sqlCommonFilter = "Select *  From EntityTemp " + str_predicate
df_results = spark.sql(str_sqlCommonFilter)

# COMMAND ----------

df_results.createOrReplaceTempView("vw_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_results a
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,1,13. Create Delta tables
# #####################################
# ## Create Delta File-Database-Table
# #####################################
# df_results.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str_deltaFilePath)
# ## create delta database
# spark.sql(""" 
#            CREATE DATABASE IF NOT EXISTS {0}
#            COMMENT '{0} database for {1}'
#            LOCATION '{2}'
#            """.format(str_databaseName, str_entityName, str_deltaFilePath)
#            )
  
# ## create delta table
# spark.sql(""" 
#           CREATE TABLE IF NOT EXISTS {0}.{1}
#           USING DELTA 
#           LOCATION '{2}'
#           """.format(str_databaseName, str_entityName, str_deltaFilePath)
#          )
# #####################################
# ## Print Message
# #####################################
# print ("mgs: Successfully created '" + str_entityName + "' delta table")