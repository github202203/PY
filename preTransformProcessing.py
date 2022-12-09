# Databricks notebook source
# DBTITLE 1,Include common functions
# MAGIC %run ../../Datalib/Common/Master

# COMMAND ----------

# DBTITLE 1,Set Parameter & Variables
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

# DBTITLE 1,Set Variables
str_layer         = "Cleansed"
str_databaseName  =  str_subject
str_deltaFilePath = 'dbfs:/mnt/main/Transformed' + "/" + str_subject + "/" + str_security + "/" + str_entityName


# COMMAND ----------

# DBTITLE 1,Get Entity Details
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

details = fn_queryControlDatabase(str_sqlTable)

# COMMAND ----------

# DBTITLE 1,Calculating variables
str_predicate = " "

for str_item in details:
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

# DBTITLE 1,Path Variables for Full Refresh
str_year     = str_endDateTime[0:4]
str_month    = str_endDateTime[0:6]
str_day      = str_endDateTime[0:8]
str_folter   = str_endDateTime[0:11]
str_filename = str_entityName + '_' + str_endDateTime
str_hour     = str(int(str_endDateTime[-4:]) - 100)
str_endRange  = str_day + '_' + str_hour

# COMMAND ----------

# DBTITLE 1,Read and Join Multiple Parquet Files
#########################################
## For Full Refresh Load Latest File only
#########################################
str_year = str_endDateTime[0:4] 
if(str_loadType == 'FullRefresh'):
  str_filePath = 'dbfs:/mnt/main/' + str_layer + '/' + str_subject + '/' + str_security + '/' + str_entityName + '/' + str_year + '/' + str_month + '/' + str_day + '/' + str_folter + '/' + str_filename + '.parquet'
  df_unfilteredData = spark.read.format('parquet').option("header", "true").load(str_filePath)
####################################################
# For Incremental Load since last table initialized
####################################################
elif(str_loadType == 'Incremental'):
  try:
    df_unfilteredData = fn_getTimestampedFileDataFrame(str_layer, str_subject, str_security, str_entityName, str_initialDateTime, str_endRange)
  except:
    print("info: This table has been initialized today. Only latest file imported")
  str_filePath = 'dbfs:/mnt/main/' + str_layer + '/' + str_subject + '/' + str_security + '/' + str_entityName + '/' + str_year + '/' + str_month + '/' + str_day + '/' + str_folter + '/' + str_filename + '.parquet'
  df_newFile = spark.read.format('parquet').option("header", "true").load(str_filePath)

# COMMAND ----------

# DBTITLE 1,Data Validations
# data validation step was added for incremental loads
# data validatin for full refresh load is missing because it depends on the metadata pipeline to be fixed.**
int_executeProcessValidate = 0
try:
  if(str_loadType == 'Incremental' and df_unfilteredData and df_newFile):
    int_executeProcessValidate = 1
except:
  print('Info: No data validation checks applied')

if(int_executeProcessValidate == 1):
  fn_transformedValidateDatasetsSchema(df_unfilteredData,df_newFile) 
else:
  print("Info: No validations applied to the data since only one file found")


# COMMAND ----------

# DBTITLE 1,Union data for Incremental
if(int_executeProcessValidate == 1):
  lst_cols = df_unfilteredData.columns
  df_newFile = df_newFile.select(lst_cols)
  df_unfilteredData = df_unfilteredData.union(df_newFile)
elif(str_loadType == 'Incremental' and int_executeProcessValidate == 0):
  df_unfilteredData = df_newFile

# COMMAND ----------

# DBTITLE 1, Add DataMartLoadID to the file
df_unfilteredData = df_unfilteredData.withColumn('DataMartLoadID', lit(str_dataMartLoadID))
df_unfilteredData.createOrReplaceTempView("EntityTemp")

# COMMAND ----------

# DBTITLE 1,Apply common filter
str_sqlCommonFilter = "Select *  From EntityTemp " + str_predicate
df_results = spark.sql(str_sqlCommonFilter)

# COMMAND ----------

# DBTITLE 1,Additional Filter for Incremental Loads
######################################################
# For change tracking source excluding deleted records
######################################################
if(str_loadType == 'Incremental'):
  df_results.createOrReplaceTempView("filteredTemp1")
  str_sqlDeleteFilter = """
    SELECT P.*
       FROM filteredTemp1 P
       ANTI JOIN (
          SELECT """ + str_identifier + """  
          FROM filteredTemp1 
          WHERE SYS_CHANGE_OPERATION = 'D'
        )  
        CT ON """ + str_join +""""""
  df_results = spark.sql(str_sqlDeleteFilter)
  lst_cols = df_results.columns

# COMMAND ----------

######################################################
# For change tracking source Getting Latest Records
######################################################
if(str_loadType == 'Incremental'):
  df_results.createOrReplaceTempView("filteredTemp2")
  str_sqlLatestFilter = """WITH Policies AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY """ + str_identifier + """ ORDER BY LoadDateTime DESC) AS rownumber
    FROM filteredTemp2
    )
  SELECT * 
    FROM Policies 
  WHERE rownumber = 1 """
  df_results = spark.sql(str_sqlLatestFilter)
  df_results = df_results.select(lst_cols)

# COMMAND ----------

# DBTITLE 1,Create Delta tables
#####################################
## Create Delta File-Database-Table
#####################################
df_results.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str_deltaFilePath)
## create delta database
spark.sql(""" 
           CREATE DATABASE IF NOT EXISTS {0}
           COMMENT '{0} database for {1}'
           LOCATION '{2}'
           """.format(str_databaseName, str_entityName, str_deltaFilePath)
           )
  
## create delta table
spark.sql(""" 
          CREATE TABLE IF NOT EXISTS {0}.{1}
          USING DELTA 
          LOCATION '{2}'
          """.format(str_databaseName, str_entityName, str_deltaFilePath)
         )
#####################################
## Print Message
#####################################
print ("mgs: Successfully created '" + str_entityName + "' delta table")

# COMMAND ----------

# DBTITLE 1,Read Delta Table Test
## %sql
##--Testing purposes
## -- Use delta table
## SELECT count(*)
## FROM  stg_velocity.dbo_allcontacts
##
## --#1: 266357 (File from dates: 20191218 + 20191220)
## --#2: 437835

# COMMAND ----------


## %sql
##  --## Drop table script for renaming conventions, left for ease of copy paste whilst still in build phase. 
## DROP TABLE IF EXISTS stg_velocity.dbo_ALLCONTACTS;
## DROP TABLE IF EXISTS stg_velocity.dbo_APP_MULTIPLEUNDER;
## DROP TABLE IF EXISTS stg_velocity.dbo_APPENDPOLICY;
## DROP TABLE IF EXISTS stg_velocity.dbo_ARCUST;
## DROP TABLE IF EXISTS stg_velocity.dbo_ARINV;
## DROP TABLE IF EXISTS stg_velocity.dbo_ARINVTRAN;
## DROP TABLE IF EXISTS stg_velocity.dbo_CASEWORKER;
## DROP TABLE IF EXISTS stg_velocity.dbo_CLAIMANT;
## DROP TABLE IF EXISTS stg_velocity.dbo_CLAIMATTDATA;
## DROP TABLE IF EXISTS stg_velocity.dbo_ClaimMast;
## DROP TABLE IF EXISTS stg_velocity.dbo_CLAIMNOTE;
## DROP TABLE IF EXISTS stg_velocity.dbo_ClaimTran;
## DROP TABLE IF EXISTS stg_velocity.dbo_claimtrial;
## DROP TABLE IF EXISTS stg_velocity.dbo_CLAIMTYPE;
## DROP TABLE IF EXISTS stg_velocity.dbo_CLMASSIGNHISTORY;
## DROP TABLE IF EXISTS stg_velocity.dbo_CORRESPONDHEAD;
## DROP TABLE IF EXISTS stg_velocity.dbo_CORRESPONDLINE;
## DROP TABLE IF EXISTS stg_velocity.dbo_custtype;
## DROP TABLE IF EXISTS stg_velocity.dbo_DETAILATTCODES;
## DROP TABLE IF EXISTS stg_velocity.dbo_DETAILATTDATA;
## DROP TABLE IF EXISTS stg_velocity.dbo_GLORG2;
## DROP TABLE IF EXISTS stg_velocity.dbo_installtran;
## DROP TABLE IF EXISTS stg_velocity.dbo_INSURCOMP;
## DROP TABLE IF EXISTS stg_velocity.dbo_LineCode;
## DROP TABLE IF EXISTS stg_velocity.dbo_major_activity;
## DROP TABLE IF EXISTS stg_velocity.dbo_Payee;
## DROP TABLE IF EXISTS stg_velocity.dbo_PolicyTran;
## DROP TABLE IF EXISTS stg_velocity.dbo_Quote;
## DROP TABLE IF EXISTS stg_velocity.dbo_QUOTEPREM;
## DROP TABLE IF EXISTS stg_velocity.dbo_RESPCODE;
## DROP TABLE IF EXISTS stg_velocity.dbo_SIC;
## DROP TABLE IF EXISTS stg_velocity.dbo_STATE;
## DROP TABLE IF EXISTS stg_velocity.dbo_SubmissionMast;
## DROP TABLE IF EXISTS stg_velocity.dbo_TIMEACTIVATE;
## DROP TABLE IF EXISTS stg_velocity.dbo_Treaty;
## DROP TABLE IF EXISTS stg_velocity.dbo_Policy;
