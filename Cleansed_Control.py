# Databricks notebook source
# MAGIC %run ../Common/Master

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
dbutils.widgets.text("EntityName","dbo_LEDG_HISTORY","EntityName")
dbutils.widgets.text("TimeStamp","20220616_1900","TimeStamp")
dbutils.widgets.text("Pattern","TimeStamped","Pattern")
dbutils.widgets.text("Layer","Raw","Layer")
dbutils.widgets.text("Subject","GXLP210","Subject")
dbutils.widgets.text("Security","Internal","Security")
dbutils.widgets.text("Format","parquet","Format")
##############################################################
str_entityName  = dbutils.widgets.get("EntityName")
str_timeStamp   = dbutils.widgets.get("TimeStamp")
str_pattern     = dbutils.widgets.get("Pattern")
str_layer       = dbutils.widgets.get("Layer")
str_subject     = dbutils.widgets.get("Subject")
str_security    = dbutils.widgets.get("Security")
str_format      = dbutils.widgets.get("Format")
#####################################
##       Local variables
#####################################
str_year  = str_timeStamp[0:4]
str_month = str_timeStamp[0:6]
str_day   = str_timeStamp[0:8]
str_hour  = str_timeStamp[0:11]

# COMMAND ----------

# DBTITLE 1,Finding Latest File for file Sources
def fn_getFilePath():
  str_root_filepath = f"mnt/main/{str_layer}/{str_subject}/{str_security}/{str_entityName}"
  
  # If parquet, use current TimeStamp
  if str_format == 'parquet':
      return f"{str_root_filepath}/{str_year}/{str_month}/{str_day}/{str_hour}/{str_entityName}_{str_timeStamp}/*.{str_format}"

  # If file path exists use latest file else use current TimeStamp
  try:
    return f"{fn_getLatestFile(str_root_filepath)['dir']}/*.{str_format}"
  except:
    return f"{str_root_filepath}/{str_year}/{str_month}/{str_day}/{str_hour}/*.{str_format}"

str_input_filepath = fn_getFilePath()

# COMMAND ----------

# DBTITLE 1,Loading data & apply basic cleansing
# Getting List of files
try:
  if str_format in ['csv', 'tsv']:
    df_results = spark.read.csv(str_input_filepath, sep="\t" if str_format == "tsv" else None, header=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)
  else:
    df_results = fn_trimRemoveBlankSpace(spark.read.parquet(str_input_filepath))
    
  print("Info: Cleansing basic operations succeeded")
except:
  raise(Exception("Error: No Files found, please check if the data exits in Source for period: " + str_hour + ", error message:" ))

# COMMAND ----------

# DBTITLE 1,Apply First Data Types Validations
#Calling First Type Conversion function
df_results = fn_cleansedTypeConversions(str_subject,str_layer,str_entityName,df_results,'FirstDataType') 
print("Info: Cleansing type validation operation succeeded")

# COMMAND ----------

# DBTITLE 1,Apply Cleansed Type Conversions
#Calling Cleansed Type Conversion function 
df_results = fn_cleansedTypeConversions(str_subject,str_layer,str_entityName,df_results,'Cleansed')   
print("Info: Cleansing type validation operation succeeded")

# COMMAND ----------

# DBTITLE 1,Renaming columns names with special characters
try:
  ls = df_results.columns
  for l in ls:
    lt = l.replace(' ','_').replace('(','').replace(')','')
    df_results = df_results.withColumnRenamed(l,lt)
except:
  raise(Exception("Error: Renaming Columns Failed. Please check if there are any special unicode characters"))

# COMMAND ----------

# DBTITLE 1,Adding Timestamped Column to existing Tables
try:
  df_results = df_results.withColumn('LoadDateTime',lit(str_timeStamp))
  print(f"Timestamp column with value {str_timeStamp} added to '{str_entityName}' feed.")
except:
  raise(Exception("Error: Adding Timestamped Column failed. Check the previous step if dataframe exists"))

# COMMAND ----------

# DBTITLE 1,Writing Into Cleansed TimeStamped
try:
  str_output_filepath = f"mnt/main/Cleansed/{str_subject}/{str_security}/{str_entityName}/{str_year}/{str_month}/{str_day}/{str_hour}/{str_entityName}_{str_timeStamp}.parquet"
  df_results.write.mode("overwrite").parquet(str_output_filepath)
  print(f"Info: Cleansed succeeded for {str_entityName} on {str_timeStamp}")
except:
  raise(Exception("Error: No File(s) received. Please investigate prior process."))
