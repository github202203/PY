# Databricks notebook source
# MAGIC %run ../Common/Master

# COMMAND ----------

# DBTITLE 1,Parameter(s)
dbutils.widgets.text("Layer","Raw","Layer")
dbutils.widgets.text("Subject","Velocity","Subject")

str_layer         = dbutils.widgets.get("Layer")
str_subject       = dbutils.widgets.get("Subject")

# COMMAND ----------

# DBTITLE 1,Reading All Entities
try:
  df_entities = fn_getAllEntitiesBySubject(str_layer,str_subject)
  int_entityCount = df_entities.count()
  
  if int_entityCount == 0:
    raise(Exception("Error: No Files found. Please Check if the data exits in " + str_layer +" layer " + str_subject + " Subject."))
  else:
    print("Info: " + str(int_entityCount) +" Entities found for "+ str_subject)
except:
  raise(Exception("Error: Unable to read from Data Lake."))

# COMMAND ----------

# DBTITLE 1,Preparing lists for entities
paths = df_entities.select('path').collect()
lst_deltas = []
lst_others = []

for p in paths:
  lst_paths = dbutils.fs.ls(p.path)
  
  for l in lst_paths:
    if(l.name == "_delta_log/"):
      lst_deltas.append(p) 
    if(l.name != "_delta_log/"):
      lst_others.append(p)

# COMMAND ----------

# DBTITLE 1,Removing duplicates
lst_new = fn_listDeDuplication(lst_others)

# COMMAND ----------

# DBTITLE 1,Reading Columns from non delta entities
if lst_new:
  # create empty data frame
  sc = spark.sparkContext
  schema = StructType([
    StructField('column', StringType(), False),
    StructField('type', StringType(), True),
    StructField('path', StringType(), True),
    StructField('format', StringType(), True), 
    StructField('layer', StringType(), True),
    StructField('subject', StringType(), True) 
  ])
  df_columnDetails = sqlContext.createDataFrame(sc.emptyRDD(), schema)
  # looping through the list of entities
  for x in lst_new:
    latest_file = fn_getLatestFile(x.path)   
    try:
      df_files = spark.read.format(latest_file['extension']).option("header", "true").load(f"{latest_file['dir']}/*.{latest_file['extension']}")
      # getting data types loaded files
      lst_types = df_files.dtypes
      # preparing staging df for df column details
      df_columnStaging = spark.createDataFrame(lst_types)
      df_columnStaging = df_columnStaging.withColumn("path",lit(x.path[:-1])).select("_1","_2","path")
      df_columnStaging = df_columnStaging\
                               .withColumn("format",lit(latest_file['extension']))\
                               .withColumn("layer",lit(str_layer))\
                               .withColumn("subject",lit(str_subject))\
                               .select("_1","_2","path","format","layer","subject")
      #forward merge data frames for Column details
      df_columnDetails = df_columnDetails.union(df_columnStaging)
    except:
      raise(Exception(f"incorrect file: {latest_file['dir']}/*.{latest_file['extension']}"))

# COMMAND ----------

# DBTITLE 1,Adding Entity Name to the data frame
try:
  df_columnDetails = df_columnDetails.withColumn('path1',lit(col("path")))
  df_columnDetails = df_columnDetails.withColumn('path1', split(df_columnDetails.path1, '/')) \
  .select(  element_at(col('path1'), -1).alias('entity'),"column","type","path","format","layer","subject"
     )
except:
  raise(Exception("Error:incorrect file path:"))

# COMMAND ----------

# DBTITLE 1,Filtered out json files
df_columnDetails = df_columnDetails.where(df_columnDetails.format != 'json')
#display(df_columnDetails)

# COMMAND ----------

# DBTITLE 1,Write Column Details to DL
str_filepath = "mnt/main/Reconciliation/AllSources/Internal/MetadataColumnDetails"
if df_columnDetails:
  dbutils.fs.rm(str_filepath, True)
df_columnDetails.write.option("header","true").mode("overwrite").parquet(str_filepath)
