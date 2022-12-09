# Databricks notebook source
# MAGIC %run /DataPlatform/Common/fn_listAllDataLakeDirectory

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from re import *

# COMMAND ----------

arr_bronzePaths = fn_listAllDataLakeDirectory("mnt/bronze")
arr_silverPaths = fn_listAllDataLakeDirectory("mnt/silver")
arr_goldPaths = fn_listAllDataLakeDirectory("mnt/gold")

# COMMAND ----------

#1) Change below arr_xxxPaths to relevant layer for analysis
arr_DeltaPaths = [p.replace("dbfs:","").replace("_delta_log/", "") for p in arr_bronzePaths if match(".*_delta_log/$",p)]
#2) If Bronze/Silver the split should look for index [7] otherwise if gold [6]
arr_DeltaTables = [t.split("/")[7] for t in arr_DeltaPaths]

arr_MaxWatermarkValues = []

for t in range(len(arr_DeltaPaths)):
    
    df = spark.read.format("delta").load(arr_DeltaPaths[t])
    maxWMValue = df.select(min("BronzeStagingSystemLoadID")).collect()[0][0] #3) change to relevant layer
    arr_MaxWatermarkValues.append(maxWMValue)
     
    print(arr_DeltaTables[t] + " - " + str(maxWMValue))

# COMMAND ----------

zip_watermarkValues = zip(arr_DeltaPaths, arr_DeltaTables, arr_MaxWatermarkValues)
df_watermarkValues = sqlContext.createDataFrame(zip_watermarkValues, StructType([StructField("DeltaPath", StringType(), True), StructField("DeltaTable", StringType(), True), StructField("MaxWatermark", LongType(), True)]))

# COMMAND ----------

df_watermarkValuesClean = (df_watermarkValues
                           .select("*",
                                   col("MaxWatermark").substr(1, 3).alias("SystemID"),
                                   to_date(col("MaxWatermark").substr(4, 8), "yyyyMMdd").alias("Date"),
                                   col("MaxWatermark").substr(12, 2).alias("Run")
                                  )
                          )


# COMMAND ----------

display(df_watermarkValuesClean)
#display(df_watermarkValuesClean.groupBy("SystemID", "Date", "Run").agg(count("DeltaPath")))

# COMMAND ----------

arr_bronzeStagingEclipsePaths = [p for p in arr_silverPaths if 'Eclipse' in p and 'Staging' in p and '.parquet' in p]
arr_bronzeSystemLoadIDsEclipsePaths = [p.split("/")[8].split("=")[1] for p in arr_bronzeStagingEclipsePaths]


# COMMAND ----------

zip_paths = zip(arr_bronzeStagingEclipsePaths, arr_bronzeSystemLoadIDsEclipsePaths)
df_stagingFiles = sqlContext.createDataFrame(zip_paths, StructType([StructField("DeltaPath", StringType(), True), StructField("SystemLoadID", StringType(), True)]))

# COMMAND ----------

df_redundantStagingFiles = (df_stagingFiles
                            .filter(col("SystemLoadID") < "1042022101001")
                           )
display(df_redundantStagingFiles)

# COMMAND ----------

arr_redundantStagingFiles = [f.DeltaPath.rsplit("/", 1)[0] for f in df_redundantStagingFiles.select("DeltaPath").collect()]

# COMMAND ----------

for f in arr_redundantStagingFiles:
    print(f)
    dbutils.fs.rm(f, True)
