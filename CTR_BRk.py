# Databricks notebook source
# MAGIC %run ../Datalib/Common/Master

# COMMAND ----------

# Please leave this for debugging in case
# params
str_subject    = "GXLP210"
str_layer      = "Raw"
str_entityName = "dbo_CTR_BRK"
str_convType = 'CleansedType'
path = 'dbfs:/mnt/main/Cleansed/GXLP210/Internal/dbo_CTR_BRK/2022/202207/20220721/20220721_19/dbo_CTR_BRK_20220721_1900.parquet/part-00000-tid-6872968565345277271-547d4f30-985b-455f-af85-bb1478be82f0-61-1-c000.snappy.parquet'
df_newFile = spark.read.format("parquet").option("header", "true").load(path)

# 1.2 Gets Columns details from DB
str_entityId = fn_getSqlEntityID(str_layer, str_subject, str_entityName)
df_colDetail = fn_getSqlColumnDetails(str_entityId)
df_colDetail = df_colDetail.select("Name", str_convType)
display(df_colDetail)
df_colDetail = df_colDetail.withColumnRenamed(str_convType, "DataType")

# 2.0 Get the data types for the data file
df_file = spark.createDataFrame(df_newFile.dtypes)
df_file = df_file.select(col("_1").alias("NewColName"), col("_2").alias("NewColDataType"))

# 3.0 Left Joining data sets 
df_leftJoin = df_file.join(df_colDetail, df_file.NewColName == df_colDetail.Name, how="left").select("NewColName", "NewColDataType", "DataType")

# 4.0 Check to see if there are any differences in the data types
print("Info: Checking for DataType differences...")
df_diff = df_leftJoin.where(df_leftJoin["DataType"] != df_leftJoin["NewColDataType"]) 

#df = fn_cleansedTypeConversions(str_subject,str_layer,str_entityName,df_newFile,'FirstDataType')
display(df_diff)
display(df_diff.take(1))


# COMMAND ----------

str_filePath = 'dbfs:/mnt/main/Raw/GXLP210/Internal/dbo_CTR_BRK/2022/202207/20220707/20220707_19/dbo_CTR_BRK_20220707_1900/data_23c9b92f-f2eb-450f-a06e-ec4ff412c695_ad129df3-bea8-47e0-ad62-21fabbbff5f3.parquet'
df_unfilteredData = spark.read.format('parquet').option("header", "true").load(str_filePath)
##df= spark.read.format('parquet').option("header", "true").load(str_filePath)
display(df_unfilteredData)
df_fileHL = spark.createDataFrame(df_unfilteredData.dtypes)
display(df_fileHL)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * --MIN(REBATE_PC),MAX(REBATE_PC),MIN(BROKERAGE_PC),MAX(BROKERAGE_PC),MIN(BROKER_PC),MAX(BROKER_PC)
# MAGIC FROM MYMI_PRE_GXLP.DBO_CTR_BRK
# MAGIC WHERE BROKERAGE_PC IS NULL AND REBATE_PC IS NULL

# COMMAND ----------

df_colDetail = df_colDetail.withColumnRenamed(str_convType, "DataType")

# 2.0 Get the data types for the data file
df_file = spark.createDataFrame(df_newFile.dtypes)
df_file = df_file.select(col("_1").alias("NewColName"), col("_2").alias("NewColDataType"))

# 3.0 Left Joining data sets 
df_leftJoin = df_file.join(df_colDetail, df_file.NewColName == df_colDetail.Name, how="left").select("NewColName", "NewColDataType", "DataType")

# 4.0 Check to see if there are any differences in the data types
print("Info: Checking for DataType differences...")
df_diff = df_leftJoin.where(df_leftJoin["DataType"] != df_leftJoin["NewColDataType"]) 

#df = fn_cleansedTypeConversions(str_subject,str_layer,str_entityName,df_newFile,'FirstDataType')
display(df_diff)
--display(df_diff.take(1))

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from mdm.MyMIToGXLP_Development

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM   mymi_pre_gxlp.DBO_LEDGER
# MAGIC WHERE CONTRACT = 'ZC000A05A001'
