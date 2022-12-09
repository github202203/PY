# Databricks notebook source
# Process Variables
str_sourceBFilePath = "/mnt/bronze/masterdata/Internal/Profisee/DeltaLake/"
str_targetBDatabaseName = "EclipseBronze"

str_sourceSFilePath = "/mnt/silver/masterdata/Internal/Profisee/DeltaLake/"
str_targetSDatabaseName = "EclipseSilver"



# COMMAND ----------

ProfiseeObjects = [x.name.replace("/","") for x in dbutils.fs.ls(str_sourceBFilePath)]

# COMMAND ----------

for str_sourceObject in ProfiseeObjects:
    
    try:
        df_bronze = spark.read.format("delta").load(str_sourceBFilePath + str_sourceObject)
        df_renameB = df_bronze.withColumnRenamed("SYS_CHANGE_OPERATION", "DP_CRUD")

        (df_renameB
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", True)
         .option("path",str_sourceBFilePath + str_sourceObject)
         .saveAsTable(f"{str_targetBDatabaseName}.{str_sourceObject}")
        )
    
        print(str_sourceObject + " Bronze renamed and overwritten")
    except:
        print("No Bronze object found for " + str_sourceObject)
        
    try:
        df_silver = spark.read.format("delta").load(str_sourceSFilePath + str_sourceObject)
        df_renameS = df_silver.withColumnRenamed("SYS_CHANGE_OPERATION", "DP_CRUD")

        (df_renameS
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", True)
         .option("path",str_sourceSFilePath + str_sourceObject)
         .saveAsTable(f"{str_targetSDatabaseName}.{str_sourceObject}")
        )

        print(str_sourceObject + " Silver renamed and overwritten")
    except:
        print("No Silver object found for " + str_sourceObject)
