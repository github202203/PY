# Databricks notebook source

from pyspark.sql.functions import *

def concatenate_cols(*list_cols):
    return '%'.join(list([str(i) for i in list_cols]))
concatenate_cols = udf(concatenate_cols)  

str_tablelist = [('DWH_DimORITransaction')], [('DWH_DimORICession')]

str_partitionprd = 'C20220327'
str_partitiondev = 'C20220328'
print(str_tablelist)
#print('Production Partition for GLXP 2.5 = ' + str_partitionprd)
#print('Production Partition for GLXP 2.5 = ' + str_tablelist[0])
#print('Dev Partition for GLXP 2.10       = ' + str_partitiondev)

# COMMAND ----------

# ######## Both 2.5 & 2.10 DataFrame (Working OK)
for GXLPEntity in str_tablelist:
    #prd25 = "/mnt/main/SIT/C20220327/DWH_DimORITransaction.parquet/*.parquet"
    prd25       = "/mnt/main/SIT/" + str_partitionprd + "/" + GXLPEntity[0] + ".parquet" + "/" + "*.parquet"
    #print('prd 25 = '+prd25)
    df25 = spark.read.format("parquet").option("header", "true").load(prd25)
    df25 = df25.toDF(*(cols.replace(cols,cols+"25") for cols in df25.columns))

allcolumns25=[]
for i in df25.columns:
  allcolumns25.append(i)
    #print(allcolumns25)
    
  df25 = df25.withColumn('allcolumnsjoin25', concatenate_cols(*allcolumns25))
  df25.createOrReplaceTempView("vw25")
  df25 = spark.sql(" SELECT * from vw25 WHERE ValidFromDataMartLoadId25 = (SELECT MAX(ValidFromDataMartLoadId25) from vw25) ")
  df25 = df25.withColumn('Sha2Hash25',sha2('allcolumnsjoin25',256))    
    
  #path210  = "/mnt/main/Snapshot/MyMI_Snapshot_Group/Internal/DWH_DimORITransaction/C20220328/*.parquet"
  path210  = "/mnt/main/Snapshot/MyMI_Snapshot_Group/Internal/" + GXLPEntity[0] + "/" + str_partitiondev + "/" + "*.parquet"
  #print('df210 path = ' + path210)
  df210 = spark.read.format("parquet").option("header", "true").load(path210)

allcolumns210=[]
for i in df210.columns:
  allcolumns210.append(i)
  #print(allcolumns25)

  df210 = df210.withColumn('allcolumnsjoin210', concatenate_cols(*allcolumns210))
  df210.createOrReplaceTempView("vw210")
  df210 = spark.sql(" SELECT * from vw210 WHERE ValidFromDataMartLoadId = (SELECT MAX(ValidFromDataMartLoadId) from vw210) ")
  df210 = df210.withColumn('Sha2Hash210',sha2('allcolumnsjoin210',256))

  #df25.select(col("BusinessKey").alias("BusinessKey25"),col("Sha2Hash25").alias("Hash25")).show(truncate=False)
  #df210.select(col("BusinessKey").alias("BusinessKey210"),col("Sha2Hash210").alias("Hash210")).show(truncate=False)
  #dfFullJoin = df25.join(df210,df25.Sha2Hash25==df210.Sha2Hash210,"full")

  dfFullJoin = df25.join(df210,df25.BusinessKey25==df210.BusinessKey,"full")
  #dfFullJoin.display()

  dfFullJoin.createOrReplaceTempView("vwAll")
  df_result = spark.sql("SELECT Sha2Hash25,Sha2Hash210, Case when Sha2Hash25 = Sha2Hash210 then 'Passed' else 'Failed' End as TestResult, * FROM vwAll")


  str_FilePath = f"dbfs:/mnt/main/SIT/TestResult/{GXLPEntity[0]}"

#str_table = 'DWH_DimOriTransaction'
dfFullJoin.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str_FilePath)
#df_result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str_FilePath)
spark.sql(""" 
               CREATE DATABASE IF NOT EXISTS {0}
               COMMENT '{0} database for {1}'
               LOCATION '{2}'
               """.format('SIT', GXLPEntity[0], str_FilePath)
               )

    ## create delta table
spark.sql(""" 
              CREATE TABLE IF NOT EXISTS {0}.{1}
              USING DELTA 
              LOCATION '{2}'
              """.format('SIT', GXLPEntity[0], str_FilePath)
             )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT Businesskey25,Businesskey, sha2Hash25, sha2Hash210,Case when Sha2Hash25 = Sha2Hash210 then 'Passed' else 'Failed' End as TestResult FROM vwAll

# COMMAND ----------


