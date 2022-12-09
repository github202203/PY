# Databricks notebook source
# MAGIC %md
# MAGIC #Setting up Parameters for List of DIM (Snapshot) tables in Scope for SIT including Partition details

# COMMAND ----------

#from pyspark.sql.functions import md5, sha1, sha2, col, concat_ws,udf
###################
# using Table List
###################

from pyspark.sql.functions import *

#ALL SIT Tables in scope for Testing 

#str_SITDIMList = [('DWH_DimORIPlacement')],[('DWH_DimCode')],[('DWH_DimORITransaction')],[('DWH_DimRiskCount')],[('DWH_DimORICession')],[('DWH_DimBroker')],[('DWH_DimORIApplicablePremium')],[('DWH_DimORIClaimSubEvent')],[('DWH_DimORIContractType')],[('DWH_DimORIDepartment')],[('DWH_DimORIUSMTransaction')],[('DWH_DimORIProgramme')],[('DWH_DimORIReinsurer')]

#str_SITFACTList = [('DWH_FactORIPlacementDataHistory')],[('DWH_FactRiskCountDataHistory')],[('DWH_FactORICessionDataHistory')],[('DWH_FactORIApplicablePremiumDataHistory')],[('DWH_BridgeInwardOutward')]


#print(str_SITList)


str_tablelist = [('DWH_DimORITransaction')], [('DWH_DimORICession')], ['DWH_DimRiskCount'],[('DWH_DimORIPlacement')],[('DWH_DimCode')],[('DWH_DimBroker')],[('DWH_DimORIApplicablePremium')],[('DWH_DimORIClaimSubEvent')],[('DWH_DimORIContractType')],[('DWH_DimORIDepartment')],[('DWH_DimORIUSMTransaction')],[('DWH_DimORIProgramme')],[('DWH_DimORIReinsurer')]

str_partitionprd = 'C20220327' #Partition from Production
str_partitiondev = 'C20220328' #Partition from Dev

print(str_tablelist)
#print('Production Partition for GLXP 2.5 = ' + str_partitionprd)
#print('Dev Partition for GLXP 2.10       = ' + str_partitiondev)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Dataframe for GXLP 2.5/2.10 and compare the data between Prod & Dev

# COMMAND ----------

for SITEntity in str_tablelist: 
    #print('Entity Name (1)= ' + SITEntity[0])
    
    pathprd25   = "/mnt/main/SIT/Data/" + str_partitionprd + "/" + SITEntity[0] + ".parquet" + "/" +"*.parquet"
    pathdev210  = "/mnt/main/Snapshot/MyMI_Snapshot_Group/Internal/" + SITEntity[0] + "/" + str_partitiondev + "/*.parquet"
    
    df25 = spark.read.format("parquet").option("header", "true").load(pathprd25)
    df25 = df25.toDF(*(cols.replace(cols,cols+"25") for cols in df25.columns))
    df25.cache()
    df25.createOrReplaceTempView("vw25")
    df25 = spark.sql(" SELECT * from vw25 WHERE ValidFromDataMartLoadId25 = (SELECT MAX(ValidFromDataMartLoadId25) from vw25) ")
    
    df210 = spark.read.format("parquet").option("header", "true").load(pathdev210)
    df210.cache()
    df210.createOrReplaceTempView("vw210")
    df210 = spark.sql(" SELECT * from vw210 WHERE ValidFromDataMartLoadId = (SELECT MAX(ValidFromDataMartLoadId) from vw210) ")

################################################################################################################################    
####Hash for 2.5 DataFrame        
################################################################################################################################    
    ######### Row Hash using xxhash64
    df25  = df25.withColumn("Hash25", xxhash64(*df25.schema.names))  
    
    ######### Row Hash using Sha2
    #df25  = df25.withColumn("Hash25", sha2(concat_ws("||", *df25.columns), 256))

################################################################################################################################        
####Hash for 2.10 DataFrame            
################################################################################################################################        
    ######### Row Hash using xxhash64
    df210 = df210.withColumn("Hash210", xxhash64(*df210.schema.names))      
    ######### Row Hash using Sha2
    #df210 = df210.withColumn("Hash210", sha2(concat_ws("||", *df210.columns), 256))
    
    dfFullJoin = df25.join(df210,df25.Hash25==df210.Hash210,"full")
    
    dfFullJoin.createOrReplaceTempView("FullJoinOutput")
    
    df_result = spark.sql("SELECT *,CASE WHEN Hash25 = Hash210  THEN 'Passed' \
                                         WHEN Hash25  is NULL then 'Failed-Missing_in_2.5' \
                                         WHEN Hash210 is NULL then 'Failed-Missing_in_2.10' \
                                     END TestResult \
                             FROM FullJoinOutput \
                            WHERE BusinessKey25 = BusinessKey"
                         )
    #df_result.display()
    
    str_FilePath = f"dbfs:/mnt/main/SIT/TestResult/{SITEntity[0]}"
    #print('STR File path = ' + str_FilePath)
    
    df_result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str_FilePath)

    spark.sql(""" 
               CREATE DATABASE IF NOT EXISTS {0}
               COMMENT '{0} database for {1}'
               LOCATION '{2}'
               """.format('SIT1', SITEntity[0], str_FilePath)
               )

    ## create delta table
    spark.sql(""" 
               CREATE TABLE IF NOT EXISTS {0}.{1}
               USING DELTA 
               LOCATION '{2}'
               """.format('SIT1', SITEntity[0], str_FilePath)
             )
    
    
    #####################################
    ## Print Message
    #####################################
    print ("Info: Delta table Successfully created for '" + SITEntity[0] + "' under the database SIT1")
             

# COMMAND ----------


