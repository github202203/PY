# Databricks notebook source
# MAGIC %md
# MAGIC #Setting up Parameters for List of FACT (Snapshot) tables in Scope for SIT including Partition details

# COMMAND ----------

#from pyspark.sql.functions import md5, sha1, sha2, col, concat_ws,udf
###################
# using Table List
###################

from pyspark.sql.functions import *

#ALL SIT Tables in scope for Testing 

#str_SITDIMList = [('DWH_DimORIPlacement')],[('DWH_DimCode')],[('DWH_DimORITransaction')],[('DWH_DimRiskCount')],[('DWH_DimORICession')],[('DWH_DimBroker')],[('DWH_DimORIApplicablePremium')],[('DWH_DimORIClaimSubEvent')],[('DWH_DimORIContractType')],[('DWH_DimORIDepartment')],[('DWH_DimORIUSMTransaction')],[('DWH_DimORIProgramme')],[('DWH_DimORIReinsurer')]

#str_SITFACTList = [('DWH_FactORIPlacementDataHistory')],[('DWH_FactRiskCountDataHistory')],[('DWH_FactORICessionDataHistory')],[('DWH_FactORIApplicablePremiumDataHistory')],[('DWH_BridgeInwardOutward')],['DWH.FactORITransactionDataHistory']



#print(str_SITList)


#str_tablelist = [('DWH_DimORITransaction')], [('DWH_DimORICession')], ['DWH_DimRiskCount'],[('DWH_DimORIPlacement')],[('DWH_DimCode')],[('DWH_DimBroker')],[('DWH_DimORIApplicablePremium')],[('DWH_DimORIClaimSubEvent')],[('DWH_DimORIContractType')],[('DWH_DimORIDepartment')],[('DWH_DimORIUSMTransaction')],[('DWH_DimORIProgramme')],[('DWH_DimORIReinsurer')]

str_facttablelist = [('DWH_FactORITransactionDataHistory')],

str_partitionprd = 'C20220327' #Partition from Production
str_partitiondev = 'C20220328' #Partition from Dev

print(str_facttablelist)
#print('Production Partition for GLXP 2.5 = ' + str_partitionprd)
#print('Dev Partition for GLXP 2.10       = ' + str_partitiondev)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Dataframe for GXLP 2.5/2.10 and compare the data between Prod & Dev FACT Tables

# COMMAND ----------

for SITFACTEntity in str_facttablelist: 
    #print('Entity Name (1)= ' + SITFACTEntity[0])

    
    pathprd25   = "/mnt/main/SIT/Data/" + str_partitionprd + "/" + SITFACTEntity[0].replace("DataHistory","") + "_" + str_partitionprd + ".parquet" + "/" +"*.parquet"
    #print(pathprd25)
    
    #pathdev210  = "/mnt/main/Snapshot/MyMI_Snapshot_Group/Internal/" + SITFACTEntity[0].replace("DataHistory","") + "/" + SITFACTEntity[0].replace("DataHistory","") + "_"  + str_partitiondev + ".parquet" + "/*.parquet"
    pathprd210   = "/mnt/main/SIT/Data/" + str_partitionprd + "/" + SITFACTEntity[0].replace("DataHistory","") + "_" + str_partitionprd + ".parquet" + "/" +"*.parquet"
    #print(pathdev210)
    
    df25 = spark.read.format("parquet").option("header", "true").load(pathprd25)
    df25 = df25.toDF(*(cols.replace(cols,cols+"25") for cols in df25.columns))
    df25.display()
    df25.count()
    df25.createOrReplaceTempView("vw25")
    #df25 = spark.sql(" SELECT * from vw25 WHERE ValidFromDataMartLoadId25 = (SELECT MAX(ValidFromDataMartLoadId25) from vw25) ")
    df25 = spark.sql(" SELECT * from vw25 WHERE DL_DWH_DimDataHistory_ID25 = 3225")

    
    df210 = spark.read.format("parquet").option("header", "true").load(pathdev210)
    df210.display()
    df210.createOrReplaceTempView("vw210")
    #df210 = spark.sql(" SELECT * from vw210 WHERE ValidFromDataMartLoadId = (SELECT MAX(ValidFromDataMartLoadId) from vw210) ")
    df25 = spark.sql(" SELECT * from vw25 WHERE DL_DWH_DimDataHistory_ID25 = 3225")

################################################################################################################################    
####Hash for 2.5 DataFrame        
################################################################################################################################    
    ######### Row Hash using xxhash64
    #df25  = df25.withColumn("Hash25", xxhash64(*df25.schema.names))  
    
    ######### Row Hash using Sha2
    df25  = df25.withColumn("Hash25", sha2(concat_ws("||", *df25.columns), 256))

################################################################################################################################        
####Hash for 2.10 DataFrame            
################################################################################################################################        
    ######### Row Hash using xxhash64
    #df210 = df210.withColumn("Hash210", xxhash64(*df210.schema.names))      
    ######### Row Hash using Sha2
    df210 = df210.withColumn("Hash210", sha2(concat_ws("||", *df210.columns), 256))
    
    #dfFullJoin = df25.join(df210,df25.DL_DWH_DimORITransaction_ID==df210.DL_DWH_DimORITransaction_ID,"full") 
    dfFullJoin = df25.join(df210,df25.DL_DWH_DimDataHistory_ID25==df210.DL_DWH_DimDataHistory_ID,"full") 
      
    
    dfFullJoin.createOrReplaceTempView("FullJoinOutput")
    
    

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select COUNT(*) from FullJoinOutput 

# COMMAND ----------

