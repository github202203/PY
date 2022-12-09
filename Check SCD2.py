# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE EclipseBronzeTest.dbo_policyline

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP DATABASE EclipseBronzeTest

# COMMAND ----------

# MAGIC %run ../Common/fn_checkSCDValidity

# COMMAND ----------

from pyspark.sql.functions import desc, row_number, dense_rank, col, lit, lag, expr, count, when
from pyspark.sql.window import Window

from delta.tables import *

# COMMAND ----------

arr_eclipseTables = ['dbo_Policy', 'dbo_PolicyLine', 'dbo_EclipseClaim', 'dbo_ClaimLine', 'dbo_ClaimEvent', 'dbo_ClaimStatus', 'dbo_LossRegister', 'dbo_SequelModelExtensionFields',
                     'dbo_ReportingClass', 'dbo_AuditLog', 'dbo_ScmTrans', 'dbo_InternalCatCode', 'dbo_BusinessCode', 'dbo_ClaimLineShare', 'dbo_Movement']
arr_eclipseUniqueCol = [["PolicyId"],["PolicyLineId"],["RowUniqueID"],["ClaimLineID"],["ClaimEventID"],["ClaimStatusID"],["LossUniqueRowID"],["SequelModelExtensionFieldsId"],["Synd","PIMYear","Class1","Class2","Class3","Class4","ProducingTeam"],["UpdateId"],["SCMUniqueTransID"],["InternalCatCodeId"],["BusinessCodeId"],["ClaimLineShareID"],["MovementID"]]

arr_myMIAdhocSourceTables = ['dbo_CRS_CauseCodes', 'dbo_Trax_CauseCodes']
arr_myMIAdhocSourceUniqueCol = [[],["UCR"]]

arr_MDSTables = ["mdm_CRSCauseCode_Development"]
arr_MDSUniqueCol = [["CauseCode"]]


# COMMAND ----------

for table in arr_eclipseTables:
    print(table)
    
    df_bronzestaging = spark.read.format('parquet').load('/mnt/bronze/underwriting/Internal/Eclipse/Staging/'+table+'/')
    df_bronze = spark.read.format('delta').load('/mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/'+table+'/')
    df_bronzescd2 = spark.read.format('delta').load('/mnt/bronze/SCD2/underwriting/Internal/Eclipse/DeltaLake/'+table+'/')
    
#     assert(df_bronzestaging.count() == df_bronzescd2.count())
#     assert(df_bronze.count() == df_bronzescd2.filter(col("Current")==True).count())
#     print("Staging to SCD2 counts equal, latest counts equal")
    
    print("Bronze staging:      " + str(df_bronzestaging.count()))
    print("Bronze SCD2:         " + str(df_bronzescd2.count()))
    print("Bronze:              " + str(df_bronze.count()))
    print("Bronze current SCD2: " + str(df_bronzescd2.filter(col("Current")==True).count()))

    print("")

# COMMAND ----------

arr_uniqueColumn = [""]

for table in arr_MDSTables:
    
    df_updated = DeltaTable.forPath(spark, "/mnt/bronze/SCD2/masterdata/Internal/MDS/DeltaLake/"+table+"/").toDF()
    fn_checkSCDValidity(df_updated, arr_uniqueColumn, str_layer+"SystemLoadID")

# COMMAND ----------

for i in range(len(arr_eclipseTables)):
    
    table = arr_eclipseTables[i]
    arr_uniqueColumn = arr_eclipseUniqueCol[i]
    str_layer = "Bronze"
    
    if(arr_uniqueCol != []):
        df = DeltaTable.forPath(spark, "/mnt/bronze/SCD2/underwriting/Internal/Eclipse/DeltaLake/"+table+"/").toDF()
        
        print(table)
        fn_checkSCDValidity(df, arr_uniqueColumn, str_layer+"SystemLoadID")
        print("")
    else:
        print("no unique column")
