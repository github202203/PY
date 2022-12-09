# Databricks notebook source
from pyspark.sql.functions import sha1, md5, concat_ws

# COMMAND ----------

# MAGIC 
# MAGIC %run ../../../Datalib/Common/Master

# COMMAND ----------

#dbutils.widgets.text("DatalakeEntity","dbo_CTR_BRK","DatalakeEntity")
#dbutils.widgets.text("FileTimestamp","20220411_1700","FileTimestamp")
#dbutils.widgets.text("Format","parquet","Format")

#str_dataLakeEntity         = dbutils.widgets.get("DatalakeEntity")
#str_fileTimestamp          = dbutils.widgets.get("FileTimestamp")
#str_format                 = dbutils.widgets.get("Format")

str_format                 = "parquet"
str_fileTimestamp          = "20220411_1700"
str_dataLakeLayerRaw       = "Raw"
str_dataLakeLayerCleansed  = "Cleansed"
str_dataLakeSubject        = "GXLP210"
str_dataLakeSecurity       = "Internal"

str_YYYY = str_fileTimestamp[0:4]
str_MM = str_fileTimestamp[4:6]
str_DD = str_fileTimestamp[6:8]
str_YYYYMM = str_YYYY + str_MM
str_YYYYMMDD = str_YYYY + str_MM + str_DD
str_HH = str_fileTimestamp[9:11]
str_YYYYMMDD_HH = str_YYYYMMDD + "_" + str_HH

str_MI = str_fileTimestamp[11:14]


# COMMAND ----------

str_GetEntitiesGXLP210 = """
 SELECT S.Name AS Pre_Database,E.Name ,	
        S.name+'.'+E.Name EntityName, SUBLAYER
  FROM	METADATA.ENTITY E
  JOIN	METADATA.SUBJECT S
    ON		E.SUBJECTID = S.SUBJECTID
 WHERE	S.NAME LIKE '%GXLP210' AND S.SubLayer = 'Pre' --and E.Name in ( 'dbo_Accrual' ,'dbo_BROKER' )
 ORDER BY S.NAME,SEQUENCE
 """

# COMMAND ----------

#df_raw = fn_getTimestampedFileDataFrame(str_dataLakeLayer,str_dataLakeSubject,str_dataLakeSecurity,str_dataLakeEntity,str_initialTimestamp,str_endTimestamp)
str_resultgxlp210 = fn_queryControlDatabase(str_GetEntitiesGXLP210)
#print(str_resultgxlp210)

# COMMAND ----------

for GXLPEntity in str_resultgxlp210: 
    str_dataLakeEntity = GXLPEntity[1]
    str_Path_Raw      = "/mnt/main/" + str_dataLakeLayerRaw + "/" + str_dataLakeSubject + "/" + str_dataLakeSecurity + "/" 
    str_Path_Cleansed = "/mnt/main/" + str_dataLakeLayerCleansed + "/" + str_dataLakeSubject + "/" + str_dataLakeSecurity + "/" 
    
    
    str_Raw_File_Folder = str_Path_Raw + str_dataLakeEntity + "/" + str_YYYY + "/" +  str_YYYYMM + "/" + str_YYYYMMDD + "/" + str_YYYYMMDD_HH + "/" + str_dataLakeEntity + "_" + str_YYYY + str_MM + str_DD + "_" + str_HH + str_MI + "/*"
    str_Cleansed_File_Folder = str_Path_Cleansed + str_dataLakeEntity + "/" + str_YYYY + "/" +  str_YYYYMM + "/" + str_YYYYMMDD + "/" + str_YYYYMMDD_HH + "/*"    
    

    #print(str_Raw_File_Folder)
    #print(str_Cleansed_File_Folder)
    
    df_Raw = spark.read.format(str_format).option("header","true").load(str_Raw_File_Folder)
    df_Cleansed = spark.read.format(str_format).option("header","true").load(str_Cleansed_File_Folder).drop('LoadDateTime')
    
    RC = df_Raw.columns
    #print("Raw Columns = " + str(df_Raw.columns))
    #print("Cleansing Columns = " + str(df_Cleansed.columns))
    Raw_Count = df_Raw.count()
    #print("Raw Layer Count = " +str(Raw_Count))

    Cleansed_Count = df_Cleansed.count()
    #print("Cleansed Layer Count = " +str(Cleansed_Count))

    #print("*************************"+str_dataLakeEntity + " - " + str_fileTimestamp+"***********************************")
    if Raw_Count == Cleansed_Count:
       print("Record COUNT PASSED:- "+ str_dataLakeEntity + " Record Count Matches between Raw & Clansed Layer for " + " Timestamp = " + str_fileTimestamp  + " Raw Count = " +str(Raw_Count) + " Cleansed Count = " +str(Cleansed_Count))
    else:
       print("Record COUNT FAILED:-" + str_dataLakeEntity + " Record Count Mismatch between Raw & Clansed Layer "  + " FILE Timestamp = " + str_fileTimestamp )

    if len(df_Raw.columns) != len(df_Cleansed.columns):
      #check_column_count == False
      #print(str_dataLakeEntity + " ----->  *************************  No. of columns not matching")
       print("Column COUNT FAILED:- Total no. of columns from Raw Layer = " + str(len(df_Raw.columns)) + ' & ' + "Total no. of columns from Cleansed Layer = " + str(len(df_Cleansed.columns)))
    else :
      #check_column_count == True
      #print('No. of columns matching')
      print("Column COUNT PASSED:- Total no. of columns from Raw Layer = " + str(len(df_Raw.columns)) + ' & ' + "Total no. of columns from Cleansed Layer = " + str(len(df_Cleansed.columns)))
  

# COMMAND ----------


