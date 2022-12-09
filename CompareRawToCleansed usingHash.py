# Databricks notebook source
from pyspark.sql.functions import sha1, md5, concat_ws

dbutils.widgets.text("DatalakeEntity","dbo_CTR_BRK","DatalakeEntity")
dbutils.widgets.text("FileTimestamp","20220411_1700","FileTimestamp")
dbutils.widgets.text("Format","parquet","Format")


str_dataLakeEntity         = dbutils.widgets.get("DatalakeEntity")
str_fileTimestamp          = dbutils.widgets.get("FileTimestamp")
str_format                 = dbutils.widgets.get("Format")

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

str_Raw_File_Folder = str_dataLakeEntity + "/" + str_YYYY + "/" +  str_YYYYMM + "/" + str_YYYYMMDD + "/" + str_YYYYMMDD_HH + "/" + str_dataLakeEntity + "_" + str_YYYY + str_MM + str_DD + "_" + str_HH + str_MI + "/*"
str_Cleansed_File_Folder = str_dataLakeEntity + "/" + str_YYYY + "/" +  str_YYYYMM + "/" + str_YYYYMMDD + "/" + str_YYYYMMDD_HH + "/*"

#print(str_entityFolder)

str_Path_Raw = "/mnt/main/" + str_dataLakeLayerRaw + "/" + str_dataLakeSubject + "/" + str_dataLakeSecurity + "/" + str_Raw_File_Folder
str_Path_Cleansed = "/mnt/main/" + str_dataLakeLayerRaw + "/" + str_dataLakeSubject + "/" + str_dataLakeSecurity + "/" + str_Raw_File_Folder

#print("Raw Folder = " + str_Raw_File_Folder)
#print("Raw Path = "   +str_Path_Raw)

#print("Cleansed Folder = " + str_Cleansed_File_Folder)
#print("Cleansed Path = "   + str_Path_Cleansed)



df_Raw = spark.read.format(str_format).option("header","true").load(str_Path_Raw)
df_Cleansed = spark.read.format(str_format).option("header","true").load(str_Path_Cleansed)

# COMMAND ----------

#df_Raw.show()
#df_Raw.show(truncate=False)

# COMMAND ----------

#df_Raw.printSchema()

# COMMAND ----------

raw_column_list=[]
df_Raw.columns

for i in df_Raw.columns:
    raw_column_list.append(i)
    #print(column_list)

#for i in df_Raw.columns:
 #   column_list.append[i]

# COMMAND ----------

#df_Cleansed.show()
#df_Cleansed.show(truncate=False)

# COMMAND ----------

#df_Cleansed.printSchema()

# COMMAND ----------

cleansed_column_list=[]
df_Cleansed.columns

for i in df_Cleansed.columns:
    cleansed_column_list.append(i)
    cleansed_column_list.append('%')
    
    #print(column_list)


# COMMAND ----------

#print(cleansed_column_list)

# COMMAND ----------

check_column_count = True

if len(df_Raw.columns) < len(df_Cleansed.columns):
  check_column_count == False
  print('No. of columns in cleansed layer is more than raw layer.' + 'Raw Layer Column Count = ' + str(len(df_Raw.Columns)) + ' & ' + 'Cleansed Layer Column Count = ' + str(len(df_Cleansed.columns)))
  
if len(df_Raw.columns) > len(df_Cleansed.columns):
  check_column_count == False
  print('No. of columns in cleansed layer is less than raw layer.' + 'Raw Layer Column Count = ' + str(len(df_Raw.Columns)) + ' & ' + 'Cleansed Layer Column Count = ' + str(len(df_Cleansed.columns)))  

if len(df_Raw.columns) != len(df_Cleansed.columns):
  check_column_count == False
  print('No. of columns not matching')
else :
  check_column_count == True
  print('No. of columns matching')

if check_column_count == False:
  print("Error : Mismatch of column count between Raw & Cleansed Layer")
else:
  print('Total no. of columns from Raw Layer = ' + str(len(df_Raw.columns)) + ' & ' + 'Total no. of columns from Cleansed Layer = ' + str(len(df_Cleansed.columns)))

# COMMAND ----------

#Check the column list between Raw & Cleansed 

if df_Raw.columns == df_Cleansed.columns:
  print('Column Matches between Raw & Cleanssed layer - PASSED')
else:
  print('Column Matches between Raw & Cleansed layer FAILED ')

# COMMAND ----------

#Check count of records between Raw & Cleansed Layer

Raw_Count = df_Raw.count()
print("Raw Layer Count = " +str(Raw_Count))

Cleansed_Count = df_Cleansed.count()
print("Cleansed Layer Count = " +str(Cleansed_Count))

if Raw_Count == Cleansed_Count:
   print('Count Matches between Raw & Clansed Layer for ' + str_dataLakeEntity + ' Timestamp = ' + str_fileTimestamp )
else:
   print('Count mismatch between Raw & Clansed Layer for ' + str_dataLakeEntity + ' Timestamp = ' + str_fileTimestamp )


# COMMAND ----------



# COMMAND ----------


