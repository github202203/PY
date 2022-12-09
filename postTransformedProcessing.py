# Databricks notebook source
print("Post Transformed Processing Started...")
print("------------------------------------------------") 

# COMMAND ----------

# DBTITLE 1,Backward Compatibility - Create Empty List
try:
    lst_snowflakeResolveOrder
except NameError:
    lst_snowflakeResolveOrder = []

# COMMAND ----------

# DBTITLE 1,Pattern Description and Set Variables
####################################################################################################################################################
#### PostTransformedProcessing Pattern Steps:
## 1.0. Validate Transformed Entity using function
## 2.0. Write & Create delta objects, files and store in Data Lake using function
## 3.0. Get Business Keys, Late Arriving Column, Default Mapping Value and Snowflake Resolve Order from List
## 4.0. Get Column Details Metadata from Delta Table
## 5.0. Append to Metadata SQL Database ColumnDetails Table using function
####################################################################################################################################################

####################################
## Set Variables for Pattern
####################################
# str_deltaFilePath   = 'dbfs:/mnt/main/' + str_layer + "/" + str_subject + "/" + str_security + "/" + str_entityName
str_currentDateTime = datetime.strptime(str(datetime.now()), "%Y-%m-%d %H:%M:%S.%f")

####################################
## Get Subject and Entity from Metadata Database
####################################
print("Obtaining objects from Metadata Database (DataLib_Control)...")

int_subjectID = fn_getSqlSubjectID(str_layer, str_subject)
int_entityID  = fn_getSqlEntityID(str_layer, str_subject, str_entityName)

# COMMAND ----------

# DBTITLE 1,Post Transformed - Processing Pattern
####################################
## 1.0. Validate Transformed Entity using function
####################################
print("\n" + "Validating Entity Notebook...")
if(str_subject.startswith("MyMI_Abst") or str_entityName.startswith("Bridge")  ):
  print("Info: Successfully completed - No BusinessKey(s) to validate in table: '" + str_subject + "." + str_entityName + "'")
else:
  if(str_entityName.startswith("Fact")):
    fn_transformedValidateEntity(df_transformedQuery, str_subject, str_entityName, str_tableType = "Fact")
  else:
    fn_transformedValidateEntity(df_transformedQuery, str_subject, str_entityName, str_tableType = "Dim")

#####################################
## 2.0. Write & Create delta objects, files and store in Data Lake using function
#####################################
print("\n" + "Generating Delta files, database and table...")
fn_createDeltaObjects(df_transformedQuery, str_layer, str_subject, str_security, str_entityName, "overwrite")

#####################################
## 2.1. Remove (vacuum) old Delta files that is stored in Data Lake using function - default retention period is 7 days
#####################################
#print("\n" + "Removing (vacuum) older Delta files ...")
#fn_vacuumDeltaTable(str_layer, str_subject, str_security, str_entityName)

# COMMAND ----------

# DBTITLE 1,Append to Metadata SQL Database ColumnDetails Table
####################################
## 3.0 Get Business Keys, Late Arriving Column, Default Mapping Value and Snowflake Resolve Order from List
#####################################
## Get Business Keys from List and Create Dataframe
df_businessKeyColumn   = spark.createDataFrame(lst_transformedBusinessKey, StringType())
df_businessKeyColumn   = df_businessKeyColumn \
                            .withColumnRenamed("value", "ColumnName") \
                            .withColumn("IsBusinessKey", lit(True))

## Get Late Arriving Column from List and Create Dataframe
df_lateArrivingColumn  = spark.createDataFrame(lst_transformedLateArrivingColumn, StringType())
df_lateArrivingColumn  = df_lateArrivingColumn \
                            .withColumnRenamed("value", "ColumnName") \
                            .withColumn("IsLateArriving", lit(True))

## Get Default Mapping Values from Lis and Create Schema and Dataframe 
df_defaultMappingSchema  = StructType([StructField("column", StringType()) \
                                      ,StructField("value", StringType()) ])
df_defaultMappingValue = spark.createDataFrame(lst_transformedDefaultValue, schema = df_defaultMappingSchema)
df_defaultMappingValue = df_defaultMappingValue \
                            .withColumnRenamed("column", "ColumnName") \
                            .withColumnRenamed("value", "DefaultMapping")

## Get SnowflakeResolveOrder Lits and Create Schema and Dataframe
df_snowflakeResolveOrder  = StructType([StructField("ColumnName", StringType()) \
                                      ,StructField("SnowflakeResolveOrder", IntegerType()) ])
df_snowflakeResolveOrder = spark.createDataFrame(lst_snowflakeResolveOrder, schema = df_snowflakeResolveOrder)

####################################
## 4.0 Get Column Details Metadata from Delta Table
####################################
## Get Columns, DataType from Delta Table

schema_dataType = StructType([
  StructField("ColumnName", StringType(), True),
  StructField("DataType", StringType(), True),
])

df_deltaTableMetadata = spark.createDataFrame([(c.name,  c.dataType) for c in spark.catalog.listColumns(str_entityName, str_subject)], schema=schema_dataType)  \
                            .filter(col("DataType") != "")

## Join Dataframe - DeltaTable Columns with Business Keys, Late Arriving, Default Mapping Columns, Snowflake Resolve Order and add EntityID column
df_deltaTableMetadata   = df_deltaTableMetadata \
                            .join(df_businessKeyColumn,  "ColumnName" ,how = 'left').na.fill({'IsBusinessKey': False}) \
                            .join(df_lateArrivingColumn, "ColumnName" ,how = 'left').na.fill({'IsLateArriving': False}) \
                            .join(df_defaultMappingValue,"ColumnName" ,how = 'left') \
                            .join(df_snowflakeResolveOrder, "ColumnName" ,how = 'left').na.fill({'SnowflakeResolveOrder': 999}) \
                            .withColumn("EntityID", lit(int_entityID))


# display(df_deltaTableMetadata)

#####################################
## 5.0. Append to Metadata SQL Database ColumnDetails Table using function
#####################################
print("\n" + "Appending objects to Metadata Database (DataLib_Control)...")

## If entity columns does not exists in Metadata SQL Database ColumnDetails Table then append else throw error 
fn_appendSqlColumnsDetail(int_entityID, str_entityName, df_deltaTableMetadata)

# COMMAND ----------



# COMMAND ----------

print("------------------------------------------------")  
print("Post Transformed Processing Completed.")
