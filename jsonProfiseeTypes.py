# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import desc, row_number, dense_rank, col, lit, lag, expr, count, when, explode
from pyspark.sql.types import *

from delta.tables import *

import requests
import json

# COMMAND ----------

# DBTITLE 1,Struct Entities Attributes response
schema_profiseeTypes = (StructType([
    StructField("data", ArrayType(StructType([
        StructField("attributeType", LongType(), True),
        StructField("changeTrackingGroup", LongType(), True),
        StructField("dataType", LongType(), True),
        StructField("dataTypeInformation", LongType(), True),
        StructField("displayWidth", LongType(), True),
        StructField("domainEntityId", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("internalId", LongType(), True),
            StructField("isReferenceValid", BooleanType(), True)
        ]), True),
        StructField("domainEntityIsFlat", BooleanType(), True),
        StructField("domainEntityPermission", LongType(), True),
        StructField("fullyQualifiedName", StringType(), True),
        StructField("isCode", BooleanType(), True),
        StructField("isName", BooleanType(), True),
        StructField("isReadOnly", BooleanType(), True),        
        StructField("isSystem", BooleanType(), True),
        StructField("sortOrder", LongType(), True),
        StructField("profiseePermission", LongType(), True),
        StructField("longDescription", StringType(), True),
        StructField("isAlwaysRequired", BooleanType(), True),
        StructField("isRestricted", BooleanType(), True),
        StructField("displayOrder", LongType(), True),
        StructField("externalSystem", StructType([
            StructField("id", StringType(), True),
            StructField("internalId", LongType(), True),
            StructField("name", StringType(), True),
            StructField("isReferenceValid", BooleanType(), True)
        ]), True),
        StructField("domainEntityIsSystemAware", BooleanType(), True),
        StructField("isClearOnClone", BooleanType(), True),
        StructField("doShowTime", BooleanType(), True),
        StructField("multiLevelContext", StringType(), True),
        StructField("integrationName", StringType(), True),
        StructField("isIndexed", BooleanType(), True),
        StructField("isUnique", BooleanType(), True),
        StructField("hasDefault", BooleanType(), True),
        StructField("defaultValue", StringType(), True),
        StructField("auditInfo", StructType([
            StructField("createdDateTime", StringType(), True),
            StructField("createdUserId", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("internalId", LongType(), True),
                StructField("isReferenceValid", BooleanType(), True)
            ]), True),
            StructField("updatedDateTime", StringType(), True),
            StructField("updatedUserId", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("internalId", LongType(), True),
                StructField("isReferenceValid", BooleanType(), True)
            ]), True)
        ]), True),
        StructField("identifier", StructType([
            StructField("memberType", LongType(), True),
            StructField("entityId", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("internalId", LongType(), True),
                StructField("isReferenceValid", BooleanType(), True)
            ]), True),
            StructField("modelId", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("internalId", LongType(), True),
                StructField("isReferenceValid", BooleanType(), True)
            ]), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("internalId", LongType(), True),
            StructField("isReferenceValid", BooleanType(), True),
            StructField("modelId", StructType([
                StructField("id", StringType(), True),
                StructField("internalId", LongType(), True),
                StructField("isReferenceValid", BooleanType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("permission", LongType(), True)
        ]), True),
        
    ])
                                 )),
    StructField("totalRecords", LongType(), True)
])
                       )

# COMMAND ----------

# DBTITLE 1,Struct and DF to create Data Types
schema_dataType = StructType([
  StructField('dataType', LongType(), True),
  StructField('dataTypeDescription', StringType(),True)
])

arr_dataType = [(0, "NotSpecified")
                , (1, "Text")
                , (2, "Number")
                , (3, "DateTime")
                , (4, "Date")
                , (6, "Link")]

df_dataType = spark.createDataFrame(data=arr_dataType, schema=schema_dataType)

# COMMAND ----------

arr_profiseeTables = ["LoV_CurrencyStatus", "LoV_PublishStatus", "LoV_YesNo", "Ref_BritEntity", "Ref_BritEntityType", "Ref_BusinessClass", "Ref_ClaimDivision", "Ref_ClaimPortfolio", "Ref_Conformed_BritEntity", "Ref_Conformed_LloydsSyndicate", "Ref_Conformed_PlacingBasis", "Ref_Conformed_PolicyLineStatus",
                      "Ref_Currency", "Ref_Domain", "Ref_ExchangeRate", "Ref_GroupClass", "Ref_GroupDivision", "Ref_GroupPortfolio", "Ref_LegalEntity", "Ref_LloydsSyndicate", "Ref_NewRenewal", "Ref_PlacingBasis", "Ref_PlacingChannel", "Ref_PolicyActiveStatus",
                      "Ref_PolicyLineStatus", "Ref_RateSet", "Ref_RateSource", "Ref_ReportingEntity_Map", "Ref_SourceSystem"]

dbutils.widgets.dropdown("profiseeTables", "LoV_CurrencyStatus", arr_profiseeTables)

# COMMAND ----------

# DBTITLE 1,API call to get entity attributes, then convert response to df
str_entityName = dbutils.widgets.get("profiseeTables")

str_url = "https://profiseecloud.co.uk/britinsdev/rest/v1/Entities/"+str_entityName+"/attributes"
dict_headers = {"x-api-key": "365d3b33592444aa83c4b88f2beed252",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br"
               }

response = requests.get(url = str_url,
                        headers = dict_headers
)

df_profiseeTypes = (spark
                    .read
                    .schema(schema_profiseeTypes)
                    .json(sc.parallelize([response.text])) 
                    .withColumn("data", explode("data"))
                    .select("data.*")
                   )

display(df_profiseeTypes)

# COMMAND ----------

# DBTITLE 1,Display key attributes for creating views
print(str_entityName)
display(df_profiseeTypes
        .join(df_dataType, on="dataType", how="left")
        .select("integrationName",
                "attributeType",
               "dataTypeDescription",
               "dataTypeInformation",
                "displayOrder",
                col("domainEntityId.name").alias("domainEntityName")
               )
        .orderBy("displayOrder")
       )

# COMMAND ----------

print(str_entityName)
display(df_profiseeTypes
        .filter(col("integrationName").isin(["ValidFrom","ValidTo"]))
       )

# COMMAND ----------

# DBTITLE 1,Get the delta names - so can cross check API output
df_Columns = spark.read.format("delta").load("/mnt/bronze/masterdata/Internal/Profisee/DeltaLake/"+str_entityName+"/")

arr_Columns = df_Columns.drop("BronzeStagingSystemLoadID", "BronzeSystemLoadID", "Current", "EffectiveDateUTC", "EndDateUTC", "RowNumber", "canDelete", "canUpdate",
                             "id", "internalId", "validationIssueClauseIds", "validationStatusID").columns
arr_Columns.sort()

arr_Columns
