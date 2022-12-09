# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import explode
from pyspark.sql.types import *

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

# DBTITLE 1,API call to get entity attributes, then convert response to df
str_entityName = "LoV_CurrencyStatus"

str_url = "https://profiseecloud.co.uk/britinsdev/rest/v1/Entities/"+str_entityName+"/attributes"
dict_headers = {"x-api-key": "{api-key}", # this could be stored as an ADB secret
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br"
               }

response = requests.get(url = str_url,
                        headers = dict_headers
)

df_profiseeTypes = (spark
                    .read
                    .schema(schema_profiseeTypes) # this is the key line for ensuring the nested structs are available within the output
                    .json(sc.parallelize([response.text])) 
                    .withColumn("data", explode("data"))
                    .select("data.*")
                   )

display(df_profiseeTypes)
