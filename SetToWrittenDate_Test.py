# Databricks notebook source
# Databricks notebook source
# DBTITLE 1,Import PySpark Libraries
# Import Libraries
from pyspark.sql import *
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time
from dateutil import relativedelta
from enum import Enum
from typing import Any, List
from dateutil.relativedelta import relativedelta

import datalib.processing.snapshot as snp

import math
import re
import json
from html import escape
import pyspark
from delta.tables import *
import calendar

import py4j
import pyodbc
import time as tm

import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

import traceback
import logging

from decimal import Decimal

# COMMAND ----------

df_dup= spark.read.format("parquet").option("header", "true").load("dbfs:/mnt/main/Raw/Eclipse/Internal/dbo_PolicyDeduction/2022/202202/20220206/20220206_22/dbo_PolicyDeduction_20220206_2200/data_7d351984-9947-408e-bce6-8e3f933ed516_9ae14bbd-be8e-47ab-9a7f-b3fc49d01e67.parquet")

# COMMAND ----------

df_dup.dtypes

# COMMAND ----------

df_dup.schema.fieldNames()

# COMMAND ----------

df_dup.schema.fields

# COMMAND ----------

df_dup.select("Addition").collect()

# COMMAND ----------


df_dup.select("Narrative", "FirstDataType")

# COMMAND ----------

select(df_dup["AdministeredBy"]).collect()

# COMMAND ----------

display(df_dup)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MyMI_Curated_Group.DWH_DimDataHistory where DataMartLoadID like'202103%' order by DataMartLoadID desc

# COMMAND ----------

# MAGIC %run ../../Datalib/Common/Master

# COMMAND ----------

str_filepath=dbutils.fs.ls("dbfs:/mnt/main/Cleansed/Eclipse/Internal/dbo_CashBook")

# COMMAND ----------

for i in str_filepath:
  print(i)
  #print(i.name[0:4])

# COMMAND ----------

df_dup.columns

# COMMAND ----------

lst_transformedBusinessKey = []
lst_transformedLateArrivingColumn = []
lst_transformedDefaultValue = []
lst_snowflakeResolveOrder = []


# COMMAND ----------

from pyspark.sql.types import *
df_businessKeyColumn   = spark.createDataFrame(lst_transformedBusinessKey, StringType())

# COMMAND ----------

df_businessKeyColumn   = df_businessKeyColumn \
                            .withColumnRenamed("value", "ColumnName") \
                            .withColumn("IsBusinessKey", lit(True))

# COMMAND ----------

df_businessKeyColumn.show()

# COMMAND ----------

df_businessKeyColumn.columns

# COMMAND ----------

df_lateArrivingColumn  = spark.createDataFrame(lst_transformedLateArrivingColumn, StringType())
df_lateArrivingColumn  = df_lateArrivingColumn \
                            .withColumnRenamed("value", "ColumnName") \
                            .withColumn("IsLateArriving", lit(True))

# COMMAND ----------

df_lateArrivingColumn.show()

# COMMAND ----------

df_defaultMappingSchema  = StructType([StructField("column", StringType()) \
                                      ,StructField("value", StringType()) ])
df_defaultMappingValue = spark.createDataFrame(lst_transformedDefaultValue, schema = df_defaultMappingSchema)
df_defaultMappingValue = df_defaultMappingValue \
                            .withColumnRenamed("column", "ColumnName") \
                            .withColumnRenamed("value", "DefaultMapping")

# COMMAND ----------

df_snowflakeResolveOrder  = StructType([StructField("ColumnName", StringType()) \
                                      ,StructField("SnowflakeResolveOrder", IntegerType()) ])
df_snowflakeResolveOrder = spark.createDataFrame(lst_snowflakeResolveOrder, schema = df_snowflakeResolveOrder)

# COMMAND ----------

df_snowflakeResolveOrder.show()

# COMMAND ----------

schema_dataType = StructType([
  StructField("ColumnName", StringType(), True),
  StructField("DataType", StringType(), True),
])

# COMMAND ----------

df_deltaTableMetadata = spark.createDataFrame([(c.name,  c.dataType) for c in spark.catalog.listColumns('dbo_Territory', 'MyMI_Pre_Eclipse')], schema=schema_dataType)  \
                            .filter(col("DataType") != "")

# COMMAND ----------

df_deltaTableMetadata.show()

# COMMAND ----------

df_deltaTableMetadata   = df_deltaTableMetadata \
                            .join(df_businessKeyColumn,  "ColumnName" ,how = 'left').na.fill({'IsBusinessKey': False}) \
                            .join(df_lateArrivingColumn, "ColumnName" ,how = 'left').na.fill({'IsLateArriving': False}) \
                            .join(df_defaultMappingValue,"ColumnName" ,how = 'left') \
                            .join(df_snowflakeResolveOrder, "ColumnName" ,how = 'left').na.fill({'SnowflakeResolveOrder': 999}) \
                            .withColumn("EntityID", lit('12345'))

# COMMAND ----------

df_deltaTableMetadata.show()

# COMMAND ----------

df_columnlist= spark.catalog.listColumns('dbo_CTR_SEC', 'MyMI_Pre_GXLP')

# COMMAND ----------

print(df_columnlist)
#name='CONTRACT', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False)

# COMMAND ----------

# MAGIC  %sql
# MAGIC   select * from MyMI_Abst_Eclipse.Stg20_CommonLedger where LedgerTransID = '166156'

# COMMAND ----------

# MAGIC  %sql
# MAGIC   select * from MyMI_Trans_Eclipse.DimLedger where EclipseLedgerTransID = '166156'

# COMMAND ----------

lst_transformedBusinessKey = ['EclipseFinTransId','EclipseFinTransDetailId','EclipseLedgerTransID','First_DL_DWH_DimSourceSystem_ID']
lst_transformedLateArrivingColumn = []
lst_transformedDefaultValue       = []

CONCAT(     
        'EclipseFinTransDetailId=',         FinTransDetailId,    
        ';EclipseFinTransId=',              FinTransId,        
        ';EclipseLedgerTransID=',           LedgerTransID,
        ';First_DL_DWH_DimSourceSystem_ID=',SourceSystemID 
                )  AS BusinessKey
/**************************** FOREIGN KEYS  ****************************/ 
CONCAT('MDSSourceSystemCode=',        SourceSystemID)              AS FK_DWH_DimSourceSystem,

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT		   SourceSystemID
# MAGIC  			  ,LedgerTransId
# MAGIC  			  ,FinTransId
# MAGIC  			  ,FinTransDetailId
# MAGIC  			  ,AllocatedBy
# MAGIC  			  ,LedgerTransactionReference
# MAGIC  			  ,LedgerInstalmentNumber
# MAGIC               ,RowNumber
# MAGIC  FROM		  MyMI_Abst_Eclipse.Stg20_CommonLedger
# MAGIC  WHERE         RowNumber = 1
# MAGIC  ORDER BY	  FinTransId, 
# MAGIC                FinTransDetailId, 
# MAGIC                LedgerTransId

# COMMAND ----------

# MAGIC %sql
# MAGIC select BusinessKey,MAX(First_DL_Eclipse_DimClaimExt_ID) from mymi_curated_eclipse.eclipse_dimclaimext --where 
# MAGIC group by BusinessKey
# MAGIC --DL_Eclipse_DimClaimExt_ID=3267168

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MyMI_Trans_Velocity.DimPremium

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from MyMI_Pre_Eclipse.dbo_AuditLog

# COMMAND ----------

# MAGIC %sql
# MAGIC select Year(changedate),count(*) from MyMI_Pre_Eclipse.dbo_AuditLog where year(changedate) >= '2014'
# MAGIC GROUP BY year(changedate)

# COMMAND ----------

# MAGIC %sql
# MAGIC select MONTH(changedate),count(*) from MyMI_Pre_Eclipse.dbo_AuditLog where YEAR(changedate) = '2022'
# MAGIC GROUP BY MONTH(changedate)
# MAGIC 
# MAGIC --2 3 4[3 records less],5[1 rec less],6[3 rec less],7,8,9

# COMMAND ----------

# MAGIC %sql
# MAGIC select DAY(changedate),count(*) from MyMI_Pre_Eclipse.dbo_AuditLog where YEAR(changedate) = '2022' and MONTH(changedate) = 5
# MAGIC GROUP BY DAY(changedate)
# MAGIC 
# MAGIC --DAy 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select sourceid,count(*) from MyMI_Pre_Eclipse.dbo_AuditLog where YEAR(changedate) = '2022' and MONTH(changedate) = 5 and DAY(changedate)= 4
# MAGIC group by sourceid
# MAGIC 
# MAGIC --DAy 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select record,count(*) from MyMI_Pre_Eclipse.dbo_AuditLog where year(changedate) >= '2022' and MONTH(changedate) = 5 and DAY(changedate)= 4 --and record = 'D'
# MAGIC GROUP BY record

# COMMAND ----------

# MAGIC %sql
# MAGIC select source,field,count(*) from MyMI_Pre_Eclipse.dbo_AuditLog where year(changedate) >= '2022' and MONTH(changedate) = 5 and DAY(changedate)= 4 and record = 'U' 
# MAGIC GROUP BY source,field

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MyMI_Pre_Eclipse.dbo_AuditLog where year(changedate) >= '2022' and MONTH(changedate) = 5 and DAY(changedate)= 4 and record = 'U' 
# MAGIC and source = 'ObjCode' and field = 'CodeValue'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sourceid,count(*) from MyMI_Pre_Eclipse.dbo_AuditLog where year(changedate) >= '2022' and MONTH(changedate) = 5 and DAY(changedate)= 4 and record = 'U' 
# MAGIC and source = 'ObjCode' and field = 'CodeValue'
# MAGIC GROUP BY sourceid  ---17884375

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MyMI_Pre_Eclipse.dbo_AuditLog where year(changedate) >= '2022' and MONTH(changedate) = 5 and DAY(changedate)= 4 and record = 'U' 
# MAGIC and source = 'ObjCode' and field = 'CodeValue' and sourceid='17884368'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mymi_abst_eclipse.stg10_auditlog

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT       
# MAGIC     x.*        
# MAGIC FROM dbo.make_parallel() AS mp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  a.ChangeDate 
# MAGIC         , UPPER(newvalue) newvalue
# MAGIC         , pl.policyid
# MAGIC  FROM    MyMI_Pre_Eclipse.dbo_AuditLog a
# MAGIC  INNER JOIN MyMI_Pre_Eclipse.dbo_policyline pl
# MAGIC  ON a.sourceid = pl.PolicyLineId
# MAGIC  WHERE   a.source = 'PolicyLine'
# MAGIC AND UPPER(a.ChangedBy) <> 'ECLIPSEUW'
# MAGIC AND a.sourceid='984628' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,InsDate as DateEntered
# MAGIC from MyMI_Pre_Eclipse.dbo_Policy  where PolicyId='1026905'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,InsDate as DateEntered
# MAGIC from MyMI_Trans_Eclipse.dbo_Policy  where PolicyId='1026905'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM MyMI_Pre_GXLP.dbo_LEDG_COM

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE  MyMI_Pre_GXLP.dbo_LEDG_COM

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT  a.ChangeDate 
# MAGIC        , UPPER(newvalue) newvalue
# MAGIC        , pl.policyid
# MAGIC FROM    MyMI_Pre_Eclipse.dbo_AuditLog a
# MAGIC INNER JOIN MyMI_Pre_Eclipse.dbo_policyline pl
# MAGIC ON a.sourceid = pl.PolicyLineId
# MAGIC WHERE   a.source = 'PolicyLine'
# MAGIC AND UPPER(a.ChangedBy) <> 'ECLIPSEUW'
# MAGIC AND newvalue= 'WRITTEN'
