# Databricks notebook source
## set parameter
dbutils.widgets.text("EntityName","dbo_LEDG_HISTORY","EntityName")
dbutils.widgets.text("Subject","GXLP210","Subject")
dbutils.widgets.text("Security","Internal","Security")
dbutils.widgets.text("DataMart","MyMI","DataMart")
dbutils.widgets.text("DataMartLoadID","1","DataMartLoadID")
dbutils.widgets.text("InitialDateTime","00000000_0000","InitialDateTime")
dbutils.widgets.text("EndDateTime","99999999_9999","EndDateTime")
dbutils.widgets.text("LoadType","FullRefresh","LoadType")

## set variables
str_entityName      = dbutils.widgets.get("EntityName")
str_subject         = dbutils.widgets.get("Subject")
str_security        = dbutils.widgets.get("Security")
str_dataMart        = dbutils.widgets.get("DataMart")
str_dataMartLoadID  = dbutils.widgets.get("DataMartLoadID")
str_initialDateTime   = dbutils.widgets.get("InitialDateTime")
str_endDateTime     = dbutils.widgets.get("EndDateTime")
str_loadType        = dbutils.widgets.get("LoadType")

# COMMAND ----------

# DBTITLE 1,Pre Transformed Call
# MAGIC %run ../Transformed/preTransformProcessing
