# Databricks notebook source
# MAGIC %run ../../../Datalib/Common/Master

# COMMAND ----------

#Get Entity MetaData Details from DataLib
str_GetEntitiesGXLP210 = """SELECT	Concat(S.name,'25')+'.'+E.Name EntityName
FROM	METADATA.ENTITY E
JOIN	METADATA.SUBJECT S
ON		E.SUBJECTID = S.SUBJECTID
WHERE	S.NAME LIKE '%GXLP' AND SUBLAYER IN ('Pre') and e.name like '%ledg%' and e.name not like '%ledg%H%' --and e.name not like '%PRM%' and e.name not like '%PM_U%'
order by s.name,sequence
"""

str_resultgxlp210 = fn_queryControlDatabase(str_GetEntitiesGXLP210)


# COMMAND ----------

for GXLPEntity in str_resultgxlp210:
    str_Ename = GXLPEntity[0].split('.')
    str_FilePath = f"dbfs:/mnt/main/Transformed/{str_Ename[0]}/Internal/{str_Ename[1]}"
    print(str_Ename[0] + str_Ename[1])
    print(str_FilePath)
    spark.sql(""" 
               CREATE DATABASE IF NOT EXISTS {0}
               COMMENT '{0} database for {1}'
               LOCATION '{2}'
               """.format(str_Ename[0], str_Ename[1], str_FilePath)
               )

    ## create delta table
    spark.sql(""" 
              CREATE TABLE IF NOT EXISTS {0}.{1}
              USING DELTA 
              LOCATION '{2}'
              """.format(str_Ename[0], str_Ename[1], str_FilePath)
             )
