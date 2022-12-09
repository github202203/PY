# Databricks notebook source
# MAGIC %run ../../../Datalib/Common/Master

# COMMAND ----------

#Get Entity MetaData Details from DataLib
str_GetEntitiesGXLP210 = """SELECT	S.name+'.'+E.Name EntityName
FROM	METADATA.ENTITY E
JOIN	METADATA.SUBJECT S
ON		E.SUBJECTID = S.SUBJECTID
WHERE	S.NAME LIKE '%GXLP210' AND SUBLAYER IN ('Abstract','Transformed') --and e.EntityID = 5483
order by s.name,sequence"""

str_resultgxlp210 = fn_queryControlDatabase(str_GetEntitiesGXLP210)


# COMMAND ----------

spark.sql("drop table IF EXISTS MyMI_GXLP210_Testing.Datacheckscount")
for GXLPEntity in str_resultgxlp210:
    ##Generating Select Statement Dynamically 
    str_Select = "SELECT * FROM "
    ## Loading GXLP 2.5 data to Dataframes and Renaming the columns to suffix with 25
    df_GXLP25Select = spark.sql(str_Select+GXLPEntity[0].replace('210',''))
    df_GXLP25Select = df_GXLP25Select.toDF(*(cols.replace(cols,cols+"25") for cols in df_GXLP25Select.columns))
    ## Loading GXLP 2.10 data to Dataframes 
    df_GXLP210Select = spark.sql(str_Select+GXLPEntity[0])
    df_ColumnsGXLP25 = df_GXLP25Select.columns
    df_ColumnsGXLP210 = df_GXLP210Select.columns
    df_GXLP210Select = df_GXLP210Select.withColumn("GXLP210",lit("Yes"))
    df_GXLP25Select = df_GXLP25Select.withColumn("GXLP25",lit("Yes"))
    df_GXLP25Select = df_GXLP25Select.withColumn("RowHash25",xxhash64(*df_GXLP25Select.schema.names))
    #display(df_GXLP25Select)
    df_GXLP210Select = df_GXLP210Select.withColumn("RowHash",xxhash64(*df_GXLP210Select.schema.names))
    #display(df_GXLP210Select)
    df_Join = df_GXLP210Select.join(df_GXLP25Select,df_GXLP210Select.RowHash==df_GXLP25Select.RowHash25,"fullouter")
    #df_Join = df_GXLP210Select.join(df_GXLP25Select,[col(GXLP25)==col(GXLP210) for (GXLP25,GXLP210) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)],"fullouter")
    df_Join.createOrReplaceTempView("GXLPJoinOutput")
    var_df = spark.sql("SELECT *,CASE WHEN GXLP25 IS NULL THEN 'Missing 2.5' WHEN GXLP210 IS NULL THEN 'Missing 2.10' ELSE 'MATCHED' END MatchStatus FROM GXLPJoinOutput")
    #var_df.show()
    str_Ename = GXLPEntity[0].split('.')
    str_FilePath = f"dbfs:/mnt/main/Transformed/MyMI_GXLP210_Testing/Internal/{str_Ename[1]}"
    #var_df.write.mode('overwrite').parquet(str_FilePath)
    var_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str_FilePath)
    ## create delta database
    spark.sql(""" 
               CREATE DATABASE IF NOT EXISTS {0}
               COMMENT '{0} database for {1}'
               LOCATION '{2}'
               """.format('MyMI_GXLP210_Testing', str_Ename[1], str_FilePath)
               )

    ## create delta table
    spark.sql(""" 
              CREATE TABLE IF NOT EXISTS {0}.{1}
              USING DELTA 
              LOCATION '{2}'
              """.format('MyMI_GXLP210_Testing', str_Ename[1], str_FilePath)
             )
    #####################################
    ## Print Message
    #####################################
    print ("mgs: Successfully created '" + str_Ename[1] + "' delta table")
    print ("mgs: Data check started to check match/unmatch '" + str_Ename[1] + "' delta table")
    str_Query = f""" SELECT count(*) as cnt,MatchStatus FROM MYMI_GXLP210_TESTING.{str_Ename[1]} GROUP BY MatchStatus """
    df_Count = spark.sql(str_Query)
    df_Count.withColumn("TableName",lit(str_Ename[1])).show()
    #df_test.show()
    str_FilePath = f"dbfs:/mnt/main/Transformed/MyMI_GXLP210_Testing/Internal/Datacheckscount"
    #var_df.write.mode('overwrite').parquet(str_FilePath)
    #df = spark.sql("select count(*) from MyMI_GXLP210_Testing.Datacheckscount")
    print ("mgs: Datacheck count updated for '" + str_Ename[1] + "' delta table")
    #=====================================================================================#
    """ df_Join = df_GXLP210Select.join(df_GXLP25Select,[col(GXLP25)==col(GXLP210) for (GXLP25,GXLP210) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)],"fullouter")
    df_Join = df_Join.withColumn("JoinFlag",when((col(GXLP25[0])==col(GXLP210[0]) for (GXLP25[0],GXLP210[0]) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)),lit("Matched")).otherwise(lit("Matched")))
    display(df_Join)
    df_Join = df_GXLP210Select.join(df_GXLP25Select,[col(GXLP25)==col(GXLP210) for (GXLP25,GXLP210) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)],"fullouter")
    df_Join = df_Join.withColumn("JoinFlag",when((col(GXLP25[0])==col(GXLP210[0]) for (GXLP25[0],GXLP210[0]) in zip(df_ColumnsGXLP210,df_ColumnsGXLP25)),lit("Matched")).otherwise(lit("Matched")))
    display(df_Join)
    #df_Join.withColumn('Join Flag',when())
    #str_Ename = GXLPEntity[0].split('.')
    #str_FilePath = f"dbfs:/mnt/main/Transformed/MyMI_GXLP210_Testing/Internal/{str_Ename[1]}"
    #df_Join.write.parquet(str_FilePath)"""
