# Databricks notebook source
# MAGIC %md # MyMI_Pre_GXLP.dbo_Accrual

# COMMAND ----------

# MAGIC %md ## Count by Period

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   a.period,
# MAGIC   count(*),
# MAGIC   sum(AMT_GROSS) AMT_GROSS,
# MAGIC   sum(AMT_NET) AMT_NET
# MAGIC from
# MAGIC   MyMI_Pre_GXLP.dbo_Accrual a --where a.[period] in (202109, 202110)
# MAGIC group by
# MAGIC   a.Period
# MAGIC order by
# MAGIC   a.period

# COMMAND ----------

# MAGIC %md # Cleansed - dbo_Accrual

# COMMAND ----------

str_cleansedFilePath_20220202 =  f"dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_AccrualCurrentPeriod/2022/202202/20220202/20220202_20/dbo_AccrualCurrentPeriod_20220202_2000.parquet"
df_cleansedAccrualCurrentPeriod_20220202 = spark.read.parquet(str_cleansedFilePath_20220202)
print(df_cleansedAccrualCurrentPeriod_20220202.count())
df_cleansedAccrualCurrentPeriod_20220202.createOrReplaceTempView("vw_cleansedAccrualCurrentPeriod_20220202")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_cleansedAccrualCurrentPeriod_20220202 a
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;

# COMMAND ----------

str_cleansedFilePath_20220202 =  f"dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202202/20220202/20220202_20/dbo_AccrualClosedPeriod_20220202_2000.parquet"
df_cleansedAccrualClosedPeriod_20220202 = spark.read.parquet(str_cleansedFilePath_20220202)
print(df_cleansedAccrualClosedPeriod_20220202.count())
df_cleansedAccrualClosedPeriod_20220202.createOrReplaceTempView("vw_cleansedAccrualClosedPeriod_20220202")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_cleansedAccrualClosedPeriod_20220202 a
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;

# COMMAND ----------

str_cleansedFilePath_20220202 =  f"dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_AccrualHistorical/2022/202202/20220202/20220202_20/dbo_AccrualHistorical_20220202_2000.parquet"
df_cleansedAccrualHistorical_20220202 = spark.read.parquet(str_cleansedFilePath_20220202)
print(df_cleansedAccrualHistorical_20220202.count())
df_cleansedAccrualHistorical_20220202.createOrReplaceTempView("vw_cleansedAccrualHistorical_20220202")

# COMMAND ----------

# MAGIC %sql
# MAGIC /****** Count by Period with Filter ******/
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_cleansedAccrualHistorical_20220202  a
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;

# COMMAND ----------

# MAGIC %md # Raw - dbo_Accrual

# COMMAND ----------

str_rawFilePath =  f"dbfs:/mnt/main/Raw/GXLP/Internal/dbo_AccrualClosedPeriod"
df_rawAccrualClosedPeriod = spark.read.format("parquet").option("recursiveFileLookup","true").load(str_rawFilePath)
df_rawAccrualClosedPeriod.createOrReplaceTempView("vw_rawAccrualClosedPeriod")

# COMMAND ----------

# MAGIC %sql
# MAGIC /****** Count by Period with Filter ******/
# MAGIC SELECT
# MAGIC     a.period,
# MAGIC     count(1) AS SourceCount,
# MAGIC     sum(AMT_GROSS) AMT_GROSS,
# MAGIC     sum(AMT_NET) AMT_NET
# MAGIC FROM
# MAGIC     vw_rawAccrualClosedPeriod  a
# MAGIC GROUP BY
# MAGIC     a.Period
# MAGIC ORDER BY
# MAGIC     a.period
# MAGIC ;