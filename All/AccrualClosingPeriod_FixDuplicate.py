# Databricks notebook source
# MAGIC %md # AccrualClosingPeriod_FixDuplicate

# COMMAND ----------

# MAGIC %md ## 1. Set Variables

# COMMAND ----------

str_cleansedFilePath_20220127 =  f"dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220127/20220127_20/dbo_AccrualClosedPeriod_20220127_2000.parquet"
str_cleansedFilePath_20220128 =  f"dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220128/20220128_20/dbo_AccrualClosedPeriod_20220128_2000.parquet"
str_cleansedFilePath_20220129 =  f"dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220129/20220129_20/dbo_AccrualClosedPeriod_20220129_2000.parquet"
str_cleansedFilePath_20220130 =  f"dbfs:/mnt/main/Cleansed/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220130/20220130_20/dbo_AccrualClosedPeriod_20220130_2000.parquet"
str_rawFilePath_20220127 =  f"dbfs:/mnt/main/Raw/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220127/20220127_20/dbo_AccrualClosedPeriod_20220127_2000/data_fae27dda-56b9-40b6-8969-b9f51aed5e3e_3712ba0b-b6c3-4e73-9f6e-b39515436201.parquet"
str_rawFilePath_20220128 =  f"dbfs:/mnt/main/Raw/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220128/20220128_20/dbo_AccrualClosedPeriod_20220128_2000/data_6facbb6f-f915-4307-8852-71722d96ac74_af189370-3c68-4f7c-b0f0-2df259f76e11.parquet"
str_rawFilePath_20220129 =  f"dbfs:/mnt/main/Raw/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220129/20220129_20/dbo_AccrualClosedPeriod_20220129_2000/data_985b1e28-81a7-4fc8-8558-54957edfcfad_3529fc6b-8e45-4a04-9b10-b03a75a29f47.parquet"
str_rawFilePath_20220130 =  f"dbfs:/mnt/main/Raw/GXLP/Internal/dbo_AccrualClosedPeriod/2022/202201/20220130/20220130_20/dbo_AccrualClosedPeriod_20220130_2000/data_16140dc0-18ee-4745-8974-9ebc50e3e449_c04b2f6b-4dc1-4c64-b679-53cc8489f4d8.parquet"

# COMMAND ----------

# MAGIC %md ## 2. Check Data

# COMMAND ----------

df_cleansedAccrualClosedPeriod_20220127 = spark.read.parquet(str_cleansedFilePath_20220127)
df_cleansedAccrualClosedPeriod_20220128 = spark.read.parquet(str_cleansedFilePath_20220128)
df_cleansedAccrualClosedPeriod_20220129 = spark.read.parquet(str_cleansedFilePath_20220129)
df_cleansedAccrualClosedPeriod_20220130 = spark.read.parquet(str_cleansedFilePath_20220130)
df_rawAccrualClosedPeriod_20220127 = spark.read.parquet(str_rawFilePath_20220127)
df_rawAccrualClosedPeriod_20220128 = spark.read.parquet(str_rawFilePath_20220128)
df_rawAccrualClosedPeriod_20220129 = spark.read.parquet(str_rawFilePath_20220129)
df_rawAccrualClosedPeriod_20220130 = spark.read.parquet(str_rawFilePath_20220130)

print(df_cleansedAccrualClosedPeriod_20220127.count())
print(df_cleansedAccrualClosedPeriod_20220128.count())
print(df_cleansedAccrualClosedPeriod_20220129.count())
print(df_cleansedAccrualClosedPeriod_20220130.count())
print(df_rawAccrualClosedPeriod_20220127.count())
print(df_rawAccrualClosedPeriod_20220128.count())
print(df_rawAccrualClosedPeriod_20220129.count())
print(df_rawAccrualClosedPeriod_20220130.count())

# COMMAND ----------

df_cleansedAccrualClosedPeriod_20220128.where("Period != 201601").count()

# COMMAND ----------

df_cleansedAccrualClosedPeriod_20220128.where("Period != 201601").coalesce(1).write.format("parquet").mode("overwrite").save(str_cleansedFilePath_20220128)

# COMMAND ----------

# MAGIC %md ## 3. Fix Data

# COMMAND ----------

dbutils.fs.cp(str_rawFilePath_20220127, str_rawFilePath_20220129)

# COMMAND ----------

dbutils.fs.cp(str_rawFilePath_20220127, str_rawFilePath_20220130)

# COMMAND ----------

dbutils.fs.cp(str_cleansedFilePath_20220127, str_cleansedFilePath_20220129, True)

# COMMAND ----------

dbutils.fs.cp(str_cleansedFilePath_20220127, str_cleansedFilePath_20220130, True)