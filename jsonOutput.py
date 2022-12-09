# Databricks notebook source
from pyspark.sql.functions import desc, row_number, dense_rank, col, lit, lag, expr, count, when, explode

from delta.tables import *

# COMMAND ----------

str_identifiersFilePath = "/mnt/landing/mdm/DSS/ExchangeRates/OnDemandEODPricingRequestWithIdentifiersOutput.json"
str_instrumentFilePath = "/mnt/landing/mdm/DSS/ExchangeRates/OnDemandEODPricingRequestWithInstrumentListOutput.json"

df_identifiers = (spark
                  .read
                  .json(str_identifiersFilePath) 
                  .withColumn("data", explode("Contents"))
                  .select("data.*")
                 )
df_instrument = (spark
                 .read
                 .json(str_instrumentFilePath) 
                 .withColumn("data", explode("Contents"))
                 .select("data.*")
                )

# COMMAND ----------

print("Identifiers row count: "+str(df_identifiers.count()))
print("Instrument row count: "+str(df_instrument.count()))

print("Identifiers col count: "+str(len(df_identifiers.columns)))
print("Instrument col count: "+str(len(df_instrument.columns)))

# COMMAND ----------

display(df_identifiers.filter((col("RIC")=="EURLRD=R") & (col("Security Description")=="Euro/Liberian Dollar FX Cross Rate")))
display(df_instrument.limit(1))
