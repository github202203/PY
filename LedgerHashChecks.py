# Databricks notebook source
# DBTITLE 1,Currency Rec
# MAGIC %sql
# MAGIC SELECT A.CURRENCY,b.currency,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR
# MAGIC FROM (
# MAGIC       SELECT CURRENCY,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by CURRENCY
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT CURRENCY,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by CURRENCY
# MAGIC       )B
# MAGIC       ON A.CURRENCY = B.CURRENCY
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT CURRENCY,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by CURRENCY
# MAGIC       )C
# MAGIC       ON A.CURRENCY = C.CURRENCY

# COMMAND ----------

# DBTITLE 1,Event Rec
# MAGIC %sql
# MAGIC SELECT A.Event Allocated_Event,b.Event Ledger_Event,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR
# MAGIC FROM (
# MAGIC       SELECT Event,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by Event
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Event,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Event
# MAGIC       )B
# MAGIC       ON A.Event = B.Event
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Event,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by Event
# MAGIC       )C
# MAGIC       ON A.Event = C.Event

# COMMAND ----------

# DBTITLE 1,Period Rec
# MAGIC %sql
# MAGIC SELECT A.Period Allocated_Period,b.Period Ledger_Period,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR
# MAGIC FROM (
# MAGIC       SELECT Period,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by Period
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Period,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Period
# MAGIC       )B
# MAGIC       ON A.Period = B.Period
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Period,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by Period
# MAGIC       )C
# MAGIC       ON A.Period = C.Period

# COMMAND ----------

# DBTITLE 1,Contract Rec
# MAGIC %sql
# MAGIC SELECT A.Contract Allocated_Contract,b.Contract Ledger_Contract,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR
# MAGIC FROM (
# MAGIC       SELECT Contract,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by Contract
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Contract,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Contract
# MAGIC       )B
# MAGIC       ON A.Contract = B.Contract
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Contract,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by Contract
# MAGIC       )C
# MAGIC       ON A.Contract = C.Contract

# COMMAND ----------

# DBTITLE 1,Sub_Event Rec
# MAGIC %sql
# MAGIC SELECT A.Sub_Event Allocated_Sub_Event ,b.Sub_Event  Ledger_Sub_Event ,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR
# MAGIC FROM (
# MAGIC       SELECT Sub_Event ,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by Sub_Event 
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Sub_Event ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Sub_Event 
# MAGIC       )B
# MAGIC       ON A.Sub_Event  = B.Sub_Event 
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Sub_Event ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by Sub_Event 
# MAGIC       )C
# MAGIC       ON A.Sub_Event  = C.Sub_Event 

# COMMAND ----------

# DBTITLE 1,REIN_CD Rec
# MAGIC %sql
# MAGIC SELECT A.REIN_CD Allocated_REIN_CD ,b.REIN_CD  Ledger_REIN_CD ,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR,(Allocated_AMT_CURR - b.AMT_CURR),(Allocated_AMT_CURR - C.AMT_CURR)
# MAGIC 
# MAGIC FROM (
# MAGIC       SELECT REIN_CD ,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by REIN_CD 
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT REIN_CD ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by REIN_CD 
# MAGIC       )B
# MAGIC       ON A.REIN_CD  = B.REIN_CD 
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT REIN_CD ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by REIN_CD 
# MAGIC       )C
# MAGIC       ON A.REIN_CD  = C.REIN_CD 

# COMMAND ----------

# DBTITLE 1,Broker_CD Rec
# MAGIC %sql
# MAGIC SELECT A.Broker_CD Allocated_Broker_CD ,b.Broker_CD  Ledger_Broker_CD ,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR,(Allocated_AMT_CURR - b.AMT_CURR),(Allocated_AMT_CURR - C.AMT_CURR)
# MAGIC FROM (
# MAGIC       SELECT Broker_CD ,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by Broker_CD 
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Broker_CD ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Broker_CD 
# MAGIC       )B
# MAGIC       ON A.Broker_CD  = B.Broker_CD 
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Broker_CD ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by Broker_CD 
# MAGIC       )C
# MAGIC       ON A.Broker_CD  = C.Broker_CD 

# COMMAND ----------

# DBTITLE 1,YOA Rec
# MAGIC %sql
# MAGIC SELECT A.LORS_YOA Allocated_YOA ,b.YOA  Ledger_YOA ,Allocated_AMT_CURR,b.AMT_CURR Ledg_25_Amt_CUR,c.AMT_CURR Ledg_210_Amt_CUR,(Allocated_AMT_CURR - b.AMT_CURR),(Allocated_AMT_CURR - C.AMT_CURR)
# MAGIC FROM (
# MAGIC       SELECT LORS_YOA ,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by LORS_YOA 
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT YOA ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by YOA 
# MAGIC       )B
# MAGIC       ON A.LORS_YOA  = B.YOA 
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT LORS_YOA ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by LORS_YOA 
# MAGIC       )C
# MAGIC       ON A.LORS_YOA  = C.LORS_YOA 

# COMMAND ----------

# DBTITLE 1,Entity Rec
# MAGIC %sql
# MAGIC SELECT A.Entity_CD Allocated_Entity_CD ,b.Syndicate  Ledger_Syndicate ,COALESCE(Allocated_AMT_CURR,0) Allocated_AMT_CURR,COALESCE(b.AMT_CURR,0) Ledger25_AMT_CURR,COALESCE(c.AMT_CURR,0) Ledger210_AMT_CURR,COALESCE((Allocated_AMT_CURR - b.AMT_CURR),0) DifferenceAllocLedgandLedger25,COALESCE((Allocated_AMT_CURR - c.AMT_CURR),0) DifferenceAllocLedgandLedger210
# MAGIC FROM (
# MAGIC       SELECT Entity_CD ,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       group by Entity_CD 
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Syndicate ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       group by Syndicate 
# MAGIC       )B
# MAGIC       ON COALESCE(A.Entity_CD,'')  = COALESCE(B.Syndicate,'')
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT Entity_CD ,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp.DBO_LEDGER
# MAGIC       group by Entity_CD 
# MAGIC       )C
# MAGIC       ON COALESCE(A.Entity_CD,'') = COALESCE(C.Entity_CD,'')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.CONTRACT Ledger_Apportioned,a.AC_STAT2 GroupClass_Apportioned,b.CONTRACT Ledger25,B.AC_STAT2 GroupClass25,COALESCE(Allocated_AMT_CURR,0) Allocated_AMT_CURR,COALESCE(AMT_CURR,0) AMT_CURR,COALESCE((Allocated_AMT_CURR - b.AMT_CURR),0) Difference
# MAGIC FROM (
# MAGIC       SELECT CONTRACT,AC_STAT2,SUM(Allocated_AMT_CURR) Allocated_AMT_CURR
# MAGIC       FROM MyMI_Abst_GXLP.Stg20_LedgerApportionment
# MAGIC       --WHERE AC_STAT2 = 'GC0011'
# MAGIC       group by CONTRACT,AC_STAT2
# MAGIC      )A
# MAGIC FULL OUTER JOIN (
# MAGIC       SELECT CONTRACT,AC_STAT2,SUM(AMT_CURR) AMT_CURR
# MAGIC       FROM mymi_pre_gxlp25.DBO_LEDGER
# MAGIC       --WHERE AC_STAT2 = 'GC0011'
# MAGIC       group by CONTRACT,AC_STAT2
# MAGIC       )B
# MAGIC       ON A.AC_STAT2 = B.AC_STAT2
# MAGIC       AND A.CONTRACT = B.CONTRACT
