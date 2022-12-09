# Databricks notebook source
# MAGIC %md ## 1. reconciliation.reconciliation_output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE
# MAGIC   DestinationName IN ('Eclipse', 'MDSProd', 'SequelClaims', 'Velocity', 'GXLP', 'MDSDev', 'Adhoc', 'FinancialPeriod')
# MAGIC   AND LevelID = 200
# MAGIC   AND DataLoadCode IN ('20220307_2200', '20220307_2000', '20220308_0000')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC -- AND LevelID = 500
# MAGIC AND DataLoadCode = '2022030701'
# MAGIC -- AND ToleranceFlag IN ("Red", "Amber")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND LevelID = 600
# MAGIC AND DataLoadCode IN ('2022030701')
# MAGIC AND DestinationSubjectName = 'MyMI_Trans_Eclipse'
# MAGIC AND DestinationEntityName IN ('FactClaimTransactionDWH', 'FactClaimTransactionEclipse')

# COMMAND ----------

# MAGIC %md ## 2. reconciliation.vw_reconciliation_output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.vw_reconciliation_output
# MAGIC WHERE 
# MAGIC   LevelID = 200
# MAGIC   

# COMMAND ----------

# MAGIC %md ## 3. reconciliation.reconciliation_dataload

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_dataload