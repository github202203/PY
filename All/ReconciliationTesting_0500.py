# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   reconciliation.reconciliation_output
# MAGIC WHERE DestinationName = 'MyMI'  
# MAGIC AND LevelID = 500
# MAGIC AND DataLoadCode = '2021120201'
# MAGIC ORDER BY Variance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM MyMI_Pre_Eclipse.dbo_PolicyPremDetail; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM MyMI_Pre_Velocity.dbo_ClaimTran;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM MyMI_Pre_Velocity.dbo_ClaimTran;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM mymi_Pre_Eclipse.dbo_BusinessCode;

# COMMAND ----------

try:
    spark.sql(f"""
      ALTER TABLE
        reconciliation.reconciliation_output
      ADD
        COLUMNS (
          ReconciliationType STRING         AFTER DestinationEntityName,
          IsToleranceRatio   BOOLEAN        AFTER Variance,    
          ToleranceSetGreen  DECIMAL(19, 4) AFTER IsToleranceRatio,
          ToleranceSetAmber  DECIMAL(19, 4) AFTER ToleranceSetGreen,
          ToleranceActual    DECIMAL(19, 4) AFTER ToleranceSetAmber
        )
    """)    
except Exception as e:
  print("Failed to alter Reconciliation Output Table, error message:", e)
  raise(e)    

# COMMAND ----------

SELECT count(*) FROM MyMI_Pre_Eclipse.dbo_PolicyPrem; 