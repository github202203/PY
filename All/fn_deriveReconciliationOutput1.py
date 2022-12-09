# Databricks notebook source
# MAGIC %md # Notebook Function
# MAGIC 
# MAGIC ####  
# MAGIC * **Title   :** fn_deriveReconciliationOutput
# MAGIC * **Description :** Derive Reconciliation Output Fields.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 02/11/2021 | Raja Murugan | Derive Reconciliation Output Fields. |

# COMMAND ----------

# MAGIC %md # 1. Derive Reconciliation Output

# COMMAND ----------

def fn_deriveReconciliationOutput1(df_reconciliationOutput: DataFrame, dct_tolerance: dict) -> DataFrame:
  try: 
  
    df_reconciliationOutput = df_reconciliationOutput.withColumn("SourceValue"      , expr("IFNULL(SourceValue, 0)")) \
                                                     .withColumn("DestinationValue" , expr("IFNULL(DestinationValue, 0)")) \
                                                     .withColumn("Variance"         , expr("DestinationValue - SourceValue")) \
                                                     .withColumn("ToleranceSetGreen", expr(f"""CASE ReconciliationType 
                                                                                                 WHEN 'Count' THEN {dct_tolerance['GreenCount']}
                                                                                                 WHEN 'Sum'   THEN {dct_tolerance['GreenSum']}
                                                                                               END
                                                                                            """)) \
                                                     .withColumn("ToleranceSetAmber", expr(f"""CASE ReconciliationType 
                                                                                                 WHEN 'Count' THEN {dct_tolerance['AmberCount']}
                                                                                                 WHEN 'Sum'   THEN {dct_tolerance['AmberSum']}
                                                                                               END
                                                                                            """)) \
                                                     .withColumn("ToleranceActual"  , expr(f"IFNULL(IF(IsToleranceRatio = TRUE, ABS(Variance / SourceValue), ABS(Variance)), 0)")) \
                                                     .withColumn("ToleranceFlag"    , expr(f"""CASE 
                                                                                                 WHEN (SourceValue = 0 AND DestinationValue != 0) THEN 'Red'
                                                                                                 WHEN ToleranceActual <= ToleranceSetGreen THEN 'Green'
                                                                                                 WHEN ToleranceActual <= ToleranceSetAmber THEN 'Amber'
                                                                                                 ELSE 'Red'
                                                                                               END
                                                                                            """)) \
                                                     .replace('None', None)
    return df_reconciliationOutput

  except Exception as e:
    log_status(LogLevel.ERROR, "Failed to Derive Reconciliation Output Fields, error message:", e)
    raise(e)