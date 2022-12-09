# Databricks notebook source
# MAGIC %md # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** ReconciliationAnalysis_0500
# MAGIC * **Description :** Reconciliation Analysis - Level 500 - Source To PreTransformed.
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 16/12/2021 | Raja Murugan | Reconciliation Analysis - Level 500 - Source To PreTransformed. |

# COMMAND ----------

# MAGIC %md # 1. Preprocess

# COMMAND ----------

# MAGIC %md ## 1.1 Include Common

# COMMAND ----------

# MAGIC %run /Datalib/Common/Library/MasterLibrary

# COMMAND ----------

# MAGIC %run /Datalib/Common/SQLDataAccess/MasterSQLDataAccess

# COMMAND ----------

# MAGIC %run /Users/raja.murugan@britinsurance.com/ReconciliationAnalysis/0500/fn_reconciliationRecords0500

# COMMAND ----------

# MAGIC %run /Datalib/Common/Reconciliations/MasterReconciliations

# COMMAND ----------

# MAGIC %md # 2. Eclipse

# COMMAND ----------

# MAGIC %md ## 2.1 BusinessCode

# COMMAND ----------

df_destinationMinusSource_EclipseBusinessCode, df_sourceMinusDestination_EclipseBusinessCode = fn_reconciliationRecords0500("Eclipse", "BusinessCode", "MyMI_Pre_Eclipse", "dbo_BusinessCode", "BusinessCodeID")

print(f"Destination - Source: {df_destinationMinusSource_EclipseBusinessCode.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipseBusinessCode.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipseBusinessCode.display()

# COMMAND ----------

df_sourceMinusDestination_EclipseBusinessCode.display()

# COMMAND ----------

# MAGIC %md ## 2.2 ReportingClass

# COMMAND ----------

df_destinationMinusSource_EclipseReportingClass, df_sourceMinusDestination_EclipseReportingClass = fn_reconciliationRecords0500("Eclipse", "ReportingClass", "MyMI_Pre_Eclipse", "dbo_ReportingClass", "ReportingClassId")

print(f"Destination - Source: {df_destinationMinusSource_EclipseReportingClass.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipseReportingClass.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipseReportingClass.display()

# COMMAND ----------

df_sourceMinusDestination_EclipseReportingClass.display()

# COMMAND ----------

# MAGIC %md ## 2.3 AuditLog

# COMMAND ----------

# df_destinationMinusSource_EclipseAuditLog, df_sourceMinusDestination_EclipseAuditLog = fn_reconciliationRecords0500("Eclipse", "AuditLog", "MyMI_Pre_Eclipse", "dbo_AuditLog", "UpdateId")

# print(f"Destination - Source: {df_destinationMinusSource_EclipseAuditLog.count()}")
# print(f"Source - Destination: {df_sourceMinusDestination_EclipseAuditLog.count()}") 

# COMMAND ----------

# df_destinationMinusSource_EclipseAuditLog.display()

# COMMAND ----------

# df_sourceMinusDestination_EclipseAuditLog.display()

# COMMAND ----------

# MAGIC %md ## 2.4 ObjCode

# COMMAND ----------

df_destinationMinusSource_EclipseObjCode, df_sourceMinusDestination_EclipseObjCode = fn_reconciliationRecords0500("Eclipse", "ObjCode", "MyMI_Pre_Eclipse", "dbo_ObjCode", "ObjCodeId")

print(f"Destination - Source: {df_destinationMinusSource_EclipseObjCode.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipseObjCode.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipseObjCode.display()

# COMMAND ----------

df_sourceMinusDestination_EclipseObjCode.display()

# COMMAND ----------

# MAGIC %md ## 2.5 OrgValidOrgType

# COMMAND ----------

df_destinationMinusSource_EclipseOrgValidOrgType, df_sourceMinusDestination_EclipseOrgValidOrgType = fn_reconciliationRecords0500("Eclipse", "OrgValidOrgType", "MyMI_Pre_Eclipse", "dbo_OrgValidOrgType", "OrgOrgTypeId")

print(f"Destination - Source: {df_destinationMinusSource_EclipseOrgValidOrgType.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipseOrgValidOrgType.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipseOrgValidOrgType.display()

# COMMAND ----------

df_sourceMinusDestination_EclipseOrgValidOrgType.display()

# COMMAND ----------

# MAGIC %md ## 2.6 PolicyDeduction

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyDeduction, df_sourceMinusDestination_EclipsePolicyDeduction = fn_reconciliationRecords0500("Eclipse", "PolicyDeduction", "MyMI_Pre_Eclipse", "dbo_PolicyDeduction", "PolicyDeductionId")

print(f"Destination - Source: {df_destinationMinusSource_EclipsePolicyDeduction.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipsePolicyDeduction.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyDeduction.display()

# COMMAND ----------

df_sourceMinusDestination_EclipsePolicyDeduction.display()

# COMMAND ----------

# MAGIC %md ## 2.7 PolicyLink

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyLink, df_sourceMinusDestination_EclipsePolicyLink = fn_reconciliationRecords0500("Eclipse", "PolicyLink", "MyMI_Pre_Eclipse", "dbo_PolicyLink", "PolicyLinkId")

print(f"Destination - Source: {df_destinationMinusSource_EclipsePolicyLink.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipsePolicyLink.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyLink.display()

# COMMAND ----------

df_sourceMinusDestination_EclipsePolicyLink.display()

# COMMAND ----------

# MAGIC %md ## 2.8 PolicyLinkXref

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyLinkXref, df_sourceMinusDestination_EclipsePolicyLinkXref = fn_reconciliationRecords0500("Eclipse", "PolicyLinkXref", "MyMI_Pre_Eclipse", "dbo_PolicyLinkXref", "PolicyLinkXrefId")

print(f"Destination - Source: {df_destinationMinusSource_EclipsePolicyLinkXref.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipsePolicyLinkXref.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyLinkXref.display()

# COMMAND ----------

df_sourceMinusDestination_EclipsePolicyLinkXref.display()

# COMMAND ----------

# MAGIC %md ## 2.9 PolicyOrg

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyOrg, df_sourceMinusDestination_EclipsePolicyOrg = fn_reconciliationRecords0500("Eclipse", "PolicyOrg", "MyMI_Pre_Eclipse", "dbo_PolicyOrg", "PolicyOrgId")

print(f"Destination - Source: {df_destinationMinusSource_EclipsePolicyOrg.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipsePolicyOrg.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyOrg.display()

# COMMAND ----------

df_sourceMinusDestination_EclipsePolicyOrg.display()

# COMMAND ----------

# MAGIC %md ## 2.10 PolicyPRI

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyPRI, df_sourceMinusDestination_EclipsePolicyPRI = fn_reconciliationRecords0500("Eclipse", "PolicyPRI", "MyMI_Pre_Eclipse", "dbo_PolicyPRI", "PolicyPRIId")

print(f"Destination - Source: {df_destinationMinusSource_EclipsePolicyPRI.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipsePolicyPRI.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyPRI.display()

# COMMAND ----------

df_sourceMinusDestination_EclipsePolicyPRI.display()

# COMMAND ----------

# MAGIC %md ## 2.11 PolicyReinst

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyReinst, df_sourceMinusDestination_EclipsePolicyReinst = fn_reconciliationRecords0500("Eclipse", "PolicyReinst", "MyMI_Pre_Eclipse", "dbo_PolicyReinst", "PolicyReinstId")

print(f"Destination - Source: {df_destinationMinusSource_EclipsePolicyReinst.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_EclipsePolicyReinst.count()}") 

# COMMAND ----------

df_destinationMinusSource_EclipsePolicyReinst.display()

# COMMAND ----------

df_sourceMinusDestination_EclipsePolicyReinst.display()

# COMMAND ----------

# MAGIC %md # 2. GXLP

# COMMAND ----------

# MAGIC %md ## 2.1 CTR_CESS

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_CESS, df_sourceMinusDestination_GXLP_CTR_CESS = fn_reconciliationRecords0500("GXLP", "CTR_CESS", "MyMI_Pre_GXLP", "dbo_CTR_CESS", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_CESS.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_CESS.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_CESS.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_CESS.display()

# COMMAND ----------

# MAGIC %md ## 2.2 CTR_PCOMM

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_PCOMM, df_sourceMinusDestination_GXLP_CTR_PCOMM = fn_reconciliationRecords0500("GXLP", "CTR_PCOMM", "MyMI_Pre_GXLP", "dbo_CTR_PCOMM", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_PCOMM.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_PCOMM.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_PCOMM.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_PCOMM.display()

# COMMAND ----------

# MAGIC %md ## 2.3 Accrual

# COMMAND ----------

df_destinationMinusSource_GXLP_Accrual, df_sourceMinusDestination_GXLP_Accrual = fn_reconciliationRecords0500("GXLP", "Accrual", "MyMI_Pre_GXLP", "dbo_Accrual", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_Accrual.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_Accrual.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_Accrual.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_Accrual.display()

# COMMAND ----------

# MAGIC %md ## 2.4 CTR_BRK

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_BRK, df_sourceMinusDestination_GXLP_CTR_BRK = fn_reconciliationRecords0500("GXLP", "CTR_BRK", "MyMI_Pre_GXLP", "dbo_CTR_BRK", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_BRK.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_BRK.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_BRK.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_BRK.display()

# COMMAND ----------

# MAGIC %md ## 2.5 CTR_HDR

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_HDR, df_sourceMinusDestination_GXLP_CTR_HDR = fn_reconciliationRecords0500("GXLP", "CTR_HDR", "MyMI_Pre_GXLP", "dbo_CTR_HDR", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_HDR.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_HDR.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_HDR.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_HDR.display()

# COMMAND ----------

# MAGIC %md ## 2.6 CTR_HDRM

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_HDRM, df_sourceMinusDestination_GXLP_CTR_HDRM = fn_reconciliationRecords0500("GXLP", "CTR_HDRM", "MyMI_Pre_GXLP", "dbo_CTR_HDRM", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_HDRM.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_HDRM.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_HDRM.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_HDRM.display()

# COMMAND ----------

# MAGIC %md ## 2.7 CTR_HDRP

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_HDRP, df_sourceMinusDestination_GXLP_CTR_HDRP = fn_reconciliationRecords0500("GXLP", "CTR_HDRP", "MyMI_Pre_GXLP", "dbo_CTR_HDRP", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_HDRP.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_HDRP.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_HDRP.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_HDRP.display()

# COMMAND ----------

# MAGIC %md ## 2.8 ctr_hdrx

# COMMAND ----------

df_destinationMinusSource_GXLP_ctr_hdrx, df_sourceMinusDestination_GXLP_ctr_hdrx = fn_reconciliationRecords0500("GXLP", "ctr_hdrx", "MyMI_Pre_GXLP", "dbo_ctr_hdrx", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_ctr_hdrx.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_ctr_hdrx.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_ctr_hdrx.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_ctr_hdrx.display()

# COMMAND ----------

# MAGIC %md ## 2.9 CTR_PM_ADJ

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_PM_ADJ, df_sourceMinusDestination_GXLP_CTR_PM_ADJ = fn_reconciliationRecords0500("GXLP", "CTR_PM_ADJ", "MyMI_Pre_GXLP", "dbo_CTR_PM_ADJ", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_PM_ADJ.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_PM_ADJ.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_PM_ADJ.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_PM_ADJ.display()

# COMMAND ----------

# MAGIC %md ## 2.10 CTR_RST

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_RST, df_sourceMinusDestination_GXLP_CTR_RST = fn_reconciliationRecords0500("GXLP", "CTR_RST", "MyMI_Pre_GXLP", "dbo_CTR_RST", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_RST.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_RST.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_RST.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_RST.display()

# COMMAND ----------

# MAGIC %md ## 2.11 CTR_SEC

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_SEC, df_sourceMinusDestination_GXLP_CTR_SEC = fn_reconciliationRecords0500("GXLP", "CTR_SEC", "MyMI_Pre_GXLP", "dbo_CTR_SEC", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_CTR_SEC.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_CTR_SEC.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_CTR_SEC.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_CTR_SEC.display()

# COMMAND ----------

# MAGIC %md ## 2.12 PROG_ACC

# COMMAND ----------

df_destinationMinusSource_GXLP_PROG_ACC, df_sourceMinusDestination_GXLP_PROG_ACC = fn_reconciliationRecords0500("GXLP", "PROG_ACC", "MyMI_Pre_GXLP", "dbo_PROG_ACC", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_PROG_ACC.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_PROG_ACC.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_PROG_ACC.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_PROG_ACC.display()

# COMMAND ----------

# MAGIC %md ## 2.13 REINSURE

# COMMAND ----------

df_destinationMinusSource_GXLP_REINSURE, df_sourceMinusDestination_GXLP_REINSURE = fn_reconciliationRecords0500("GXLP", "REINSURE", "MyMI_Pre_GXLP", "dbo_REINSURE", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_REINSURE.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_REINSURE.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_REINSURE.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_REINSURE.display()

# COMMAND ----------

# MAGIC %md ## 2.14 Sub_Event

# COMMAND ----------

df_destinationMinusSource_GXLP_Sub_Event, df_sourceMinusDestination_GXLP_Sub_Event = fn_reconciliationRecords0500("GXLP", "Sub_Event", "MyMI_Pre_GXLP", "dbo_Sub_Event", "unique_key")

print(f"Destination - Source: {df_destinationMinusSource_GXLP_Sub_Event.count()}")
print(f"Source - Destination: {df_sourceMinusDestination_GXLP_Sub_Event.count()}") 

# COMMAND ----------

df_destinationMinusSource_GXLP_Sub_Event.display()

# COMMAND ----------

df_sourceMinusDestination_GXLP_Sub_Event.display()