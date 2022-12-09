# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM  MyMI_Abst_GXLP.Stg10_ORIGXLPPeriodForMyMI
# MAGIC ORDER BY GXLPPERIOD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT GXLPPeriod, ROW_NUMBER() OVER (ORDER BY GXLPPeriod ) AS PeriodOrder, MyMIProcessPeriod, FinancialPeriodEndDate, IsMDMClosedPeriod
# MAGIC FROM (
# MAGIC   SELECT mdm.GXLPPeriod, mdm.MyMIProcessPeriod, mdm.FinancialPeriodEndDate, true AS IsMDMClosedPeriod
# MAGIC   FROM MyMI_Pre_MDS.mdm_financialperiod mdm
# MAGIC   WHERE IsNotNull(mdm.GXLPPeriod)
# MAGIC   UNION 
# MAGIC   SELECT min(OpenGXLPPeriod.GXLPPeriod) AS GXLPPeriod
# MAGIC     , min(OpenMDMPeriod.MyMIProcessPeriod) AS MyMIProcessPeriod
# MAGIC     , min(OpenMDMPeriod.FinancialPeriodEndDate) AS FinancialPeriodEndDate
# MAGIC     , false AS IsMDMClosedPeriod
# MAGIC   FROM (
# MAGIC     SELECT a.GXLPPeriod
# MAGIC     FROM (
# MAGIC       SELECT Min(gxlp.Period) AS GXLPPeriod
# MAGIC       FROM  MyMI_Pre_GXLP.dbo_ACCRUAL gxlp
# MAGIC       WHERE gxlp.Period > (
# MAGIC         SELECT max(mdm.GXLPPeriod) as LastMDMClosedPeriod
# MAGIC         FROM MyMI_Pre_MDS.mdm_financialperiod mdm
# MAGIC         WHERE IsNotNull(mdm.GXLPPeriod)
# MAGIC         )
# MAGIC       ) a
# MAGIC   ) OpenGXLPPeriod
# MAGIC CROSS JOIN MyMI_Pre_MDS.mdm_financialperiod OpenMDMPeriod
# MAGIC WHERE OpenMDMPeriod.MyMIProcessPeriod>(SELECT MAX(a.MyMIProcessPeriod) FROM MyMI_Pre_MDS.mdm_financialperiod a WHERE IsNotNull(a.GXLPPeriod))
# MAGIC ) Periods
# MAGIC WHERE GXLPPeriod IS not null

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT Min(gxlp.Period) AS GXLPPeriod
# MAGIC       FROM  MyMI_Pre_GXLP.dbo_ACCRUAL gxlp
# MAGIC       WHERE gxlp.Period > (
# MAGIC         SELECT max(mdm.GXLPPeriod) as LastMDMClosedPeriod
# MAGIC         FROM MyMI_Pre_MDS.mdm_financialperiod mdm
# MAGIC         WHERE IsNotNull(mdm.GXLPPeriod)
# MAGIC         )

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_GXLPPeriod
# MAGIC AS
# MAGIC SELECT GXLPPeriod, ROW_NUMBER() OVER (ORDER BY GXLPPeriod ) AS PeriodOrder, MyMIProcessPeriod, FinancialPeriodEndDate, IsMDMClosedPeriod
# MAGIC FROM (
# MAGIC   SELECT mdm.GXLPPeriod, mdm.MyMIProcessPeriod, mdm.FinancialPeriodEndDate, true AS IsMDMClosedPeriod
# MAGIC   FROM MyMI_Pre_MDS.mdm_financialperiod mdm
# MAGIC   WHERE IsNotNull(mdm.GXLPPeriod)
# MAGIC   UNION 
# MAGIC   SELECT min(OpenGXLPPeriod.GXLPPeriod) AS GXLPPeriod
# MAGIC     , min(OpenMDMPeriod.MyMIProcessPeriod) AS MyMIProcessPeriod
# MAGIC     , min(OpenMDMPeriod.FinancialPeriodEndDate) AS FinancialPeriodEndDate
# MAGIC     , false AS IsMDMClosedPeriod
# MAGIC   FROM (
# MAGIC     SELECT a.GXLPPeriod
# MAGIC     FROM (
# MAGIC       SELECT Min(gxlp.Period) AS GXLPPeriod
# MAGIC       FROM  MyMI_Pre_GXLP.dbo_ACCRUAL gxlp
# MAGIC       WHERE gxlp.Period > (
# MAGIC         SELECT max(mdm.GXLPPeriod) as LastMDMClosedPeriod
# MAGIC         FROM MyMI_Pre_MDS.mdm_financialperiod mdm
# MAGIC         WHERE IsNotNull(mdm.GXLPPeriod)
# MAGIC         )
# MAGIC       ) a
# MAGIC   ) OpenGXLPPeriod
# MAGIC CROSS JOIN MyMI_Pre_MDS.mdm_financialperiod OpenMDMPeriod
# MAGIC WHERE OpenMDMPeriod.MyMIProcessPeriod>(SELECT MAX(a.MyMIProcessPeriod) FROM MyMI_Pre_MDS.mdm_financialperiod a WHERE IsNotNull(a.GXLPPeriod))
# MAGIC ) Periods WHERE GXLPPeriod IS not null --Revert the change after GXLP2.10 upgrade/transition is complete

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_GXLPPeriodFlags
# MAGIC AS
# MAGIC SELECT p.GXLPPeriod, p.PeriodOrder, p.MyMIProcessPeriod, p.FinancialPeriodEndDate, p.IsMDMClosedPeriod
# MAGIC   , CASE WHEN p.GXLPPeriod = (SELECT MAX(c.GXLPPeriod) FROM vw_GXLPPeriod c WHERE c.IsMDMClosedPeriod) THEN true ELSE false END AS IsLastMDMClosedPeriod
# MAGIC   , CASE WHEN p.MyMIProcessPeriod = (SELECT MAX(c.First_DL_DWH_DimProcessPeriod_ID) FROM MyMI_Curated_Group.DWH_FactORITransaction c WHERE c.TransformedSubject="MyMI_Trans_GXLPClosed") THEN true ELSE false END AS IsLastDataLakeClosedPeriod
# MAGIC   , CASE WHEN p.PeriodOrder = (SELECT MAX(c.PeriodOrder) FROM vw_GXLPPeriod c) THEN true ELSE false END AS IsMDMCurrentPeriod
# MAGIC FROM vw_GXLPPeriod p

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ORIGXLPPeriodPrev
# MAGIC AS
# MAGIC SELECT thisperiod.GXLPPeriod, thisperiod.PeriodOrder, prevPeriod.GXLPPeriod AS PrevGXLPPeriod, thisperiod.MyMIProcessPeriod, thisperiod.FinancialPeriodEndDate, thisperiod.IsMDMClosedPeriod, thisperiod.IsLastMDMClosedPeriod, thisperiod.IsLastDataLakeClosedPeriod, thisperiod.IsMDMCurrentPeriod
# MAGIC FROM vw_GXLPPeriodFlags thisperiod
# MAGIC LEFT JOIN vw_GXLPPeriodFlags prevperiod
# MAGIC ON thisperiod.PeriodOrder - 1 = prevperiod.PeriodOrder

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_ORIGXLPPeriod
# MAGIC AS
# MAGIC SELECT GXLPPeriod, PeriodOrder, PrevGXLPPeriod, MyMIProcessPeriod, FinancialPeriodEndDate, IsMDMClosedPeriod, IsLastMDMClosedPeriod, IsLastDataLakeClosedPeriod, IsMDMCurrentPeriod
# MAGIC   , CASE 
# MAGIC       WHEN NOT MonthEndCompleted AND IsLastMDMClosedPeriod THEN true
# MAGIC       WHEN MonthEndCompleted AND IsMDMCurrentPeriod THEN true
# MAGIC       ELSE false
# MAGIC     END AS IsCurrentPeriodForORITrans
# MAGIC   , CASE 
# MAGIC       WHEN IsLastMDMClosedPeriod AND NOT IsLastDataLakeClosedPeriod THEN true
# MAGIC       ELSE false
# MAGIC     END AS IsClosingPeriod
# MAGIC   
# MAGIC FROM (
# MAGIC   SELECT a.GXLPPeriod, a.PeriodOrder, a.PrevGXLPPeriod, a.MyMIProcessPeriod, a.FinancialPeriodEndDate, a.IsMDMClosedPeriod, a.IsLastMDMClosedPeriod, a.IsLastDataLakeClosedPeriod, a.IsMDMCurrentPeriod, a.IsLastMDMClosedPeriod, c.MonthEndCompleted
# MAGIC   FROM vw_ORIGXLPPeriodPrev a
# MAGIC   CROSS JOIN (
# MAGIC     SELECT true AS MonthEndCompleted FROM vw_ORIGXLPPeriodPrev b WHERE IsLastMDMClosedPeriod AND IsLastDataLakeClosedPeriod
# MAGIC     UNION
# MAGIC     SELECT false AS MonthEndCompleted FROM vw_ORIGXLPPeriodPrev b WHERE IsLastMDMClosedPeriod AND NOT IsLastDataLakeClosedPeriod
# MAGIC   ) c
# MAGIC ) withcompletedstatus

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_ORIGXLPPeriod
