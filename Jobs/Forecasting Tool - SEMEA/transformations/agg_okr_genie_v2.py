# Databricks notebook source
# DBTITLE 1,agg_okr_genie_v2
# MAGIC %md
# MAGIC # agg_okr_genie_v2
# MAGIC
# MAGIC Redesigned version of `agg_okr_genie` sourcing Genie metrics from `main.field_emea_product_usage.gold_genie_account_daily` (the [Product Adoption - AI-BI dashboard](https://adb-2548836972759138.18.azuredatabricks.net/sql/dashboardsv3/01f1320165661fe492792a67802e6fbc) source).
# MAGIC
# MAGIC Key metric definitions from "Measure Drill Level" filter:
# MAGIC - **Genie $ (28d)** = `dashboard_dollars_t28d + genie_dollars_t28d`
# MAGIC - **Genie MAU (28d)** = `dashboard_users_t28d_account_level + genie_users_t28d_account_level`

# COMMAND ----------

# DBTITLE 1,agg_okr_genie
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_genie
# MAGIC CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
# MAGIC AS
# MAGIC WITH account_dims AS (
# MAGIC   -- Hierarchy columns from account_consumption_daily (same as original)
# MAGIC   SELECT DISTINCT
# MAGIC     deployable_account_name,
# MAGIC     horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC     Business_Unit,
# MAGIC     sales_subregion_level_1,
# MAGIC     sales_subregion_level_2
# MAGIC   FROM main.gtm_gold.account_consumption_daily
# MAGIC   WHERE YEAR(usage_date) + CASE WHEN MONTH(usage_date) >= 2 THEN 1 ELSE 0 END >= 2026
# MAGIC     AND Business_Unit = '${business_unit}'
# MAGIC     AND sales_subregion_level_1 = '${region_level_1}'
# MAGIC ),
# MAGIC -- Map sfdcAccountName → deployable_account_name
# MAGIC account_mapping AS (
# MAGIC   SELECT DISTINCT
# MAGIC     sfdc_account_name,
# MAGIC     deployable_account_name
# MAGIC   FROM main.fin_live_gold.sfdc_hierarchy_mapping
# MAGIC ),
# MAGIC -- New metrics from gold_genie_account_daily (AI-BI dashboard source)
# MAGIC -- "Genie $ (28d)"   = dashboard_dollars_t28d   + genie_dollars_t28d
# MAGIC -- "Genie MAU (28d)" = dashboard_users_t28d_account_level + genie_users_t28d_account_level
# MAGIC genie_metrics AS (
# MAGIC   SELECT
# MAGIC     g.date,
# MAGIC     h.deployable_account_name,
# MAGIC     -- Genie MAU (7d): Dashboard + Genie Agents users
# MAGIC     SUM(COALESCE(g.dashboard_users_t7d_account_level, 0)
# MAGIC       + COALESCE(g.genie_users_t7d_account_level, 0))       AS genie_t7d_users,
# MAGIC     -- Genie MAU (28d): Dashboard + Genie Agents users
# MAGIC     SUM(COALESCE(g.dashboard_users_t28d_account_level, 0)
# MAGIC       + COALESCE(g.genie_users_t28d_account_level, 0))      AS genie_t28d_users,
# MAGIC     -- Genie $ (daily)
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t1d, 0)
# MAGIC       + COALESCE(g.genie_dollars_t1d, 0)), 2)               AS genie_dbu_dollars,
# MAGIC     -- Genie $ (7d) daily average
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t7d, 0)
# MAGIC       + COALESCE(g.genie_dollars_t7d, 0)) / 7, 2)           AS genie_t7d_dbu_dollars,
# MAGIC     -- Genie $ (7d) prev (lag 28d) daily average
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t7d_lag28, 0)
# MAGIC       + COALESCE(g.genie_dollars_t7d_lag28, 0)) / 7, 2)     AS genie_t7d_dbu_dollars_prev,
# MAGIC     -- Genie $ (28d) daily average
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t28d, 0)
# MAGIC       + COALESCE(g.genie_dollars_t28d, 0)) / 28, 2)         AS genie_t28d_dbu_dollars,
# MAGIC     -- Genie $ (28d) prev (lag 28d) daily average
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t28d_lag28, 0)
# MAGIC       + COALESCE(g.genie_dollars_t28d_lag28, 0)) / 28, 2)   AS genie_t28d_dbu_dollars_prev
# MAGIC   FROM main.field_emea_product_usage.gold_genie_account_daily g
# MAGIC   INNER JOIN account_mapping h
# MAGIC     ON g.sfdcAccountName = h.sfdc_account_name
# MAGIC   WHERE YEAR(g.date) + CASE WHEN MONTH(g.date) >= 2 THEN 1 ELSE 0 END >= 2026
# MAGIC     AND g.sfdcAccountName IS NOT NULL
# MAGIC   GROUP BY g.date, h.deployable_account_name
# MAGIC ),
# MAGIC fy_start AS (
# MAGIC   SELECT MIN(date) AS fy_first_date
# MAGIC   FROM main.field_emea_product_usage.gold_genie_account_daily
# MAGIC   WHERE date >= MAKE_DATE(
# MAGIC     CASE WHEN MONTH((SELECT MAX(date) FROM main.field_emea_product_usage.gold_genie_account_daily)) >= 2
# MAGIC          THEN YEAR((SELECT MAX(date) FROM main.field_emea_product_usage.gold_genie_account_daily))
# MAGIC          ELSE YEAR((SELECT MAX(date) FROM main.field_emea_product_usage.gold_genie_account_daily)) - 1 END, 2, 1)
# MAGIC ),
# MAGIC fy_start_kpis AS (
# MAGIC   SELECT
# MAGIC     h.deployable_account_name,
# MAGIC     SUM(COALESCE(g.dashboard_users_t28d_account_level, 0)
# MAGIC       + COALESCE(g.genie_users_t28d_account_level, 0)) AS fy_start_t28d_users
# MAGIC   FROM main.field_emea_product_usage.gold_genie_account_daily g
# MAGIC   INNER JOIN account_mapping h
# MAGIC     ON g.sfdcAccountName = h.sfdc_account_name
# MAGIC   CROSS JOIN fy_start fs
# MAGIC   WHERE g.date = fs.fy_first_date
# MAGIC     AND g.sfdcAccountName IS NOT NULL
# MAGIC   GROUP BY h.deployable_account_name
# MAGIC )
# MAGIC SELECT
# MAGIC   d.horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   d.Business_Unit,
# MAGIC   d.sales_subregion_level_1,
# MAGIC   d.sales_subregion_level_2,
# MAGIC   m.date,
# MAGIC   YEAR(m.date) + CASE WHEN MONTH(m.date) >= 2 THEN 1 ELSE 0 END AS fiscal_year,
# MAGIC   CONCAT('FY', RIGHT(CAST(YEAR(m.date) + CASE WHEN MONTH(m.date) >= 2 THEN 1 ELSE 0 END AS STRING), 2), '-',
# MAGIC     CASE WHEN MONTH(m.date) IN (2,3,4) THEN 'Q1'
# MAGIC          WHEN MONTH(m.date) IN (5,6,7) THEN 'Q2'
# MAGIC          WHEN MONTH(m.date) IN (8,9,10) THEN 'Q3'
# MAGIC          ELSE 'Q4' END) AS fiscal_quarter,
# MAGIC   CASE WHEN m.date = (SELECT MAX(date) FROM main.field_emea_product_usage.gold_genie_account_daily)
# MAGIC        THEN 'Y' ELSE 'N' END AS latest_snapshot,
# MAGIC   m.deployable_account_name,
# MAGIC   m.genie_t7d_users,
# MAGIC   m.genie_t28d_users,
# MAGIC   m.genie_t28d_users - COALESCE(f.fy_start_t28d_users, 0) AS t28d_users_ytd_diff,
# MAGIC   COALESCE(f.fy_start_t28d_users, 0) AS fy_start_t28d_users,
# MAGIC   m.genie_dbu_dollars,
# MAGIC   m.genie_t7d_dbu_dollars,
# MAGIC   m.genie_t7d_dbu_dollars_prev,
# MAGIC   m.genie_t28d_dbu_dollars,
# MAGIC   m.genie_t28d_dbu_dollars_prev
# MAGIC FROM genie_metrics m
# MAGIC INNER JOIN account_dims d
# MAGIC   ON m.deployable_account_name = d.deployable_account_name
# MAGIC LEFT JOIN fy_start_kpis f
# MAGIC   ON m.deployable_account_name = f.deployable_account_name
# MAGIC WHERE (DAYOFWEEK(m.date) = 6
# MAGIC        OR m.date = (SELECT MAX(date) FROM main.field_emea_product_usage.gold_genie_account_daily))