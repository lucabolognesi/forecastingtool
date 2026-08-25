# Databricks notebook source
# DBTITLE 1,agg_okr_product
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_product
# MAGIC CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2, fiscal_year_quarter)
# MAGIC AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     a.horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC     a.Business_Unit,
# MAGIC     a.sales_subregion_level_1,
# MAGIC     a.sales_subregion_level_2,
# MAGIC     a.deployable_account_name,
# MAGIC     a.fiscal_year,
# MAGIC     REPLACE(REPLACE(a.fiscal_year_quarter, CHR(39), ''), ' ', '-') AS fiscal_year_quarter,
# MAGIC     a.usage_date,
# MAGIC     CASE WHEN a.usage_date = (SELECT MAX(usage_date) FROM main.gtm_gold.account_consumption_daily)
# MAGIC          THEN 'Y' ELSE 'N' END AS latest_snapshot,
# MAGIC     ROUND(SUM(a.dbu_dollars_t7d_avg), 2)  AS dbu_dollars_t7d_avg,
# MAGIC     ROUND(SUM(a.dbu_dollars_t28d_avg), 2) AS dbu_dollars_t28d_avg,
# MAGIC     ROUND(SUM(a.lakebase_dbu_dollars_ytd), 2)            AS lakebase_dbu_dollars_ytd,
# MAGIC     ROUND(SUM(a.lakebase_dbu_dollars_t7d_avg), 2)        AS lakebase_dbu_dollars_t7d_avg,
# MAGIC     ROUND(SUM(a.lakebase_dbu_dollars_t7d_avg_prev), 2)   AS lakebase_dbu_dollars_t7d_prev,
# MAGIC     ROUND(SUM(a.lakebase_dbu_dollars_t28d_avg), 2)       AS lakebase_dbu_dollars_t28d_avg,
# MAGIC     ROUND(SUM(a.lakebase_dbu_dollars_t28d_avg_prev), 2)  AS lakebase_dbu_dollars_t28d_avg_prev,
# MAGIC     ROUND(SUM(a.lakebase_dbu_dollars_t28d_sum), 2)       AS lakebase_dbu_dollars_t28d_sum,
# MAGIC     ROUND(SUM(a.lakebase_dbu_dollars_t28d_sum_prev), 2)  AS lakebase_dbu_dollars_t28d_sum_prev,
# MAGIC     ROUND(SUM(a.ai_gateway_dbu_dollars_ytd), 2)          AS ai_gateway_dbu_dollars_ytd,
# MAGIC     ROUND(SUM(a.ai_gateway_dbu_dollars_t7d_avg), 2)      AS ai_gateway_dbu_dollars_t7d_avg,
# MAGIC     ROUND(SUM(a.ai_gateway_dbu_dollars_t7d_avg_prev), 2) AS ai_gateway_dbu_dollars_t7d_prev,
# MAGIC     ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_avg), 2)     AS ai_gateway_dbu_dollars_t28d_avg,
# MAGIC     ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_avg_prev), 2) AS ai_gateway_dbu_dollars_t28d_avg_prev,
# MAGIC     ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_sum), 2)     AS ai_gateway_dbu_dollars_t28d_sum,
# MAGIC     ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_sum_prev), 2) AS ai_gateway_dbu_dollars_t28d_sum_prev,
# MAGIC     ROUND(SUM(a.dwh_dbu_dollars_ytd), 2)            AS dwh_dbu_dollars_ytd,
# MAGIC     ROUND(SUM(a.dwh_dbu_dollars_t7d_avg), 2)        AS dwh_dbu_dollars_t7d_avg,
# MAGIC     ROUND(SUM(a.dwh_dbu_dollars_t28d_avg), 2)       AS dwh_dbu_dollars_t28d_avg,
# MAGIC     ROUND(SUM(a.dwh_dbu_dollars_t7d_avg_prev), 2)   AS dwh_dbu_dollars_t7d_avg_prev,
# MAGIC     ROUND(SUM(a.dwh_dbu_dollars_t28d_avg_prev), 2)  AS dwh_dbu_dollars_t28d_avg_prev
# MAGIC   FROM main.gtm_gold.account_consumption_daily AS a
# MAGIC   WHERE a.dbu_dollars_t7d_avg > 0
# MAGIC     AND a.fiscal_year >= 2026
# MAGIC     AND a.Business_Unit = '${business_unit}'
# MAGIC     AND a.sales_subregion_level_1 = '${region_level_1}'
# MAGIC     AND (dayofweek(a.usage_date) = 6
# MAGIC          OR a.usage_date = (SELECT MAX(usage_date) FROM main.gtm_gold.account_consumption_daily))
# MAGIC   GROUP BY
# MAGIC     a.horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC     a.Business_Unit, a.sales_subregion_level_1, a.sales_subregion_level_2,
# MAGIC     a.deployable_account_name, a.fiscal_year,
# MAGIC     REPLACE(REPLACE(a.fiscal_year_quarter, CHR(39), ''), ' ', '-'),
# MAGIC     a.usage_date
# MAGIC )
# MAGIC SELECT
# MAGIC   base.*,
# MAGIC   (lakebase_dbu_dollars_t28d_sum > 5000 AND lakebase_dbu_dollars_t28d_sum_prev > 5000)     AS lakebase_activated,
# MAGIC   (ai_gateway_dbu_dollars_t28d_sum > 5000 AND ai_gateway_dbu_dollars_t28d_sum_prev > 5000) AS ai_gateway_activated,
# MAGIC   ROUND(TRY_DIVIDE(lakebase_dbu_dollars_t7d_avg,  dbu_dollars_t7d_avg)  * 100, 4) AS lakebase_penetration_t7d_pct,
# MAGIC   ROUND(TRY_DIVIDE(lakebase_dbu_dollars_t28d_avg, dbu_dollars_t28d_avg) * 100, 4) AS lakebase_penetration_t28d_pct,
# MAGIC   ROUND(TRY_DIVIDE(ai_gateway_dbu_dollars_t7d_avg,  dbu_dollars_t7d_avg)  * 100, 4) AS ai_gateway_penetration_t7d_pct,
# MAGIC   ROUND(TRY_DIVIDE(ai_gateway_dbu_dollars_t28d_avg, dbu_dollars_t28d_avg) * 100, 4) AS ai_gateway_penetration_t28d_pct,
# MAGIC   ROUND(TRY_DIVIDE(dwh_dbu_dollars_t7d_avg,  dbu_dollars_t7d_avg)  * 100, 2) AS dwh_penetration_t7d_pct,
# MAGIC   ROUND(TRY_DIVIDE(dwh_dbu_dollars_t28d_avg, dbu_dollars_t28d_avg) * 100, 2) AS dwh_penetration_t28d_pct
# MAGIC FROM base

# COMMAND ----------

# DBTITLE 1,agg_okr_product_pipe
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_product_pipe
# MAGIC CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
# MAGIC AS
# MAGIC WITH account_dims AS (
# MAGIC   SELECT DISTINCT
# MAGIC     deployable_account_name,
# MAGIC     account_id,
# MAGIC     horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC     Business_Unit,
# MAGIC     sales_subregion_level_1,
# MAGIC     sales_subregion_level_2
# MAGIC   FROM main.gtm_gold.account_consumption_daily
# MAGIC   WHERE fiscal_year >= 2026
# MAGIC     AND dbu_dollars_t7d_avg > 0
# MAGIC     AND Business_Unit = '${business_unit}'
# MAGIC     AND sales_subregion_level_1 = '${region_level_1}'
# MAGIC ),
# MAGIC product_pivot AS (
# MAGIC   SELECT
# MAGIC     apq.account_id,
# MAGIC     REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-') AS fiscal_year_quarter,
# MAGIC     CONCAT(CONCAT('20', SUBSTR(REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-'), 3, 2)),
# MAGIC            ' ', SPLIT(REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-'), '-')[1]) AS fiscal_quarter,
# MAGIC     SUM(CASE WHEN product = 'AI'            THEN COALESCE(weighted_projection,0) ELSE 0 END) AS ai_wp,
# MAGIC     SUM(CASE WHEN product = 'AI'            THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS ai_uc_count,
# MAGIC     SUM(CASE WHEN product = 'AI/BI'         THEN COALESCE(weighted_projection,0) ELSE 0 END) AS ai_bi_wp,
# MAGIC     SUM(CASE WHEN product = 'AI/BI'         THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS ai_bi_uc_count,
# MAGIC     SUM(CASE WHEN product = 'DWH'           THEN COALESCE(weighted_projection,0) ELSE 0 END) AS dwh_wp,
# MAGIC     SUM(CASE WHEN product = 'DWH'           THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS dwh_uc_count,
# MAGIC     SUM(CASE WHEN product = 'FMAPI Partner' THEN COALESCE(weighted_projection,0) ELSE 0 END) AS fmapi_partner_wp,
# MAGIC     SUM(CASE WHEN product = 'FMAPI Partner' THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS fmapi_partner_uc_count,
# MAGIC     SUM(CASE WHEN product = 'Lakebase'      THEN COALESCE(weighted_projection,0) ELSE 0 END) AS lakebase_wp,
# MAGIC     SUM(CASE WHEN product = 'Lakebase'      THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS lakebase_uc_count
# MAGIC   FROM main.gtm_gold.account_product_quarterly apq
# MAGIC   WHERE CAST(CONCAT('20', SUBSTR(apq.fiscal_year_quarter, 4, 2)) AS INT) >= 2027
# MAGIC   GROUP BY apq.account_id,
# MAGIC     REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-')
# MAGIC )
# MAGIC SELECT
# MAGIC   d.horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   d.Business_Unit,
# MAGIC   d.sales_subregion_level_1,
# MAGIC   d.sales_subregion_level_2,
# MAGIC   d.deployable_account_name,
# MAGIC   p.fiscal_quarter,
# MAGIC   p.ai_wp, p.ai_uc_count,
# MAGIC   p.ai_bi_wp, p.ai_bi_uc_count,
# MAGIC   p.dwh_wp, p.dwh_uc_count,
# MAGIC   p.fmapi_partner_wp, p.fmapi_partner_uc_count,
# MAGIC   p.lakebase_wp, p.lakebase_uc_count,
# MAGIC   CAST(SPLIT(p.fiscal_quarter, ' ')[0] AS INT) = (
# MAGIC     CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN YEAR(CURRENT_DATE()) ELSE YEAR(CURRENT_DATE()) + 1 END
# MAGIC   ) AS is_current_fiscal
# MAGIC FROM account_dims d
# MAGIC LEFT JOIN product_pivot p ON d.account_id = p.account_id

# COMMAND ----------

# DBTITLE 1,agg_okr_qoq_growth
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_qoq_growth
# MAGIC CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2, fiscal_year_quarter)
# MAGIC AS
# MAGIC WITH agg AS (
# MAGIC   SELECT
# MAGIC     horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC     Business_Unit,
# MAGIC     sales_subregion_level_1,
# MAGIC     sales_subregion_level_2,
# MAGIC     fiscal_year,
# MAGIC     deployable_account_name,
# MAGIC     REPLACE(REPLACE(fiscal_year_quarter, CHR(39), ''), ' ', '-') AS fiscal_year_quarter,
# MAGIC     usage_date AS snapshot_date,
# MAGIC     ROUND(AVG(dbu_dollars_t28d_avg), 2) AS dbu_dollars_t28d_avg
# MAGIC   FROM main.gtm_gold.account_consumption_daily
# MAGIC   WHERE fiscal_year >= 2026
# MAGIC     AND Business_Unit = '${business_unit}'
# MAGIC     AND sales_subregion_level_1 = '${region_level_1}'
# MAGIC   GROUP BY
# MAGIC     horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC     Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
# MAGIC     fiscal_year, deployable_account_name,
# MAGIC     REPLACE(REPLACE(fiscal_year_quarter, CHR(39), ''), ' ', '-'),
# MAGIC     usage_date
# MAGIC )
# MAGIC SELECT
# MAGIC   horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   Business_Unit,
# MAGIC   sales_subregion_level_1,
# MAGIC   sales_subregion_level_2,
# MAGIC   fiscal_year,
# MAGIC   deployable_account_name,
# MAGIC   fiscal_year_quarter,
# MAGIC   snapshot_date,
# MAGIC   dbu_dollars_t28d_avg
# MAGIC FROM agg
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY deployable_account_name, fiscal_year_quarter
# MAGIC   ORDER BY snapshot_date DESC) = 1

# COMMAND ----------

# DBTITLE 1,agg_okr_genie
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_genie
# MAGIC CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
# MAGIC AS
# MAGIC WITH account_mapping AS (
# MAGIC   -- Still needed: maps sfdcAccountName → deployable_account_name.
# MAGIC   -- No existing pipeline MV provides this mapping.
# MAGIC   SELECT DISTINCT
# MAGIC     sfdc_account_name,
# MAGIC     deployable_account_name
# MAGIC   FROM main.fin_live_gold.sfdc_hierarchy_mapping
# MAGIC ),
# MAGIC account_dims AS (
# MAGIC   -- Still needed: maps deployable_account_name → hierarchy columns.
# MAGIC   -- No existing pipeline MV exposes concatenated_emails at account level.
# MAGIC   SELECT DISTINCT
# MAGIC     deployable_account_name,
# MAGIC     horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC     Business_Unit,
# MAGIC     sales_subregion_level_1,
# MAGIC     sales_subregion_level_2
# MAGIC   FROM main.gtm_gold.account_consumption_daily
# MAGIC   WHERE usage_date >= (SELECT MIN(m) FROM dates)
# MAGIC     AND Business_Unit = '${business_unit}'
# MAGIC     AND sales_subregion_level_1 = '${region_level_1}'
# MAGIC ),
# MAGIC genie_metrics AS (
# MAGIC   SELECT
# MAGIC     g.date,
# MAGIC     h.deployable_account_name,
# MAGIC     SUM(COALESCE(g.dashboard_users_t7d_account_level, 0)
# MAGIC       + COALESCE(g.genie_users_t7d_account_level, 0))       AS genie_t7d_users,
# MAGIC     SUM(COALESCE(g.dashboard_users_t28d_account_level, 0)
# MAGIC       + COALESCE(g.genie_users_t28d_account_level, 0))      AS genie_t28d_users,
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t1d, 0)
# MAGIC       + COALESCE(g.genie_dollars_t1d, 0)), 2)               AS genie_dbu_dollars,
# MAGIC     -- Daily averages (sum divided by window length)
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t7d, 0)
# MAGIC       + COALESCE(g.genie_dollars_t7d, 0)) / 7, 2)           AS genie_t7d_dbu_dollars,
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t7d_lag28, 0)
# MAGIC       + COALESCE(g.genie_dollars_t7d_lag28, 0)) / 7, 2)     AS genie_t7d_dbu_dollars_prev,
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t28d, 0)
# MAGIC       + COALESCE(g.genie_dollars_t28d, 0)) / 28, 2)         AS genie_t28d_dbu_dollars,
# MAGIC     ROUND(SUM(COALESCE(g.dashboard_dollars_t28d_lag28, 0)
# MAGIC       + COALESCE(g.genie_dollars_t28d_lag28, 0)) / 28, 2)   AS genie_t28d_dbu_dollars_prev
# MAGIC   FROM main.field_emea_product_usage.gold_genie_account_daily g
# MAGIC   INNER JOIN account_mapping h
# MAGIC     ON g.sfdcAccountName = h.sfdc_account_name
# MAGIC   WHERE g.date >= (SELECT MIN(m) FROM dates)
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
# MAGIC   dt.fy AS fiscal_year,
# MAGIC   dt.fq AS fiscal_quarter,
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
# MAGIC INNER JOIN dates dt
# MAGIC   ON date_trunc('month', m.date) = dt.m
# MAGIC LEFT JOIN fy_start_kpis f
# MAGIC   ON m.deployable_account_name = f.deployable_account_name
# MAGIC WHERE (DAYOFWEEK(m.date) = 6
# MAGIC        OR m.date = (SELECT MAX(date) FROM main.field_emea_product_usage.gold_genie_account_daily))

# COMMAND ----------

# DBTITLE 1,agg_okr_migrations
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_migrations
# MAGIC CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
# MAGIC AS
# MAGIC SELECT
# MAGIC   horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   Business_Unit,
# MAGIC   sales_subregion_level_1,
# MAGIC   sales_subregion_level_2,
# MAGIC   deployable_account_name,
# MAGIC   target_live_fiscal_year,
# MAGIC   REPLACE(REPLACE(target_live_fiscal_year_quarter, CHR(39), ''), ' ', '-') AS target_live_fiscal_year_quarter,
# MAGIC   ROUND(SUM(estimated_quarterly_dollar_dbus), 2) AS estimated_quarterly_dollar_dbus,
# MAGIC   ROUND(SUM(CASE WHEN is_migration_usecase THEN estimated_quarterly_dollar_dbus ELSE 0 END), 2) AS migration_pipeline,
# MAGIC   ROUND(SUM(estimated_quarterly_dollar_dbus_weighted), 2) AS total_weighted_pipeline,
# MAGIC   ROUND(SUM(CASE WHEN is_migration_usecase THEN estimated_quarterly_dollar_dbus_weighted ELSE 0 END), 2) AS migration_weighted_pipeline
# MAGIC FROM main.gtm_gold.rpt_use_case_detail
# MAGIC WHERE stage_number <= 6
# MAGIC   AND is_incremental = true
# MAGIC   AND YEAR(target_go_live_fiscal_year_end_date) >= 2026
# MAGIC   AND Business_Unit = '${business_unit}'
# MAGIC   AND sales_subregion_level_1 = '${region_level_1}'
# MAGIC GROUP BY
# MAGIC   horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
# MAGIC   deployable_account_name, target_live_fiscal_year,
# MAGIC   REPLACE(REPLACE(target_live_fiscal_year_quarter, CHR(39), ''), ' ', '-')

# COMMAND ----------

# DBTITLE 1,agg_okr_u3_velocity
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_u3_velocity
# MAGIC AS
# MAGIC SELECT
# MAGIC   horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   Business_Unit,
# MAGIC   sales_subregion_level_1,
# MAGIC   sales_subregion_level_2,
# MAGIC   solution_architect_user_name AS solution_architect,
# MAGIC   date_format(confirming_date, 'yyyy-MM') AS confirming_date_month,
# MAGIC   date_format(add_months(confirming_date, 11), 'yyyy') AS confirming_date_fiscal,
# MAGIC   date_format(dateadd(year, +1, dateadd(month, -1, confirming_date)), "'FY'yy'-Q'Q") AS confirming_date_fq,
# MAGIC   SUM(CASE WHEN stage_number > 3 AND days_in_evaluating_all_time = 0 THEN 1 END) AS uco_count_eval_skipped,
# MAGIC   SUM(CASE WHEN stage_number > 3 AND days_in_evaluating > 0 AND days_in_evaluating <= 60 THEN 1 END) AS eval_lt_60_days_uco_count,
# MAGIC   SUM(CASE WHEN stage_number > 3 AND days_in_evaluating > 0 THEN days_in_evaluating END) AS past_eval_days_sum,
# MAGIC   NULLIF(COUNT(CASE WHEN stage_number > 3 AND days_in_evaluating > 0 THEN 1 END), 0) AS past_eval_total_uco_count
# MAGIC FROM main.gtm_gold.rpt_use_case_detail
# MAGIC WHERE date_format(add_months(confirming_date, 11), 'yyyy') >= 2026
# MAGIC   AND stage_number > 3
# MAGIC   AND solution_architect_user_name IS NOT NULL
# MAGIC   AND Business_Unit = '${business_unit}'
# MAGIC   AND sales_subregion_level_1 = '${region_level_1}'
# MAGIC GROUP BY
# MAGIC   horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
# MAGIC   solution_architect_user_name,
# MAGIC   date_format(confirming_date, 'yyyy-MM'),
# MAGIC   date_format(add_months(confirming_date, 11), 'yyyy'),
# MAGIC   date_format(dateadd(year, +1, dateadd(month, -1, confirming_date)), "'FY'yy'-Q'Q")

# COMMAND ----------

# DBTITLE 1,agg_okr_u5_velocity
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_okr_u5_velocity
# MAGIC AS
# MAGIC SELECT
# MAGIC   horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   Business_Unit,
# MAGIC   sales_subregion_level_1,
# MAGIC   sales_subregion_level_2,
# MAGIC   solution_architect_user_name AS solution_architect,
# MAGIC   date_format(add_months(live_date, 11), 'yyyy') AS live_date_fiscal,
# MAGIC   date_format(dateadd(year, +1, dateadd(month, -1, live_date)), "'FY'yy'-Q'Q") AS live_date_fq,
# MAGIC   SUM(CASE WHEN stage_number = 6 AND days_in_live_all_time = 0 THEN 1 END) AS uco_count_live_skipped,
# MAGIC   SUM(CASE WHEN stage_number = 6 AND days_in_live > 0 AND days_in_live <= 60 THEN 1 END) AS live_lt_60_days_uco_count,
# MAGIC   SUM(CASE WHEN stage_number = 6 AND days_in_live > 0 THEN days_in_live END) AS past_live_days_sum,
# MAGIC   NULLIF(COUNT(CASE WHEN stage_number = 6 AND days_in_live > 0 THEN 1 END), 0) AS past_live_total_uco_count
# MAGIC FROM main.gtm_gold.rpt_use_case_detail
# MAGIC WHERE date_format(add_months(live_date, 11), 'yyyy') >= 2026
# MAGIC   AND stage_number = 6
# MAGIC   AND solution_architect_user_name IS NOT NULL
# MAGIC   AND Business_Unit = '${business_unit}'
# MAGIC   AND sales_subregion_level_1 = '${region_level_1}'
# MAGIC GROUP BY
# MAGIC   horizontal_and_vertical_hierarchy_concatenated_emails,
# MAGIC   Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
# MAGIC   solution_architect_user_name,
# MAGIC   date_format(add_months(live_date, 11), 'yyyy'),
# MAGIC   date_format(dateadd(year, +1, dateadd(month, -1, live_date)), "'FY'yy'-Q'Q")

# COMMAND ----------

# DBTITLE 1,agg_blockers
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_blockers
# MAGIC CLUSTER BY (region_level_3)
# MAGIC AS
# MAGIC SELECT
# MAGIC   b.concatenated_emails,
# MAGIC   b.region_level_3,
# MAGIC   b.account_name,
# MAGIC   b.account_executive,
# MAGIC   b.usecase_id,
# MAGIC   CONCAT('<a href="https://databricks.lightning.force.com/lightning/r/UseCase__c/', b.usecase_id,
# MAGIC          '/view" target="_blank">', b.use_case_name, '</a>') AS usecase_url,
# MAGIC   u.estimated_monthly_dollar_dbus,
# MAGIC   b.blocker_id,
# MAGIC   b.blocker_name,
# MAGIC   b.category,
# MAGIC   b.type,
# MAGIC   CASE WHEN b.type = 'Blocked'  THEN u.estimated_monthly_dollar_dbus ELSE 0 END AS blocked_dbus,
# MAGIC   CASE WHEN b.type = 'Blocked'  THEN 1 ELSE 0 END AS blocked_count,
# MAGIC   CASE WHEN b.type = 'Friction' THEN u.estimated_monthly_dollar_dbus ELSE 0 END AS friction_dbus,
# MAGIC   CASE WHEN b.type = 'Friction' THEN 1 ELSE 0 END AS friction_count,
# MAGIC   b.comment,
# MAGIC   aha_item.aha_reference,
# MAGIC   CASE WHEN aha_item.aha_reference IS NOT NULL
# MAGIC        THEN '<a href="https://databrickinternal.ideas.aha.io/ideas/' || aha_item.aha_reference ||
# MAGIC             '" target="_blank">' || aha_item.aha_reference || '</a>' END AS aha_reference_url,
# MAGIC   aha_item.aha_name   AS aha_idea,
# MAGIC   aha_item.aha_status AS aha_status,
# MAGIC   aha_item.aha_link   AS aha_link
# MAGIC FROM main.gtm_silver.blocker_detail b
# MAGIC INNER JOIN main.gtm_silver.use_case_detail u
# MAGIC   ON b.usecase_id = u.usecase_id
# MAGIC LATERAL VIEW OUTER EXPLODE(b.aha) AS aha_item
# MAGIC WHERE b.snapshot_date = current_date()
# MAGIC   AND u.stage_number BETWEEN 1 AND 5
# MAGIC   AND b.business_unit = '${business_unit}'
# MAGIC   AND b.region_level_1 = '${region_level_1}'