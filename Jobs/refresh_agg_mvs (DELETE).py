# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh agg_* materialized views for the Forecasting Tool dashboard
# MAGIC Rebuilds the pre-aggregated MVs in `home_luca_bolognesi.forecasting_tool` that the
# MAGIC agg-optimized dashboard reads. Scheduled daily. Generated from repo agg/*.sql.

# COMMAND ----------

# MAGIC %md ### agg_lists

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_list_bu
AS
SELECT DISTINCT Business_Unit AS business_unit
FROM main.gtm_silver.individual_hierarchy_salesforce
WHERE Business_Unit IS NOT NULL""")
print("refreshed agg_list_bu")

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_region_hierarchy
AS
SELECT DISTINCT
  Business_Unit   AS business_unit,
  Region_Level_1  AS region_level_1,
  Region_Level_2  AS region_level_2,
  Region_Level_3  AS region_level_3
FROM main.gtm_silver.individual_hierarchy_salesforce
WHERE Business_Unit IS NOT NULL""")
print("refreshed agg_region_hierarchy")

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_quarters
AS
SELECT fiscal_quarter
FROM (
  VALUES
    ('FY26-Q1'), ('FY26-Q2'), ('FY26-Q3'), ('FY26-Q4'),
    ('FY27-Q1'), ('FY27-Q2'), ('FY27-Q3'), ('FY27-Q4'),
    ('FY28-Q1'), ('FY28-Q2'), ('FY28-Q3'), ('FY28-Q4')
) AS t(fiscal_quarter)""")
print("refreshed agg_quarters")

# COMMAND ----------

# MAGIC %md ### agg_hierarchy

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_sales_hierarchy
AS
SELECT DISTINCT
  Email               AS user_email,
  user_name,
  Business_Unit       AS business_unit,
  Region_Level_1      AS region_level_1,
  Region_Level_2      AS region_level_2,
  concatenated_emails            
FROM main.gtm_gold.rpt_individual_hierarchy_active_valid_sales_user_only""")
print("refreshed agg_sales_hierarchy")

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_field_hierarchy
AS
SELECT DISTINCT
  email               AS user_email,
  concatenated_emails
FROM main.gtm_silver.individual_hierarchy_field""")
print("refreshed agg_field_hierarchy")

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_sa_ae_map
AS
SELECT DISTINCT
  a.sa_only_concatenated_emails,          
  ti.Email          AS ae_email,
  ti.user_name      AS ae_user_name,
  ti.Business_Unit  AS business_unit,
  ti.Region_Level_1 AS region_level_1,
  ti.Region_Level_2 AS region_level_2
FROM main.gtm_gold.account_active_users_daily a
INNER JOIN main.gtm_silver.targets_individual ti
  ON ti.sfdc_user_id = a.account_executive_user_id
WHERE a.account_executive_user_id IS NOT NULL
  AND a.sa_only_concatenated_emails IS NOT NULL""")
print("refreshed agg_sa_ae_map")

# COMMAND ----------

# MAGIC %md ### agg_okr_product

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_okr_product
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2, fiscal_year_quarter)
AS
WITH base AS (
  SELECT
    a.horizontal_and_vertical_hierarchy_concatenated_emails,
    a.Business_Unit,
    a.sales_subregion_level_1,
    a.sales_subregion_level_2,
    a.deployable_account_name,
    a.fiscal_year,
    REPLACE(REPLACE(a.fiscal_year_quarter, '\'', ''), ' ', '-') AS fiscal_year_quarter,
    a.usage_date,
    CASE WHEN a.usage_date = (SELECT MAX(usage_date) FROM main.gtm_gold.account_consumption_daily)
         THEN 'Y' ELSE 'N' END AS latest_snapshot,
    ROUND(SUM(a.dbu_dollars_t7d_avg), 2)  AS dbu_dollars_t7d_avg,
    ROUND(SUM(a.dbu_dollars_t28d_avg), 2) AS dbu_dollars_t28d_avg,
    ROUND(SUM(a.lakebase_dbu_dollars_ytd), 2)            AS lakebase_dbu_dollars_ytd,
    ROUND(SUM(a.lakebase_dbu_dollars_t7d_avg), 2)        AS lakebase_dbu_dollars_t7d_avg,
    ROUND(SUM(a.lakebase_dbu_dollars_t7d_avg_prev), 2)   AS lakebase_dbu_dollars_t7d_prev,
    ROUND(SUM(a.lakebase_dbu_dollars_t28d_avg), 2)       AS lakebase_dbu_dollars_t28d_avg,
    ROUND(SUM(a.lakebase_dbu_dollars_t28d_avg_prev), 2)  AS lakebase_dbu_dollars_t28d_avg_prev,
    ROUND(SUM(a.lakebase_dbu_dollars_t28d_sum), 2)       AS lakebase_dbu_dollars_t28d_sum,
    ROUND(SUM(a.lakebase_dbu_dollars_t28d_sum_prev), 2)  AS lakebase_dbu_dollars_t28d_sum_prev,
    ROUND(SUM(a.ai_gateway_dbu_dollars_ytd), 2)          AS ai_gateway_dbu_dollars_ytd,
    ROUND(SUM(a.ai_gateway_dbu_dollars_t7d_avg), 2)      AS ai_gateway_dbu_dollars_t7d_avg,
    ROUND(SUM(a.ai_gateway_dbu_dollars_t7d_avg_prev), 2) AS ai_gateway_dbu_dollars_t7d_prev,
    ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_avg), 2)     AS ai_gateway_dbu_dollars_t28d_avg,
    ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_avg_prev), 2) AS ai_gateway_dbu_dollars_t28d_avg_prev,
    ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_sum), 2)     AS ai_gateway_dbu_dollars_t28d_sum,
    ROUND(SUM(a.ai_gateway_dbu_dollars_t28d_sum_prev), 2) AS ai_gateway_dbu_dollars_t28d_sum_prev,
    ROUND(SUM(a.dwh_dbu_dollars_ytd), 2)            AS dwh_dbu_dollars_ytd,
    ROUND(SUM(a.dwh_dbu_dollars_t7d_avg), 2)        AS dwh_dbu_dollars_t7d_avg,
    ROUND(SUM(a.dwh_dbu_dollars_t28d_avg), 2)       AS dwh_dbu_dollars_t28d_avg,
    ROUND(SUM(a.dwh_dbu_dollars_t7d_avg_prev), 2)   AS dwh_dbu_dollars_t7d_avg_prev,
    ROUND(SUM(a.dwh_dbu_dollars_t28d_avg_prev), 2)  AS dwh_dbu_dollars_t28d_avg_prev
  FROM main.gtm_gold.account_consumption_daily AS a
  WHERE a.dbu_dollars_t7d_avg > 0
    AND a.fiscal_year >= 2026
    AND (dayofweek(a.usage_date) = 6
         OR a.usage_date = (SELECT MAX(usage_date) FROM main.gtm_gold.account_consumption_daily))
  GROUP BY
    a.horizontal_and_vertical_hierarchy_concatenated_emails,
    a.Business_Unit, a.sales_subregion_level_1, a.sales_subregion_level_2,
    a.deployable_account_name, a.fiscal_year,
    REPLACE(REPLACE(a.fiscal_year_quarter, '\'', ''), ' ', '-'),
    a.usage_date
)
SELECT
  base.*,
  
  (lakebase_dbu_dollars_t28d_sum > 5000 AND lakebase_dbu_dollars_t28d_sum_prev > 5000)     AS lakebase_activated,
  (ai_gateway_dbu_dollars_t28d_sum > 5000 AND ai_gateway_dbu_dollars_t28d_sum_prev > 5000) AS ai_gateway_activated,
  
  ROUND(TRY_DIVIDE(lakebase_dbu_dollars_t7d_avg,  dbu_dollars_t7d_avg)  * 100, 4) AS lakebase_penetration_t7d_pct,
  ROUND(TRY_DIVIDE(lakebase_dbu_dollars_t28d_avg, dbu_dollars_t28d_avg) * 100, 4) AS lakebase_penetration_t28d_pct,
  ROUND(TRY_DIVIDE(ai_gateway_dbu_dollars_t7d_avg,  dbu_dollars_t7d_avg)  * 100, 4) AS ai_gateway_penetration_t7d_pct,
  ROUND(TRY_DIVIDE(ai_gateway_dbu_dollars_t28d_avg, dbu_dollars_t28d_avg) * 100, 4) AS ai_gateway_penetration_t28d_pct,
  ROUND(TRY_DIVIDE(dwh_dbu_dollars_t7d_avg,  dbu_dollars_t7d_avg)  * 100, 2) AS dwh_penetration_t7d_pct,
  ROUND(TRY_DIVIDE(dwh_dbu_dollars_t28d_avg, dbu_dollars_t28d_avg) * 100, 2) AS dwh_penetration_t28d_pct
FROM base""")
print("refreshed agg_okr_product")

# COMMAND ----------

# MAGIC %md ### agg_okr_product_pipe

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_okr_product_pipe
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
AS
WITH account_dims AS (
  
  SELECT DISTINCT
    deployable_account_name,
    account_id,
    horizontal_and_vertical_hierarchy_concatenated_emails,
    Business_Unit,
    sales_subregion_level_1,
    sales_subregion_level_2
  FROM main.gtm_gold.account_consumption_daily
  WHERE fiscal_year >= 2026
    AND dbu_dollars_t7d_avg > 0
),
product_pivot AS (
  SELECT
    apq.account_id,
    REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-') AS fiscal_year_quarter,
    CONCAT(CONCAT('20', SUBSTR(REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-'), 3, 2)),
           ' ', SPLIT(REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-'), '-')[1]) AS fiscal_quarter,
    SUM(CASE WHEN product = 'AI'            THEN COALESCE(weighted_projection,0) ELSE 0 END) AS ai_wp,
    SUM(CASE WHEN product = 'AI'            THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS ai_uc_count,
    SUM(CASE WHEN product = 'AI/BI'         THEN COALESCE(weighted_projection,0) ELSE 0 END) AS ai_bi_wp,
    SUM(CASE WHEN product = 'AI/BI'         THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS ai_bi_uc_count,
    SUM(CASE WHEN product = 'DWH'           THEN COALESCE(weighted_projection,0) ELSE 0 END) AS dwh_wp,
    SUM(CASE WHEN product = 'DWH'           THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS dwh_uc_count,
    SUM(CASE WHEN product = 'FMAPI Partner' THEN COALESCE(weighted_projection,0) ELSE 0 END) AS fmapi_partner_wp,
    SUM(CASE WHEN product = 'FMAPI Partner' THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS fmapi_partner_uc_count,
    SUM(CASE WHEN product = 'Lakebase'      THEN COALESCE(weighted_projection,0) ELSE 0 END) AS lakebase_wp,
    SUM(CASE WHEN product = 'Lakebase'      THEN COALESCE(use_case_pipe_count,0) ELSE 0 END) AS lakebase_uc_count
  FROM main.gtm_gold.account_product_quarterly apq
  WHERE CAST(CONCAT('20', SUBSTR(apq.fiscal_year_quarter, 4, 2)) AS INT) >= 2027
  GROUP BY apq.account_id,
    REPLACE(REPLACE(apq.fiscal_year_quarter, CHR(39), ''), ' ', '-')
)
SELECT
  d.horizontal_and_vertical_hierarchy_concatenated_emails,
  d.Business_Unit,
  d.sales_subregion_level_1,
  d.sales_subregion_level_2,
  d.deployable_account_name,
  p.fiscal_quarter,
  p.ai_wp, p.ai_uc_count,
  p.ai_bi_wp, p.ai_bi_uc_count,
  p.dwh_wp, p.dwh_uc_count,
  p.fmapi_partner_wp, p.fmapi_partner_uc_count,
  p.lakebase_wp, p.lakebase_uc_count,
  CAST(SPLIT(p.fiscal_quarter, ' ')[0] AS INT) = (
    CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN YEAR(CURRENT_DATE()) ELSE YEAR(CURRENT_DATE()) + 1 END
  ) AS is_current_fiscal
FROM account_dims d
LEFT JOIN product_pivot p ON d.account_id = p.account_id""")
print("refreshed agg_okr_product_pipe")

# COMMAND ----------

# MAGIC %md ### agg_okr_qoq_growth

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_okr_qoq_growth
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2, fiscal_year_quarter)
AS
WITH agg AS (
  SELECT
    horizontal_and_vertical_hierarchy_concatenated_emails,
    Business_Unit,
    sales_subregion_level_1,
    sales_subregion_level_2,
    fiscal_year,
    deployable_account_name,
    REPLACE(REPLACE(fiscal_year_quarter, '\'', ''), ' ', '-') AS fiscal_year_quarter,
    usage_date AS snapshot_date,
    ROUND(AVG(dbu_dollars_t28d_avg), 2) AS dbu_dollars_t28d_avg
  FROM main.gtm_gold.account_consumption_daily
  WHERE fiscal_year >= 2026
  GROUP BY
    horizontal_and_vertical_hierarchy_concatenated_emails,
    Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
    fiscal_year, deployable_account_name,
    REPLACE(REPLACE(fiscal_year_quarter, '\'', ''), ' ', '-'),
    usage_date
)
SELECT
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit,
  sales_subregion_level_1,
  sales_subregion_level_2,
  fiscal_year,
  deployable_account_name,
  fiscal_year_quarter,
  snapshot_date,
  dbu_dollars_t28d_avg
FROM agg
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY deployable_account_name, fiscal_year_quarter
  ORDER BY snapshot_date DESC) = 1""")
print("refreshed agg_okr_qoq_growth")

# COMMAND ----------

# MAGIC %md ### agg_okr_genie

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_okr_genie
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
AS
WITH account_dims AS (
  SELECT DISTINCT
    deployable_account_name,
    horizontal_and_vertical_hierarchy_concatenated_emails,
    Business_Unit,
    sales_subregion_level_1,
    sales_subregion_level_2
  FROM main.gtm_gold.account_consumption_daily
  WHERE YEAR(usage_date) + CASE WHEN MONTH(usage_date) >= 2 THEN 1 ELSE 0 END >= 2026
),
genie_kpis AS (
  SELECT
    g.date,
    h.deployable_account_name,
    SUM(g.T7D_Users)  AS genie_t7d_users,
    SUM(g.T28D_Users) AS genie_t28d_users
  FROM main.eng_datarooms.genie_daily_kpis g
  INNER JOIN main.fin_live_gold.sfdc_hierarchy_mapping h
    ON g.salesforce_account_name = h.sfdc_account_name
  WHERE YEAR(g.date) + CASE WHEN MONTH(g.date) >= 2 THEN 1 ELSE 0 END >= 2026
  GROUP BY g.date, h.deployable_account_name
),
genie_dbu AS (
  SELECT
    usage_date,
    deployable_account_name,
    SUM(genie_standalone_dbu_dollars)               AS genie_dbu_dollars,
    SUM(genie_standalone_dbu_dollars_t7d_sum)       AS genie_t7d_dbu_dollars,
    SUM(genie_standalone_dbu_dollars_t7d_sum_prev)  AS genie_t7d_dbu_dollars_prev,
    SUM(genie_standalone_dbu_dollars_t28d_sum)      AS genie_t28d_dbu_dollars,
    SUM(genie_standalone_dbu_dollars_t28d_sum_prev) AS genie_t28d_dbu_dollars_prev
  FROM main.gtm_gold.account_consumption_daily
  WHERE YEAR(usage_date) + CASE WHEN MONTH(usage_date) >= 2 THEN 1 ELSE 0 END >= 2026
  GROUP BY usage_date, deployable_account_name
),
fy_start AS (
  SELECT MIN(date) AS fy_first_date
  FROM main.eng_datarooms.genie_daily_kpis
  WHERE date >= MAKE_DATE(
    CASE WHEN MONTH((SELECT MAX(date) FROM main.eng_datarooms.genie_daily_kpis)) >= 2
         THEN YEAR((SELECT MAX(date) FROM main.eng_datarooms.genie_daily_kpis))
         ELSE YEAR((SELECT MAX(date) FROM main.eng_datarooms.genie_daily_kpis)) - 1 END, 2, 1)
),
fy_start_kpis AS (
  SELECT
    h.deployable_account_name,
    SUM(g.T28D_Users) AS fy_start_t28d_users
  FROM main.eng_datarooms.genie_daily_kpis g
  INNER JOIN main.fin_live_gold.sfdc_hierarchy_mapping h
    ON g.salesforce_account_name = h.sfdc_account_name
  CROSS JOIN fy_start fs
  WHERE g.date = fs.fy_first_date
  GROUP BY h.deployable_account_name
)
SELECT
  d.horizontal_and_vertical_hierarchy_concatenated_emails,
  d.Business_Unit,
  d.sales_subregion_level_1,
  d.sales_subregion_level_2,
  k.date,
  YEAR(k.date) + CASE WHEN MONTH(k.date) >= 2 THEN 1 ELSE 0 END AS fiscal_year,
  CONCAT('FY', RIGHT(CAST(YEAR(k.date) + CASE WHEN MONTH(k.date) >= 2 THEN 1 ELSE 0 END AS STRING), 2), '-',
    CASE WHEN MONTH(k.date) IN (2,3,4) THEN 'Q1'
         WHEN MONTH(k.date) IN (5,6,7) THEN 'Q2'
         WHEN MONTH(k.date) IN (8,9,10) THEN 'Q3'
         ELSE 'Q4' END) AS fiscal_quarter,
  CASE WHEN k.date = (SELECT MAX(date) FROM genie_kpis) THEN 'Y' ELSE 'N' END AS latest_snapshot,
  k.deployable_account_name,
  k.genie_t7d_users,
  k.genie_t28d_users,
  k.genie_t28d_users - f.fy_start_t28d_users AS t28d_users_ytd_diff,
  f.fy_start_t28d_users,
  d2.genie_dbu_dollars,
  d2.genie_t7d_dbu_dollars,
  d2.genie_t7d_dbu_dollars_prev,
  d2.genie_t28d_dbu_dollars,
  d2.genie_t28d_dbu_dollars_prev
FROM genie_kpis k
INNER JOIN account_dims d ON k.deployable_account_name = d.deployable_account_name
LEFT JOIN genie_dbu d2
  ON k.deployable_account_name = d2.deployable_account_name AND k.date = d2.usage_date
LEFT JOIN fy_start_kpis f
  ON k.deployable_account_name = f.deployable_account_name
WHERE (DAYOFWEEK(k.date) = 6 OR k.date = (SELECT MAX(date) FROM genie_kpis))""")
print("refreshed agg_okr_genie")

# COMMAND ----------

# MAGIC %md ### agg_okr_migrations

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_okr_migrations
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
AS
SELECT
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit,
  sales_subregion_level_1,
  sales_subregion_level_2,
  deployable_account_name,
  target_live_fiscal_year,
  REPLACE(REPLACE(target_live_fiscal_year_quarter, '\'', ''), ' ', '-') AS target_live_fiscal_year_quarter,
  ROUND(SUM(estimated_quarterly_dollar_dbus), 2) AS estimated_quarterly_dollar_dbus,
  ROUND(SUM(CASE WHEN is_migration_usecase THEN estimated_quarterly_dollar_dbus ELSE 0 END), 2) AS migration_pipeline,
  ROUND(SUM(estimated_quarterly_dollar_dbus_weighted), 2) AS total_weighted_pipeline,
  ROUND(SUM(CASE WHEN is_migration_usecase THEN estimated_quarterly_dollar_dbus_weighted ELSE 0 END), 2) AS migration_weighted_pipeline
FROM main.gtm_gold.rpt_use_case_detail
WHERE stage_number <= 6
  AND is_incremental = true
  AND YEAR(target_go_live_fiscal_year_end_date) >= 2026
GROUP BY
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
  deployable_account_name, target_live_fiscal_year,
  REPLACE(REPLACE(target_live_fiscal_year_quarter, '\'', ''), ' ', '-')""")
print("refreshed agg_okr_migrations")

# COMMAND ----------

# MAGIC %md ### agg_okr_velocity

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_okr_u3_velocity
AS
SELECT
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit,
  sales_subregion_level_1,
  sales_subregion_level_2,
  solution_architect_user_name AS solution_architect,
  date_format(confirming_date, 'yyyy-MM') AS confirming_date_month,
  date_format(add_months(confirming_date, 11), 'yyyy') AS confirming_date_fiscal,
  date_format(dateadd(year, +1, dateadd(month, -1, confirming_date)), "'FY'yy'-Q'Q") AS confirming_date_fq,
  SUM(CASE WHEN stage_number > 3 AND days_in_evaluating_all_time = 0 THEN 1 END) AS uco_count_eval_skipped,
  SUM(CASE WHEN stage_number > 3 AND days_in_evaluating > 0 AND days_in_evaluating <= 60 THEN 1 END) AS eval_lt_60_days_uco_count,
  SUM(CASE WHEN stage_number > 3 AND days_in_evaluating > 0 THEN days_in_evaluating END) AS past_eval_days_sum,
  NULLIF(COUNT(CASE WHEN stage_number > 3 AND days_in_evaluating > 0 THEN 1 END), 0) AS past_eval_total_uco_count
FROM main.gtm_gold.rpt_use_case_detail
WHERE date_format(add_months(confirming_date, 11), 'yyyy') >= 2026
  AND stage_number > 3
  AND solution_architect_user_name IS NOT NULL
GROUP BY
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
  solution_architect_user_name,
  date_format(confirming_date, 'yyyy-MM'),
  date_format(add_months(confirming_date, 11), 'yyyy'),
  date_format(dateadd(year, +1, dateadd(month, -1, confirming_date)), "'FY'yy'-Q'Q")""")
print("refreshed agg_okr_u3_velocity")

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_okr_u5_velocity
AS
SELECT
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit,
  sales_subregion_level_1,
  sales_subregion_level_2,
  solution_architect_user_name AS solution_architect,
  date_format(add_months(live_date, 11), 'yyyy') AS live_date_fiscal,
  date_format(dateadd(year, +1, dateadd(month, -1, live_date)), "'FY'yy'-Q'Q") AS live_date_fq,
  SUM(CASE WHEN stage_number = 6 AND days_in_confirming_all_time = 0 THEN 1 END) AS uco_count_confirm_skipped,
  SUM(CASE WHEN stage_number = 6 AND days_in_confirming > 0 AND days_in_confirming <= 60 THEN 1 END) AS confirm_lt_60_days_uco_count,
  SUM(CASE WHEN stage_number = 6 AND days_in_confirming > 0 THEN days_in_confirming END) AS past_confirm_days_sum,
  NULLIF(COUNT(CASE WHEN stage_number = 6 AND days_in_confirming > 0 THEN 1 END), 0) AS past_confirm_total_uco_count
FROM main.gtm_gold.rpt_use_case_detail
WHERE date_format(add_months(live_date, 11), 'yyyy') >= 2026
  AND stage_number = 6
  AND solution_architect_user_name IS NOT NULL
GROUP BY
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
  solution_architect_user_name,
  date_format(add_months(live_date, 11), 'yyyy'),
  date_format(dateadd(year, +1, dateadd(month, -1, live_date)), "'FY'yy'-Q'Q")""")
print("refreshed agg_okr_u5_velocity")

# COMMAND ----------

# MAGIC %md ### agg_blockers

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_blockers
CLUSTER BY (region_level_3)
AS
SELECT
  b.concatenated_emails,
  b.region_level_3,
  b.account_name,
  b.account_executive,
  b.usecase_id,
  CONCAT('<a href="https://databricks.lightning.force.com/lightning/r/UseCase__c/', b.usecase_id,
         '/view" target="_blank">', b.use_case_name, '</a>') AS usecase_url,
  u.estimated_monthly_dollar_dbus,
  b.blocker_id,
  b.blocker_name,
  b.category,
  b.type,
  CASE WHEN b.type = 'Blocked'  THEN u.estimated_monthly_dollar_dbus ELSE 0 END AS blocked_dbus,
  CASE WHEN b.type = 'Blocked'  THEN 1 ELSE 0 END AS blocked_count,
  CASE WHEN b.type = 'Friction' THEN u.estimated_monthly_dollar_dbus ELSE 0 END AS friction_dbus,
  CASE WHEN b.type = 'Friction' THEN 1 ELSE 0 END AS friction_count,
  b.comment,
  aha_item.aha_reference,
  CASE WHEN aha_item.aha_reference IS NOT NULL
       THEN '<a href="https://databrickinternal.ideas.aha.io/ideas/' || aha_item.aha_reference ||
            '" target="_blank">' || aha_item.aha_reference || '</a>' END AS aha_reference_url,
  aha_item.aha_name   AS aha_idea,
  aha_item.aha_status AS aha_status,
  aha_item.aha_link   AS aha_link
FROM main.gtm_silver.blocker_detail b
INNER JOIN main.gtm_silver.use_case_detail u
  ON b.usecase_id = u.usecase_id
LATERAL VIEW OUTER EXPLODE(b.aha) AS aha_item
WHERE b.snapshot_date = current_date()
  AND u.stage_number BETWEEN 1 AND 5""")
print("refreshed agg_blockers")

# COMMAND ----------

# MAGIC %md ### agg_ucos

# COMMAND ----------

spark.sql("""CREATE OR REFRESH MATERIALIZED VIEW home_luca_bolognesi.forecasting_tool.agg_ucos
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
AS
WITH monthly_projection_pivoted AS (
  SELECT usecase_id,
    COALESCE(m_2025_02,0) AS m_2025_02, COALESCE(m_2025_03,0) AS m_2025_03, COALESCE(m_2025_04,0) AS m_2025_04,
    COALESCE(m_2025_05,0) AS m_2025_05, COALESCE(m_2025_06,0) AS m_2025_06, COALESCE(m_2025_07,0) AS m_2025_07,
    COALESCE(m_2025_08,0) AS m_2025_08, COALESCE(m_2025_09,0) AS m_2025_09, COALESCE(m_2025_10,0) AS m_2025_10,
    COALESCE(m_2025_11,0) AS m_2025_11, COALESCE(m_2025_12,0) AS m_2025_12, COALESCE(m_2026_01,0) AS m_2026_01,
    COALESCE(m_2026_02,0) AS m_2026_02, COALESCE(m_2026_03,0) AS m_2026_03, COALESCE(m_2026_04,0) AS m_2026_04,
    COALESCE(m_2026_05,0) AS m_2026_05, COALESCE(m_2026_06,0) AS m_2026_06, COALESCE(m_2026_07,0) AS m_2026_07,
    COALESCE(m_2026_08,0) AS m_2026_08, COALESCE(m_2026_09,0) AS m_2026_09, COALESCE(m_2026_10,0) AS m_2026_10,
    COALESCE(m_2026_11,0) AS m_2026_11, COALESCE(m_2026_12,0) AS m_2026_12, COALESCE(m_2027_01,0) AS m_2027_01
  FROM (
    SELECT usecase_id, m, ramping_dbus FROM home_luca_bolognesi.forecasting_tool.monthly_projection_by_usecase
  )
  PIVOT (SUM(ramping_dbus) AS dbus FOR m IN (
    '2025-02-01' AS m_2025_02, '2025-03-01' AS m_2025_03, '2025-04-01' AS m_2025_04, '2025-05-01' AS m_2025_05,
    '2025-06-01' AS m_2025_06, '2025-07-01' AS m_2025_07, '2025-08-01' AS m_2025_08, '2025-09-01' AS m_2025_09,
    '2025-10-01' AS m_2025_10, '2025-11-01' AS m_2025_11, '2025-12-01' AS m_2025_12, '2026-01-01' AS m_2026_01,
    '2026-02-01' AS m_2026_02, '2026-03-01' AS m_2026_03, '2026-04-01' AS m_2026_04, '2026-05-01' AS m_2026_05,
    '2026-06-01' AS m_2026_06, '2026-07-01' AS m_2026_07, '2026-08-01' AS m_2026_08, '2026-09-01' AS m_2026_09,
    '2026-10-01' AS m_2026_10, '2026-11-01' AS m_2026_11, '2026-12-01' AS m_2026_12, '2027-01-01' AS m_2027_01
  ))
),
forecast_quarterly_projection_pivoted AS (
  SELECT usecase_id,
    COALESCE(FY26_Q1,0) FY26_Q1, COALESCE(FY26_Q2,0) FY26_Q2, COALESCE(FY26_Q3,0) FY26_Q3, COALESCE(FY26_Q4,0) FY26_Q4,
    COALESCE(FY27_Q1,0) FY27_Q1, COALESCE(FY27_Q2,0) FY27_Q2, COALESCE(FY27_Q3,0) FY27_Q3, COALESCE(FY27_Q4,0) FY27_Q4
  FROM (
    SELECT usecase_id, fq, SUM(quarterly_incremental_dbus) AS quarterly_incremental_dbus
    FROM home_luca_bolognesi.forecasting_tool.monthly_projection_by_usecase
    GROUP BY usecase_id, fq
  )
  PIVOT (SUM(quarterly_incremental_dbus) AS dbus FOR fq IN (
    'FY26-Q1' AS FY26_Q1, 'FY26-Q2' AS FY26_Q2, 'FY26-Q3' AS FY26_Q3, 'FY26-Q4' AS FY26_Q4,
    'FY27-Q1' AS FY27_Q1, 'FY27-Q2' AS FY27_Q2, 'FY27-Q3' AS FY27_Q3, 'FY27-Q4' AS FY27_Q4
  ))
),
usecases_filtered_dedup AS (
  
  SELECT *
  FROM home_luca_bolognesi.forecasting_tool.usecases_filtered
  QUALIFY ROW_NUMBER() OVER (PARTITION BY usecase_id, ae_email ORDER BY user_id) = 1
)
SELECT
  c.Business_Unit,
  b.ae_email,
  c.sales_subregion_level_1, c.sales_subregion_level_2, c.sales_subregion_level_3,
  c.account_name, c.deployable_account_name, c.account_executive, c.solution_architect,
  c.dsa_user_name AS dsa, c.arr_band, c.usecase_id, c.usecase_name,
  c.target_onboarding_date, b.target_onboarding_date_fq, c.target_live_date, b.target_live_date_fq,
  c.target_cloud, c.is_migration_usecase,
  NULLIF(TRIM(c.migration_source_platform), '') AS migration_source_platform_adj,
  c.is_incremental, b.is_keytechwin,
  c.stage, c.stage_number, c.stage_name_ui, c.days_in_stage, c.days_in_validating, c.days_in_scoping,
  c.days_in_evaluating, c.days_in_confirming, c.days_in_onboarding,
  c.usecase_description, c.demand_plan_next_steps, c.implementation_notes,
  c.implementation_partner_name, c.has_ps_project, c.use_case_product_enriched,
  b.estimated_monthly_dollar_dbus, c.estimated_monthly_dollar_dbus_weighted, b.total_ramping_days,
  c.estimated_quarterly_dollar_dbus, c.estimated_quarterly_dollar_dbus_weighted,
  b.days_in_stage_bucket, b.usecase_url, b.eval_doc_link, b.onboarding_doc_link, b.implementation_status,
  b.num_of_blockers, b.blocked_count, b.friction_count, b.blocker_details,
  b.stage_advanced, b.stage_change_description, b.target_date_pulled_foreward, b.target_date_change_description,
  b.target_live_date_diff_days, b.amount_increased, b.amount_change_description, b.change_amount,
  b.change_type_label, b.manager_notes,
  qp.FY26_Q1, qp.FY26_Q2, qp.FY26_Q3, qp.FY26_Q4, qp.FY27_Q1, qp.FY27_Q2, qp.FY27_Q3, qp.FY27_Q4,
  mp.m_2025_02, mp.m_2025_03, mp.m_2025_04, mp.m_2025_05, mp.m_2025_06, mp.m_2025_07, mp.m_2025_08,
  mp.m_2025_09, mp.m_2025_10, mp.m_2025_11, mp.m_2025_12,
  mp.m_2026_01, mp.m_2026_02, mp.m_2026_03, mp.m_2026_04, mp.m_2026_05, mp.m_2026_06, mp.m_2026_07,
  mp.m_2026_08, mp.m_2026_09, mp.m_2026_10, mp.m_2026_11, mp.m_2026_12, mp.m_2027_01,
  b.days_to_go_live, b.days_to_onboarding,
  b.hygiene_rules, b.has_hygiene_issues, b.slippage_risk, b.has_onboarding_slippage_risk,
  b.has_go_live_slippage_risk, b.onboarding_slippage_details, b.go_live_slippage_details,
  b.stage_advanced_count, b.stage_regressed_count, b.live_date_advanced_count, b.live_date_regressed_count,
  b.amount_grew_count, b.amount_shrank_count,
  asq.ASQ_Summary_HTML
FROM main.gtm_silver.use_case_detail AS c
INNER JOIN usecases_filtered_dedup AS b ON b.usecase_id = c.usecase_id
LEFT JOIN forecast_quarterly_projection_pivoted AS qp ON qp.usecase_id = c.usecase_id
LEFT JOIN monthly_projection_pivoted AS mp ON mp.usecase_id = c.usecase_id
LEFT JOIN home_luca_bolognesi.forecasting_tool.asq_summary AS asq ON asq.usecase_id = c.usecase_id""")
print("refreshed agg_ucos")

# COMMAND ----------
