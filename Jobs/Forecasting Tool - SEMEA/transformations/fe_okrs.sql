-- Databricks notebook source
-- DBTITLE 1,acd_base
-- Shared, SEMEA-scoped, column-pruned base over account_consumption_daily so that
-- agg_okr_product / _product_pipe / _qoq_growth / _genie read one scoped copy instead
-- of each re-scanning + re-filtering the full gold table. Raw passthrough (no aggregation
-- or dedup) => downstream GROUP BY / DISTINCT / SUM are unchanged.
CREATE OR REFRESH MATERIALIZED VIEW acd_base AS
SELECT
  account_id, horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
  deployable_account_name, fiscal_year, fiscal_year_quarter, usage_date,
  dbu_dollars_t7d_avg, dbu_dollars_t28d_avg,
  lakebase_dbu_dollars_ytd, lakebase_dbu_dollars_t7d_avg, lakebase_dbu_dollars_t7d_avg_prev,
  lakebase_dbu_dollars_t28d_avg, lakebase_dbu_dollars_t28d_avg_prev,
  lakebase_dbu_dollars_t28d_sum, lakebase_dbu_dollars_t28d_sum_prev,
  ai_gateway_dbu_dollars_ytd, ai_gateway_dbu_dollars_t7d_avg, ai_gateway_dbu_dollars_t7d_avg_prev,
  ai_gateway_dbu_dollars_t28d_avg, ai_gateway_dbu_dollars_t28d_avg_prev,
  ai_gateway_dbu_dollars_t28d_sum, ai_gateway_dbu_dollars_t28d_sum_prev,
  dwh_dbu_dollars_ytd, dwh_dbu_dollars_t7d_avg, dwh_dbu_dollars_t28d_avg,
  dwh_dbu_dollars_t7d_avg_prev, dwh_dbu_dollars_t28d_avg_prev
FROM main.gtm_gold.account_consumption_daily
WHERE Business_Unit = '${business_unit}'
  AND sales_subregion_level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,agg_okr_product
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_product
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2, fiscal_year_quarter)
AS
WITH snap AS (
  SELECT MAX(usage_date) AS max_usage_date FROM main.gtm_gold.account_consumption_daily
),
base AS (
  SELECT
    a.horizontal_and_vertical_hierarchy_concatenated_emails,
    a.Business_Unit,
    a.sales_subregion_level_1,
    a.sales_subregion_level_2,
    a.deployable_account_name,
    a.fiscal_year,
    REPLACE(REPLACE(a.fiscal_year_quarter, CHR(39), ''), ' ', '-') AS fiscal_year_quarter,
    a.usage_date,
    CASE WHEN a.usage_date = snap.max_usage_date
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
  FROM acd_base AS a
  CROSS JOIN snap
  WHERE a.dbu_dollars_t7d_avg > 0
    AND a.fiscal_year >= 2026
    AND a.Business_Unit = '${business_unit}'
    AND a.sales_subregion_level_1 = '${region_level_1}'
    AND (dayofweek(a.usage_date) = 6
         OR a.usage_date = snap.max_usage_date)
  GROUP BY
    a.horizontal_and_vertical_hierarchy_concatenated_emails,
    a.Business_Unit, a.sales_subregion_level_1, a.sales_subregion_level_2,
    a.deployable_account_name, a.fiscal_year,
    REPLACE(REPLACE(a.fiscal_year_quarter, CHR(39), ''), ' ', '-'),
    a.usage_date,
    snap.max_usage_date
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
FROM base

-- COMMAND ----------

-- DBTITLE 1,agg_okr_product_pipe
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_product_pipe
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
  FROM acd_base
  WHERE fiscal_year >= 2026
    AND dbu_dollars_t7d_avg > 0
    AND Business_Unit = '${business_unit}'
    AND sales_subregion_level_1 = '${region_level_1}'
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
LEFT JOIN product_pivot p ON d.account_id = p.account_id

-- COMMAND ----------

-- DBTITLE 1,agg_okr_qoq_growth
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_qoq_growth
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
    REPLACE(REPLACE(fiscal_year_quarter, CHR(39), ''), ' ', '-') AS fiscal_year_quarter,
    usage_date AS snapshot_date,
    ROUND(AVG(dbu_dollars_t28d_avg), 2) AS dbu_dollars_t28d_avg
  FROM acd_base
  WHERE fiscal_year >= 2026
    AND Business_Unit = '${business_unit}'
    AND sales_subregion_level_1 = '${region_level_1}'
  GROUP BY
    horizontal_and_vertical_hierarchy_concatenated_emails,
    Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
    fiscal_year, deployable_account_name,
    REPLACE(REPLACE(fiscal_year_quarter, CHR(39), ''), ' ', '-'),
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
  ORDER BY snapshot_date DESC) = 1

-- COMMAND ----------

-- DBTITLE 1,agg_okr_genie
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_genie
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
AS
WITH account_mapping AS (
  -- Still needed: maps sfdcAccountName → deployable_account_name.
  -- No existing pipeline MV provides this mapping.
  SELECT DISTINCT
    sfdc_account_name,
    deployable_account_name
  FROM main.fin_live_gold.sfdc_hierarchy_mapping
),
account_dims AS (
  -- Still needed: maps deployable_account_name → hierarchy columns.
  -- No existing pipeline MV exposes concatenated_emails at account level.
  SELECT DISTINCT
    deployable_account_name,
    horizontal_and_vertical_hierarchy_concatenated_emails,
    Business_Unit,
    sales_subregion_level_1,
    sales_subregion_level_2
  FROM acd_base
  WHERE usage_date >= (SELECT MIN(m) FROM dates)
    AND Business_Unit = '${business_unit}'
    AND sales_subregion_level_1 = '${region_level_1}'
),
genie_metrics AS (
  SELECT
    g.date,
    h.deployable_account_name,
    SUM(COALESCE(g.dashboard_users_t7d_account_level, 0)
      + COALESCE(g.genie_users_t7d_account_level, 0))       AS genie_t7d_users,
    SUM(COALESCE(g.dashboard_users_t28d_account_level, 0)
      + COALESCE(g.genie_users_t28d_account_level, 0))      AS genie_t28d_users,
    ROUND(SUM(COALESCE(g.dashboard_dollars_t1d, 0)
      + COALESCE(g.genie_dollars_t1d, 0)), 2)               AS genie_dbu_dollars,
    -- Daily averages (sum divided by window length)
    ROUND(SUM(COALESCE(g.dashboard_dollars_t7d, 0)
      + COALESCE(g.genie_dollars_t7d, 0)) / 7, 2)           AS genie_t7d_dbu_dollars,
    ROUND(SUM(COALESCE(g.dashboard_dollars_t7d_lag28, 0)
      + COALESCE(g.genie_dollars_t7d_lag28, 0)) / 7, 2)     AS genie_t7d_dbu_dollars_prev,
    ROUND(SUM(COALESCE(g.dashboard_dollars_t28d, 0)
      + COALESCE(g.genie_dollars_t28d, 0)) / 28, 2)         AS genie_t28d_dbu_dollars,
    ROUND(SUM(COALESCE(g.dashboard_dollars_t28d_lag28, 0)
      + COALESCE(g.genie_dollars_t28d_lag28, 0)) / 28, 2)   AS genie_t28d_dbu_dollars_prev
  FROM main.field_emea_product_usage.gold_genie_account_daily g
  INNER JOIN account_mapping h
    ON g.sfdcAccountName = h.sfdc_account_name
  WHERE g.date >= (SELECT MIN(m) FROM dates)
    AND g.sfdcAccountName IS NOT NULL
  GROUP BY g.date, h.deployable_account_name
),
genie_max AS (
  SELECT MAX(date) AS max_date FROM main.field_emea_product_usage.gold_genie_account_daily
),
fy_start AS (
  SELECT MIN(date) AS fy_first_date
  FROM main.field_emea_product_usage.gold_genie_account_daily
  CROSS JOIN genie_max gm
  WHERE date >= MAKE_DATE(
    CASE WHEN MONTH(gm.max_date) >= 2
         THEN YEAR(gm.max_date)
         ELSE YEAR(gm.max_date) - 1 END, 2, 1)
),
fy_start_kpis AS (
  SELECT
    h.deployable_account_name,
    SUM(COALESCE(g.dashboard_users_t28d_account_level, 0)
      + COALESCE(g.genie_users_t28d_account_level, 0)) AS fy_start_t28d_users
  FROM main.field_emea_product_usage.gold_genie_account_daily g
  INNER JOIN account_mapping h
    ON g.sfdcAccountName = h.sfdc_account_name
  CROSS JOIN fy_start fs
  WHERE g.date = fs.fy_first_date
    AND g.sfdcAccountName IS NOT NULL
  GROUP BY h.deployable_account_name
)
SELECT
  d.horizontal_and_vertical_hierarchy_concatenated_emails,
  d.Business_Unit,
  d.sales_subregion_level_1,
  d.sales_subregion_level_2,
  m.date,
  dt.fy AS fiscal_year,
  dt.fq AS fiscal_quarter,
  CASE WHEN m.date = gm.max_date
       THEN 'Y' ELSE 'N' END AS latest_snapshot,
  m.deployable_account_name,
  m.genie_t7d_users,
  m.genie_t28d_users,
  m.genie_t28d_users - COALESCE(f.fy_start_t28d_users, 0) AS t28d_users_ytd_diff,
  COALESCE(f.fy_start_t28d_users, 0) AS fy_start_t28d_users,
  m.genie_dbu_dollars,
  m.genie_t7d_dbu_dollars,
  m.genie_t7d_dbu_dollars_prev,
  m.genie_t28d_dbu_dollars,
  m.genie_t28d_dbu_dollars_prev
FROM genie_metrics m
CROSS JOIN genie_max gm
INNER JOIN account_dims d
  ON m.deployable_account_name = d.deployable_account_name
INNER JOIN dates dt
  ON date_trunc('month', m.date) = dt.m
LEFT JOIN fy_start_kpis f
  ON m.deployable_account_name = f.deployable_account_name
WHERE (DAYOFWEEK(m.date) = 6
       OR m.date = gm.max_date)

-- COMMAND ----------

-- DBTITLE 1,agg_okr_migrations
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_migrations
CLUSTER BY (Business_Unit, sales_subregion_level_1, sales_subregion_level_2)
AS
SELECT
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit,
  sales_subregion_level_1,
  sales_subregion_level_2,
  deployable_account_name,
  target_live_fiscal_year,
  REPLACE(REPLACE(target_live_fiscal_year_quarter, CHR(39), ''), ' ', '-') AS target_live_fiscal_year_quarter,
  ROUND(SUM(estimated_quarterly_dollar_dbus), 2) AS estimated_quarterly_dollar_dbus,
  ROUND(SUM(CASE WHEN is_migration_usecase THEN estimated_quarterly_dollar_dbus ELSE 0 END), 2) AS migration_pipeline,
  ROUND(SUM(estimated_quarterly_dollar_dbus_weighted), 2) AS total_weighted_pipeline,
  ROUND(SUM(CASE WHEN is_migration_usecase THEN estimated_quarterly_dollar_dbus_weighted ELSE 0 END), 2) AS migration_weighted_pipeline
FROM main.gtm_gold.rpt_use_case_detail
WHERE stage_number <= 6
  AND is_incremental = true
  AND YEAR(target_go_live_fiscal_year_end_date) >= 2026
  AND Business_Unit = '${business_unit}'
  AND sales_subregion_level_1 = '${region_level_1}'
GROUP BY
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
  deployable_account_name, target_live_fiscal_year,
  REPLACE(REPLACE(target_live_fiscal_year_quarter, CHR(39), ''), ' ', '-')

-- COMMAND ----------

-- DBTITLE 1,agg_okr_u3_velocity
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_u3_velocity
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
  AND Business_Unit = '${business_unit}'
  AND sales_subregion_level_1 = '${region_level_1}'
GROUP BY
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
  solution_architect_user_name,
  date_format(confirming_date, 'yyyy-MM'),
  date_format(add_months(confirming_date, 11), 'yyyy'),
  date_format(dateadd(year, +1, dateadd(month, -1, confirming_date)), "'FY'yy'-Q'Q")

-- COMMAND ----------

-- DBTITLE 1,agg_okr_u5_velocity
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_u5_velocity
AS
SELECT
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit,
  sales_subregion_level_1,
  sales_subregion_level_2,
  solution_architect_user_name AS solution_architect,
  date_format(add_months(live_date, 11), 'yyyy') AS live_date_fiscal,
  date_format(dateadd(year, +1, dateadd(month, -1, live_date)), "'FY'yy'-Q'Q") AS live_date_fq,
  SUM(CASE WHEN stage_number = 6 AND days_in_live_all_time = 0 THEN 1 END) AS uco_count_live_skipped,
  SUM(CASE WHEN stage_number = 6 AND days_in_live > 0 AND days_in_live <= 60 THEN 1 END) AS live_lt_60_days_uco_count,
  SUM(CASE WHEN stage_number = 6 AND days_in_live > 0 THEN days_in_live END) AS past_live_days_sum,
  NULLIF(COUNT(CASE WHEN stage_number = 6 AND days_in_live > 0 THEN 1 END), 0) AS past_live_total_uco_count
FROM main.gtm_gold.rpt_use_case_detail
WHERE date_format(add_months(live_date, 11), 'yyyy') >= 2026
  AND stage_number = 6
  AND solution_architect_user_name IS NOT NULL
  AND Business_Unit = '${business_unit}'
  AND sales_subregion_level_1 = '${region_level_1}'
GROUP BY
  horizontal_and_vertical_hierarchy_concatenated_emails,
  Business_Unit, sales_subregion_level_1, sales_subregion_level_2,
  solution_architect_user_name,
  date_format(add_months(live_date, 11), 'yyyy'),
  date_format(dateadd(year, +1, dateadd(month, -1, live_date)), "'FY'yy'-Q'Q")

-- COMMAND ----------

-- DBTITLE 1,agg_blockers
CREATE OR REFRESH MATERIALIZED VIEW agg_blockers
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
  AND u.stage_number BETWEEN 1 AND 5
  AND b.business_unit = '${business_unit}'
  AND b.region_level_1 = '${region_level_1}'