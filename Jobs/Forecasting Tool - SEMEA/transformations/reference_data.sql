-- Databricks notebook source
-- DBTITLE 1,forecast_dates
CREATE OR REFRESH MATERIALIZED VIEW dates AS
select cast(m as date) as m, fq, fy, q
  , last_day(m) as last_day_of_month, year(m) as cy, month(m) as cm
  from (
    values 
      ('2025-02-01', 'FY26-Q1', 2026, 1),
      ('2025-03-01', 'FY26-Q1', 2026, 1),
      ('2025-04-01', 'FY26-Q1', 2026, 1),
      ('2025-05-01', 'FY26-Q2', 2026, 2),
      ('2025-06-01', 'FY26-Q2', 2026, 2),
      ('2025-07-01', 'FY26-Q2', 2026, 2),
      ('2025-08-01', 'FY26-Q3', 2026, 3),
      ('2025-09-01', 'FY26-Q3', 2026, 3),
      ('2025-10-01', 'FY26-Q3', 2026, 3),
      ('2025-11-01', 'FY26-Q4', 2026, 4),
      ('2025-12-01', 'FY26-Q4', 2026, 4),
      ('2026-01-01', 'FY26-Q4', 2026, 4),
      ('2026-02-01', 'FY27-Q1', 2027, 1),
      ('2026-03-01', 'FY27-Q1', 2027, 1),
      ('2026-04-01', 'FY27-Q1', 2027, 1),
      ('2026-05-01', 'FY27-Q2', 2027, 2),
      ('2026-06-01', 'FY27-Q2', 2027, 2),
      ('2026-07-01', 'FY27-Q2', 2027, 2),
      ('2026-08-01', 'FY27-Q3', 2027, 3),
      ('2026-09-01', 'FY27-Q3', 2027, 3),
      ('2026-10-01', 'FY27-Q3', 2027, 3),
      ('2026-11-01', 'FY27-Q4', 2027, 4),
      ('2026-12-01', 'FY27-Q4', 2027, 4),
      ('2027-01-01', 'FY27-Q4', 2027, 4) 
  ) as dates(m, fq, fy, q)

-- COMMAND ----------

-- DBTITLE 1,forecast_snapshot_date
CREATE OR REFRESH MATERIALIZED VIEW snapshot_date AS
select
  (select max(usage_date) from main.gtm_gold.individual_consumption_daily) as latest_usage_date

-- COMMAND ----------

-- DBTITLE 1,forecast_ae_list
CREATE OR REFRESH MATERIALIZED VIEW ae_list AS
-- Resolve :ae_email to a list of AE emails.
-- If :ae_email is an AE, returns just that email. If a manager, returns all AEs reporting to them.
select 
  user_id,
  Email as ae_email, 
  user_name, 
  IsAE,
  case 
    when level = 7 then 'AE' 
    when level = 6 then 'BU+3 Lead' 
    when level = 5 then 'BU+2 Lead' 
    when level = 4 then 'BU+1 Lead' 
  end as sales_level, 
  crominus2name as bu_plus_1_lead, 
  crominus3name as bu_plus_2_lead, 
  crominus4name as bu_plus_3_lead,
  crominus2email as bu_plus_1_lead_email,
  crominus3email as bu_plus_2_lead_email,
  crominus4email as bu_plus_3_lead_email,
  Business_Unit, Region_Level_1, Region_Level_2, Region_Level_3
from main.gtm_silver.individual_hierarchy_salesforce
--and IsAE = true
where IsActive = true
and Business_Unit = '${business_unit}'
and Region_Level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,forecast_account_region
CREATE OR REFRESH MATERIALIZED VIEW account_region AS
SELECT  
  m.account_id,
  m.territory_name,
  m.parent_name3 AS business_unit,
  m.parent_name2 AS subregion_level_1,
  m.parent_name1 AS subregion_level_2,
  m.parent_name0 AS subregion_level_3
FROM main.gtm_silver.account_territory_map m --this preserves the original SFDC hierarchy names, useful to check HOLD accounts when at are assigned at intermin to another region level 3.
WHERE m.parent_name3 = '${business_unit}'
AND m.parent_name2 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,forecast_financial_quarters
CREATE OR REFRESH MATERIALIZED VIEW financial_quarters AS
select fq, fy, q
  , max(last_day_of_month) as fiscal_quarter_end_date
  , min(m) as fiscal_quarter_start_date
  , case when getdate() > max(last_day_of_month) then q else null end as last_closed_q
  , case when getdate() > max(last_day_of_month) then true else false end as is_quarter_closed
  , sum(day(last_day_of_month)) as days_in_quarter
  , case when getdate() >= min(m) and current_date() <= max(last_day_of_month) then true else false end as is_current_fiscal_quarter
  , case when current_date() >= make_date(fy - 1, 2, 1) and current_date() <= make_date(fy, 1, 31) then 1 else 0 end as is_current_fiscal_year
  , (select latest_usage_date from snapshot_date) as latest_usage_date
  , greatest(0, least(sum(day(last_day_of_month)), datediff(max(last_day_of_month), (select latest_usage_date from snapshot_date)))) as days_left_in_quarter
  , case when getdate() >= min(m) and current_date() <= max(last_day_of_month) then q else 0 end as current_quarter_number
from dates
group by fq, fy, q

-- COMMAND ----------

-- DBTITLE 1,agg_list_bu
CREATE OR REFRESH MATERIALIZED VIEW agg_list_bu
AS
SELECT DISTINCT Business_Unit AS business_unit
FROM main.gtm_silver.individual_hierarchy_salesforce
WHERE Business_Unit IS NOT NULL
  AND Business_Unit = '${business_unit}'
  AND Region_Level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,agg_region_hierarchy
CREATE OR REFRESH MATERIALIZED VIEW agg_region_hierarchy
AS
SELECT DISTINCT
  Business_Unit   AS business_unit,
  Region_Level_1  AS region_level_1,
  Region_Level_2  AS region_level_2,
  Region_Level_3  AS region_level_3
FROM main.gtm_silver.individual_hierarchy_salesforce
WHERE Business_Unit IS NOT NULL
  AND Business_Unit = '${business_unit}'
  AND Region_Level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,agg_quarters
-- GLOBAL (static list, no region column). Do NOT parameterize.
CREATE OR REFRESH MATERIALIZED VIEW agg_quarters
AS
SELECT fiscal_quarter
FROM (
  VALUES
    ('FY26-Q1'), ('FY26-Q2'), ('FY26-Q3'), ('FY26-Q4'),
    ('FY27-Q1'), ('FY27-Q2'), ('FY27-Q3'), ('FY27-Q4'),
    ('FY28-Q1'), ('FY28-Q2'), ('FY28-Q3'), ('FY28-Q4')
) AS t(fiscal_quarter)

-- COMMAND ----------

-- DBTITLE 1,agg_sales_hierarchy
CREATE OR REFRESH MATERIALIZED VIEW agg_sales_hierarchy
AS
SELECT DISTINCT
  Email               AS user_email,
  user_name,
  Business_Unit       AS business_unit,
  Region_Level_1      AS region_level_1,
  Region_Level_2      AS region_level_2,
  concatenated_emails
FROM main.gtm_gold.rpt_individual_hierarchy_active_valid_sales_user_only
WHERE Business_Unit = '${business_unit}'
  AND Region_Level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,agg_field_hierarchy
CREATE OR REFRESH MATERIALIZED VIEW agg_field_hierarchy
AS
SELECT DISTINCT
  email               AS user_email,
  concatenated_emails
FROM main.gtm_silver.individual_hierarchy_field
WHERE Business_Unit = '${business_unit}'
  AND Region_Level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,agg_sa_ae_map
CREATE OR REFRESH MATERIALIZED VIEW agg_sa_ae_map
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
  AND a.sa_only_concatenated_emails IS NOT NULL
  AND ti.Business_Unit = '${business_unit}'
  AND ti.Region_Level_1 = '${region_level_1}'