# Databricks notebook source
# DBTITLE 1,forecast_dates
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dates AS
# MAGIC select cast(m as date) as m, fq, fy, q
# MAGIC   , last_day(m) as last_day_of_month, year(m) as cy, month(m) as cm
# MAGIC   from (
# MAGIC     values 
# MAGIC       ('2025-02-01', 'FY26-Q1', 2026, 1),
# MAGIC       ('2025-03-01', 'FY26-Q1', 2026, 1),
# MAGIC       ('2025-04-01', 'FY26-Q1', 2026, 1),
# MAGIC       ('2025-05-01', 'FY26-Q2', 2026, 2),
# MAGIC       ('2025-06-01', 'FY26-Q2', 2026, 2),
# MAGIC       ('2025-07-01', 'FY26-Q2', 2026, 2),
# MAGIC       ('2025-08-01', 'FY26-Q3', 2026, 3),
# MAGIC       ('2025-09-01', 'FY26-Q3', 2026, 3),
# MAGIC       ('2025-10-01', 'FY26-Q3', 2026, 3),
# MAGIC       ('2025-11-01', 'FY26-Q4', 2026, 4),
# MAGIC       ('2025-12-01', 'FY26-Q4', 2026, 4),
# MAGIC       ('2026-01-01', 'FY26-Q4', 2026, 4),
# MAGIC       ('2026-02-01', 'FY27-Q1', 2027, 1),
# MAGIC       ('2026-03-01', 'FY27-Q1', 2027, 1),
# MAGIC       ('2026-04-01', 'FY27-Q1', 2027, 1),
# MAGIC       ('2026-05-01', 'FY27-Q2', 2027, 2),
# MAGIC       ('2026-06-01', 'FY27-Q2', 2027, 2),
# MAGIC       ('2026-07-01', 'FY27-Q2', 2027, 2),
# MAGIC       ('2026-08-01', 'FY27-Q3', 2027, 3),
# MAGIC       ('2026-09-01', 'FY27-Q3', 2027, 3),
# MAGIC       ('2026-10-01', 'FY27-Q3', 2027, 3),
# MAGIC       ('2026-11-01', 'FY27-Q4', 2027, 4),
# MAGIC       ('2026-12-01', 'FY27-Q4', 2027, 4),
# MAGIC       ('2027-01-01', 'FY27-Q4', 2027, 4) 
# MAGIC   ) as dates(m, fq, fy, q)

# COMMAND ----------

# DBTITLE 1,forecast_snapshot_date
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW snapshot_date AS
# MAGIC select
# MAGIC   (select max(usage_date) from main.gtm_gold.individual_consumption_daily) as latest_usage_date

# COMMAND ----------

# DBTITLE 1,forecast_ae_list
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW ae_list AS
# MAGIC -- Resolve :ae_email to a list of AE emails.
# MAGIC -- If :ae_email is an AE, returns just that email. If a manager, returns all AEs reporting to them.
# MAGIC select 
# MAGIC   user_id,
# MAGIC   Email as ae_email, 
# MAGIC   user_name, 
# MAGIC   IsAE,
# MAGIC   case 
# MAGIC     when level = 7 then 'AE' 
# MAGIC     when level = 6 then 'BU+3 Lead' 
# MAGIC     when level = 5 then 'BU+2 Lead' 
# MAGIC     when level = 4 then 'BU+1 Lead' 
# MAGIC   end as sales_level, 
# MAGIC   crominus2name as bu_plus_1_lead, 
# MAGIC   crominus3name as bu_plus_2_lead, 
# MAGIC   crominus4name as bu_plus_3_lead,
# MAGIC   crominus2email as bu_plus_1_lead_email,
# MAGIC   crominus3email as bu_plus_2_lead_email,
# MAGIC   crominus4email as bu_plus_3_lead_email,
# MAGIC   Business_Unit, Region_Level_1, Region_Level_2, Region_Level_3
# MAGIC from main.gtm_silver.individual_hierarchy_salesforce
# MAGIC --and IsAE = true
# MAGIC where IsActive = true
# MAGIC and Business_Unit = '${business_unit}'
# MAGIC and Region_Level_1 = '${region_level_1}'

# COMMAND ----------

# DBTITLE 1,forecast_account_region
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW account_region AS
# MAGIC SELECT  
# MAGIC   m.account_id,
# MAGIC   m.territory_name,
# MAGIC   m.parent_name3 AS business_unit,
# MAGIC   m.parent_name2 AS subregion_level_1,
# MAGIC   m.parent_name1 AS subregion_level_2,
# MAGIC   m.parent_name0 AS subregion_level_3
# MAGIC FROM main.gtm_silver.account_territory_map m --this preserves the original SFDC hierarchy names, useful to check HOLD accounts when at are assigned at intermin to another region level 3.
# MAGIC WHERE m.parent_name3 = '${business_unit}'
# MAGIC AND m.parent_name2 = '${region_level_1}'

# COMMAND ----------

# DBTITLE 1,forecast_financial_quarters
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW financial_quarters AS
# MAGIC select fq, fy, q
# MAGIC   , max(last_day_of_month) as fiscal_quarter_end_date
# MAGIC   , min(m) as fiscal_quarter_start_date
# MAGIC   , case when getdate() > max(last_day_of_month) then q else null end as last_closed_q
# MAGIC   , case when getdate() > max(last_day_of_month) then true else false end as is_quarter_closed
# MAGIC   , sum(day(last_day_of_month)) as days_in_quarter
# MAGIC   , case when getdate() >= min(m) and current_date() <= max(last_day_of_month) then true else false end as is_current_fiscal_quarter
# MAGIC   , case when current_date() >= make_date(fy - 1, 2, 1) and current_date() <= make_date(fy, 1, 31) then 1 else 0 end as is_current_fiscal_year
# MAGIC   , (select latest_usage_date from snapshot_date) as latest_usage_date
# MAGIC   , greatest(0, least(sum(day(last_day_of_month)), datediff(max(last_day_of_month), (select latest_usage_date from snapshot_date)))) as days_left_in_quarter
# MAGIC   , case when getdate() >= min(m) and current_date() <= max(last_day_of_month) then q else 0 end as current_quarter_number
# MAGIC from dates
# MAGIC group by fq, fy, q

# COMMAND ----------

# DBTITLE 1,agg_list_bu
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_list_bu
# MAGIC AS
# MAGIC SELECT DISTINCT Business_Unit AS business_unit
# MAGIC FROM main.gtm_silver.individual_hierarchy_salesforce
# MAGIC WHERE Business_Unit IS NOT NULL
# MAGIC   AND Business_Unit = '${business_unit}'
# MAGIC   AND Region_Level_1 = '${region_level_1}'

# COMMAND ----------

# DBTITLE 1,agg_region_hierarchy
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_region_hierarchy
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   Business_Unit   AS business_unit,
# MAGIC   Region_Level_1  AS region_level_1,
# MAGIC   Region_Level_2  AS region_level_2,
# MAGIC   Region_Level_3  AS region_level_3
# MAGIC FROM main.gtm_silver.individual_hierarchy_salesforce
# MAGIC WHERE Business_Unit IS NOT NULL
# MAGIC   AND Business_Unit = '${business_unit}'
# MAGIC   AND Region_Level_1 = '${region_level_1}'

# COMMAND ----------

# DBTITLE 1,agg_quarters
# MAGIC %sql
# MAGIC -- GLOBAL (static list, no region column). Do NOT parameterize.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_quarters
# MAGIC AS
# MAGIC SELECT fiscal_quarter
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC     ('FY26-Q1'), ('FY26-Q2'), ('FY26-Q3'), ('FY26-Q4'),
# MAGIC     ('FY27-Q1'), ('FY27-Q2'), ('FY27-Q3'), ('FY27-Q4'),
# MAGIC     ('FY28-Q1'), ('FY28-Q2'), ('FY28-Q3'), ('FY28-Q4')
# MAGIC ) AS t(fiscal_quarter)

# COMMAND ----------

# DBTITLE 1,agg_sales_hierarchy
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_sales_hierarchy
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   Email               AS user_email,
# MAGIC   user_name,
# MAGIC   Business_Unit       AS business_unit,
# MAGIC   Region_Level_1      AS region_level_1,
# MAGIC   Region_Level_2      AS region_level_2,
# MAGIC   concatenated_emails
# MAGIC FROM main.gtm_gold.rpt_individual_hierarchy_active_valid_sales_user_only
# MAGIC WHERE Business_Unit = '${business_unit}'
# MAGIC   AND Region_Level_1 = '${region_level_1}'

# COMMAND ----------

# DBTITLE 1,agg_field_hierarchy
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_field_hierarchy
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   email               AS user_email,
# MAGIC   concatenated_emails
# MAGIC FROM main.gtm_silver.individual_hierarchy_field
# MAGIC WHERE Business_Unit = '${business_unit}'
# MAGIC   AND Region_Level_1 = '${region_level_1}'

# COMMAND ----------

# DBTITLE 1,agg_sa_ae_map
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW agg_sa_ae_map
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   a.sa_only_concatenated_emails,
# MAGIC   ti.Email          AS ae_email,
# MAGIC   ti.user_name      AS ae_user_name,
# MAGIC   ti.Business_Unit  AS business_unit,
# MAGIC   ti.Region_Level_1 AS region_level_1,
# MAGIC   ti.Region_Level_2 AS region_level_2
# MAGIC FROM main.gtm_gold.account_active_users_daily a
# MAGIC INNER JOIN main.gtm_silver.targets_individual ti
# MAGIC   ON ti.sfdc_user_id = a.account_executive_user_id
# MAGIC WHERE a.account_executive_user_id IS NOT NULL
# MAGIC   AND a.sa_only_concatenated_emails IS NOT NULL
# MAGIC   AND ti.Business_Unit = '${business_unit}'
# MAGIC   AND ti.Region_Level_1 = '${region_level_1}'