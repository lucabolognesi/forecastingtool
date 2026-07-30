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

-- DBTITLE 1,forecast_targets
CREATE OR REFRESH MATERIALIZED VIEW targets AS
SELECT ae.ae_email, t.Region_Level_1, t.Region_Level_2, t.Region_Level_3, t.user_id, t.dollars as fin_target, t.fiscal_year, concat("FY'", right(t.fiscal_year, 2), ' Q', t.fiscal_quarter) as fiscal_quarter, cast(t.fiscal_quarter as int) as fiscal_quarter_number
FROM main.gtm_silver.targets_individual t
INNER JOIN ae_list ae ON t.Email = ae.ae_email
where t.Business_Unit = '${business_unit}'
and t.Region_Level_1 = '${region_level_1}'
and t.type_target = 'dbu'
and t.snapshot_date = (select max(snapshot_date) from main.gtm_silver.targets_individual)

-- COMMAND ----------

-- DBTITLE 1,forecast_account_targets
CREATE OR REFRESH MATERIALIZED VIEW account_targets AS
SELECT
  t.account_id, t.account_name, ae.ae_email, ih.Region_Level_1, ih.Region_Level_2, ih.Region_Level_3, ih.user_id, t.dbu_dollar_target as fin_target, t.fiscal_year, t.fiscal_quarter, cast(right(t.fiscal_quarter, 1) as int) as fiscal_quarter_number
FROM main.gtm_silver.targets_account AS t
INNER JOIN main.gtm_silver.account_dim ad
  ON t.account_id = ad.account_id
INNER JOIN main.gtm_silver.individual_hierarchy_salesforce ih
  ON ad.account_executive_user_id = ih.user_id
INNER JOIN ae_list ae ON ih.user_id = ae.user_id 
WHERE ih.Business_Unit = '${business_unit}'
  AND ih.Region_Level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,forecast_ds_forecast_account
CREATE OR REFRESH MATERIALIZED VIEW ds_forecast_account AS
-- Rolls up forecast_ds across all accounts under each AE/manager's org, matching how the
-- 'forecast_actuals' table rolls up dbu_actuals via concatenated_emails (direct account_executive_user_id
-- matching only covers individual AEs and returns 0 for managers who own no accounts directly).
select 
  ae.user_id,
  d.account_id,
  d.fiscal_quarter_end_date,
  d.month_end_date as fiscal_month_end_date,
  sum(d.forecast_ds) as current_ds_forecast
from ae_list ae
inner join main.gtm_silver.forecast_consumption_ds_account d
  on d.concatenated_emails like '%' || ae.ae_email || '%'
where d.date_grain = 'Fiscal Quarter'
and d.business_unit = '${business_unit}'
and d.region_level_1 = '${region_level_1}'
and d.snapshot_date = (select max(snapshot_date) from main.gtm_silver.forecast_consumption_ds_account where business_unit = '${business_unit}' and region_level_1 = '${region_level_1}')
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_ds_forecast_ae
CREATE OR REFRESH MATERIALIZED VIEW ds_forecast_ae AS
select user_id,  
fiscal_quarter_end_date,
sum(current_ds_forecast) as current_ds_forecast
from ds_forecast_account
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_sales_forecast
CREATE OR REFRESH MATERIALIZED VIEW sales_forecast AS
select ae.ae_email, f.forecast_owner_id as user_id, f.fiscal_quarter_end_date
  , coalesce(f.submitted_my_call, 0) as submitted_my_call
  , 0 as prev_submitted_my_call
  , coalesce(f.submitted_my_call_w_closed_month_actuals, 0) as submitted_my_call_w_closed_month_actuals
  , coalesce(f.submitted_direct_field_consumption_forecast, 0) as submitted_direct_field_consumption_forecast
  , coalesce(dsf.current_ds_forecast, 0) as current_ds_forecast
  , coalesce(f.submitted_weighted_projection, 0) as submitted_weighted_projection
  , ae.bu_plus_2_lead, ae.bu_plus_3_lead, ae.sales_level, ae.user_name
from main.gtm_silver.forecast_consumption_mcp_individual as f
inner join ae_list ae on f.Email = ae.ae_email
left join ds_forecast_ae dsf
  on dsf.user_id = ae.user_id
  and dsf.fiscal_quarter_end_date = f.fiscal_quarter_end_date
where f.Business_Unit = '${business_unit}'
and f.Region_Level_1 = '${region_level_1}'
and f.snapshot_date = (select max(snapshot_date) from main.gtm_silver.forecast_consumption_mcp_individual where Business_Unit = '${business_unit}' and Region_Level_1 = '${region_level_1}')

-- COMMAND ----------

-- DBTITLE 1,forecast_account_forecast_cte
CREATE OR REFRESH MATERIALIZED VIEW account_forecast_cte AS
select 
  f.account_id, a.account_name, i.Email as ae_email, f.account_executive_user_id as user_id, 
  f.forecast_fiscal_quarter_end_date as fiscal_quarter_end_date
  , ae.bu_plus_2_lead, ae.bu_plus_3_lead, ae.sales_level, ae.user_name
  , coalesce(sum(f.submitted_ae_forecast), 0) as submitted_my_call
  , 0 as prev_submitted_my_call
  , coalesce(sum(f.submitted_ae_forecast), 0) as submitted_direct_field_consumption_forecast
  , coalesce(sum(f.submitted_ae_forecast_w_closed_month_actuals), 0) as submitted_my_call_w_closed_month_actuals
  , coalesce(max(dsc.current_ds_forecast), 0) as current_ds_forecast --using max because forecast_consumption_mcp_account is at month level whilst ds_forecast_account at quarter level - this is to avoid duplication.
  , coalesce(sum(f.submitted_weighted_projection), 0) as submitted_weighted_projection
from main.gtm_silver.forecast_consumption_mcp_account as f
inner join ae_list ae on f.account_executive_user_id = ae.user_id
left join main.gtm_silver.account_dim as a
  on f.account_id = a.account_id
inner join main.gtm_silver.individual_hierarchy_salesforce as i
  on f.account_executive_user_id = i.user_id  
left join ds_forecast_account dsc
  on dsc.account_id = f.account_id
  and dsc.user_id = ae.user_id
  and dsc.fiscal_quarter_end_date = f.forecast_fiscal_quarter_end_date
where i.Business_Unit = '${business_unit}'
and i.Region_Level_1 = '${region_level_1}'
and f.snapshot_date = (select max(snapshot_date) from main.gtm_silver.forecast_consumption_mcp_account)
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_actuals
CREATE OR REFRESH MATERIALIZED VIEW actuals AS
select ae.user_id, ae.ae_email, c.fiscal_quarter_start_date
  , ae.bu_plus_2_lead, ae.bu_plus_3_lead
  , ae.bu_plus_1_lead_email, ae.bu_plus_2_lead_email, ae.bu_plus_3_lead_email, ae.sales_level, ae.user_name
  , ae.business_unit AS Business_Unit, ae.region_level_1 AS Region_Level_1, ae.region_level_2 AS Region_Level_2, ae.region_level_3 AS Region_Level_3
  , sum(c.dbu_dollars_qtd) as dbu_actuals
  , sum(c.dbu_dollars_t7d_avg) as dbu_dollars_t7d_avg, sum(c.dbu_dollars_t28d_avg) as dbu_dollars_t28d_avg
  , sum(c.dbu_dollars_t7d_avg_prev) as dbu_dollars_t7d_avg_prev, sum(c.dbu_dollars_t28d_avg_prev) as dbu_dollars_t28d_avg_prev
from ae_list ae
inner join main.gtm_gold.materialized__view_account_obt as c
  on c.concatenated_emails like '%' || ae.ae_email || '%' -- I need the Sales hierarchy, not just the AE
where c.business_unit = '${business_unit}'
and c.subregion_level_1 = '${region_level_1}'
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_account_actuals
CREATE OR REFRESH MATERIALIZED VIEW account_actuals AS
select ae.user_id, ae.ae_email
  , ae.bu_plus_2_lead, ae.bu_plus_3_lead
  , ae.bu_plus_1_lead_email, ae.bu_plus_2_lead_email, ae.bu_plus_3_lead_email, ae.sales_level, ae.user_name
  , ar.business_unit AS Business_Unit, ar.subregion_level_1 AS Region_Level_1, ar.subregion_level_2 AS Region_Level_2, ar.subregion_level_3 AS Region_Level_3
  , c.account_id, c.account_name, c.fiscal_quarter_start_date
  , coalesce(sum(c.dbu_dollars_qtd), 0) as dbu_actuals
  , coalesce(sum(c.dbu_dollars_t7d_avg), 0) as dbu_dollars_t7d_avg
  , coalesce(sum(c.dbu_dollars_t28d_avg), 0) as dbu_dollars_t28d_avg
  , coalesce(sum(c.dbu_dollars_t7d_avg_prev), 0) as dbu_dollars_t7d_avg_prev
  , coalesce(sum(c.dbu_dollars_t28d_avg_prev), 0) as dbu_dollars_t28d_avg_prev
from ae_list ae
inner join main.gtm_gold.materialized__view_account_obt as c
  on c.account_executive_user_id = ae.user_id
  --on c.concatenated_emails like '%' || ae.ae_email || '%' --
left outer join account_region as ar
  on ar.account_id = c.account_id
where c.business_unit = '${business_unit}'
and c.subregion_level_1 = '${region_level_1}'
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_target_forecast_actuals
CREATE OR REFRESH MATERIALIZED VIEW target_forecast_actuals AS
select a.user_id, a.ae_email, a.bu_plus_2_lead, a.bu_plus_3_lead, a.bu_plus_1_lead_email, a.bu_plus_2_lead_email, a.bu_plus_3_lead_email, a.sales_level, a.user_name
  , a.Business_Unit, a.Region_Level_1, a.Region_Level_2, a.Region_Level_3
  , d.fy, d.q, d.fq, a.fiscal_quarter_start_date, a.dbu_actuals
  , a.dbu_dollars_t7d_avg, a.dbu_dollars_t28d_avg, a.dbu_dollars_t7d_avg_prev, a.dbu_dollars_t28d_avg_prev
  , t.fin_target
  , case when d.is_quarter_closed then a.dbu_actuals else f.submitted_my_call end as submitted_my_call
  , f.submitted_direct_field_consumption_forecast
  , f.current_ds_forecast, f.submitted_weighted_projection
  , coalesce(try_divide(f.submitted_my_call - f.prev_submitted_my_call, nullif(f.prev_submitted_my_call, 0)), 0) as forecast_change_pct
  , coalesce(a.dbu_actuals, 0) as dbu_actuals_coalesced
  , coalesce(case when d.is_quarter_closed then a.dbu_actuals else submitted_my_call end, 0) as dbu_actuals_or_forecast
  , first_value(a.dbu_dollars_t7d_avg) over(partition by a.ae_email order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t7d_avg_latest
  , coalesce(nullif(a.dbu_dollars_t7d_avg, 0), dbu_dollars_t7d_avg_latest) as dbu_dollars_t7d_adj --for future quarters, use the latest t7d available.
  , first_value(a.dbu_dollars_t28d_avg) over(partition by a.ae_email order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t28d_avg_latest
  , coalesce(nullif(a.dbu_dollars_t28d_avg, 0), dbu_dollars_t28d_avg_latest) as dbu_dollars_t28d_adj --for future quarters, use the latest t28d available.
  , first_value(a.dbu_dollars_t7d_avg_prev) over(partition by a.ae_email order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t7d_avg_prev_latest
  , coalesce(nullif(a.dbu_dollars_t7d_avg_prev, 0), dbu_dollars_t7d_avg_prev_latest) as dbu_dollars_t7d_prev_adj --for future quarters, use the latest t7d_prev available.
  , first_value(a.dbu_dollars_t28d_avg_prev) over(partition by a.ae_email order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t28d_avg_prev_latest
  , coalesce(nullif(a.dbu_dollars_t28d_avg_prev, 0), dbu_dollars_t28d_avg_prev_latest) as dbu_dollars_t28d_prev_adj --for future quarters, use the latest t28d_prev available.
  , case when d.is_quarter_closed then 0 else dbu_actuals_coalesced end dbu_actuals_current_quarter
  , case when d.is_quarter_closed then 0 else (dbu_dollars_t7d_adj * d.days_left_in_quarter) end as t7d_proj_left_in_quarter
  , case when d.is_quarter_closed then 0 else (dbu_dollars_t28d_adj * d.days_left_in_quarter) end as t28d_proj_left_in_quarter
  , coalesce(try_divide(f.submitted_my_call - dbu_actuals_coalesced, d.days_left_in_quarter), 0) as target_t7d  
from actuals as a
inner join financial_quarters d
on d.fiscal_quarter_start_date = a.fiscal_quarter_start_date
left outer join sales_forecast as f 
on f.fiscal_quarter_end_date = d.fiscal_quarter_end_date and f.ae_email = a.ae_email
left outer join targets as t 
on t.fiscal_year = d.fy and t.fiscal_quarter_number = d.q and t.user_id = a.user_id

-- COMMAND ----------

-- DBTITLE 1,forecast_account_target_forecast_actuals
CREATE OR REFRESH MATERIALIZED VIEW account_target_forecast_actuals AS
select a.user_id, a.bu_plus_2_lead, a.bu_plus_3_lead, a.bu_plus_1_lead_email, a.bu_plus_2_lead_email, a.bu_plus_3_lead_email, a.sales_level, a.user_name
  , a.Business_Unit, a.Region_Level_1, a.Region_Level_2, a.Region_Level_3
  , a.ae_email, a.account_id, a.account_name, a.fiscal_quarter_start_date, d.fy, d.q, d.fq
  , coalesce(t.fin_target, 0) as fin_target
  , case when d.is_quarter_closed then coalesce(a.dbu_actuals, 0) else coalesce(f.submitted_my_call, 0) end as submitted_my_call
  , case when d.is_quarter_closed then coalesce(a.dbu_actuals, 0) else coalesce(f.submitted_direct_field_consumption_forecast, 0) end as submitted_direct_field_consumption_forecast
  , coalesce(f.current_ds_forecast, 0) as current_ds_forecast
  , coalesce(f.submitted_weighted_projection, 0) as submitted_weighted_projection
  , coalesce(try_divide(f.submitted_my_call - f.prev_submitted_my_call, nullif(f.prev_submitted_my_call, 0)), 0) as forecast_change_pct
  , coalesce(a.dbu_actuals, 0) as dbu_actuals
  , coalesce(a.dbu_dollars_t7d_avg, 0) as dbu_dollars_t7d_avg
  , coalesce(a.dbu_dollars_t28d_avg, 0) as dbu_dollars_t28d_avg
  , coalesce(a.dbu_dollars_t7d_avg_prev, 0) as dbu_dollars_t7d_avg_prev
  , coalesce(a.dbu_dollars_t28d_avg_prev, 0) as dbu_dollars_t28d_avg_prev    
  , coalesce(a.dbu_actuals, 0) as dbu_actuals_coalesced
  , coalesce(case when d.is_quarter_closed then a.dbu_actuals else f.submitted_my_call end, 0) as dbu_actuals_or_forecast
  , first_value(a.dbu_dollars_t7d_avg) over(partition by a.ae_email, a.account_name order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t7d_avg_latest
  , coalesce(nullif(a.dbu_dollars_t7d_avg, 0), dbu_dollars_t7d_avg_latest) as dbu_dollars_t7d_adj
  , first_value(a.dbu_dollars_t28d_avg) over(partition by a.ae_email, a.account_name order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t28d_avg_latest
  , coalesce(nullif(a.dbu_dollars_t28d_avg, 0), dbu_dollars_t28d_avg_latest) as dbu_dollars_t28d_adj
  , first_value(a.dbu_dollars_t7d_avg_prev) over(partition by a.ae_email, a.account_name order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t7d_avg_prev_latest
  , coalesce(nullif(a.dbu_dollars_t7d_avg_prev, 0), dbu_dollars_t7d_avg_prev_latest) as dbu_dollars_t7d_prev_adj
  , first_value(a.dbu_dollars_t28d_avg_prev) over(partition by a.ae_email, a.account_name order by d.is_current_fiscal_quarter desc) AS dbu_dollars_t28d_avg_prev_latest
  , coalesce(nullif(a.dbu_dollars_t28d_avg_prev, 0), dbu_dollars_t28d_avg_prev_latest) as dbu_dollars_t28d_prev_adj
  , case when d.is_quarter_closed then 0 else dbu_actuals_coalesced end dbu_actuals_current_quarter
  , case when d.is_quarter_closed then 0 else (dbu_dollars_t7d_adj * d.days_left_in_quarter) end as t7d_proj_left_in_quarter
  , case when d.is_quarter_closed then 0 else (dbu_dollars_t28d_adj * d.days_left_in_quarter) end as t28d_proj_left_in_quarter
  , coalesce(try_divide(f.submitted_my_call - dbu_actuals_coalesced, d.days_left_in_quarter), 0) as target_t7d
from account_actuals as a
inner join financial_quarters d
  on d.fiscal_quarter_start_date = a.fiscal_quarter_start_date
left outer join account_forecast_cte as f
  on f.fiscal_quarter_end_date = d.fiscal_quarter_end_date and f.user_id = a.user_id and f.account_id = a.account_id
left outer join account_targets as t
  on t.fiscal_year = d.fy and t.fiscal_quarter_number = d.q and t.user_id = a.user_id and t.account_id = a.account_id

-- COMMAND ----------

-- DBTITLE 1,forecast_blockers
CREATE OR REFRESH MATERIALIZED VIEW blockers AS
SELECT
  b.usecase_id,
  SUM(case when b.type = 'Blocked' or isnull(b.type) then 1 else 0 end) as blocked_count,
  SUM(case when b.type = 'Friction' then 1 else 0 end) as friction_count,
  ARRAY_JOIN(
    ARRAY_DISTINCT(FILTER(COLLECT_LIST(
      ARRAY_JOIN(
        FILTER(
          ARRAY(
            COALESCE('<a href="https://databrickinternal.ideas.aha.io/ideas/' || aha_item.aha_reference || '" target="_blank">' || aha_item.aha_reference || '</a>', ''),
            COALESCE(aha_item.aha_name, ''),
            COALESCE(b.comment, '')
          ),
          x -> x != ''
        ),
        ' - '
      )
    ), x -> x IS NOT NULL AND x != '')),
    '. '
  ) AS blocker_details
FROM main.gtm_silver.blocker_detail b
LATERAL VIEW OUTER EXPLODE(b.aha) AS aha_item
WHERE b.business_unit = '${business_unit}'
AND b.region_level_1 = '${region_level_1}'
GROUP BY b.usecase_id

-- COMMAND ----------

-- DBTITLE 1,forecast_use_case_pipeline_changes
CREATE OR REFRESH MATERIALIZED VIEW use_case_pipeline_changes AS
select
  pop.usecase_id,
  pop.stage_number_latest,
  pop.stage_number_prior,
  pop.target_live_date_latest,
  pop.target_live_date_prior,
  pop.target_live_fiscal_year_quarter,
  pop.estimated_quarterly_dollar_dbus_latest,
  pop.estimated_quarterly_dollar_dbus_prior,
  case when pop.stage_number_prior is null then 1
         when pop.stage_number_latest > pop.stage_number_prior then 1
         when pop.stage_number_latest = pop.stage_number_prior then 0
         else -1 end as stage_advanced,
  case when pop.stage_number_prior is null then 'New in pipeline'
         when pop.stage_number_latest > pop.stage_number_prior then concat('Advanced from stage ', cast(pop.stage_number_prior as string))
         when pop.stage_number_latest = pop.stage_number_prior then 'No stage change'
         else concat('Regressed from stage ', cast(pop.stage_number_prior as string))
    end as stage_change_description,
  case when pop.target_live_date_prior is null then 1
         when pop.target_live_date_latest < pop.target_live_date_prior then 1
         when pop.target_live_date_latest = pop.target_live_date_prior then 0
         else -1 end as target_date_pulled_foreward,
  case when pop.target_live_date_prior is null then 'New in pipeline'
         when pop.target_live_date_latest < pop.target_live_date_prior then concat('Pulled forward by ', cast(abs(datediff(pop.target_live_date_latest, pop.target_live_date_prior)) as string), ' days from ', cast(pop.target_live_date_prior as string))
         when pop.target_live_date_latest = pop.target_live_date_prior then 'No target date change'
         else concat('Pushed back by ', cast(datediff(pop.target_live_date_latest, pop.target_live_date_prior) as string), ' days from ', cast(pop.target_live_date_prior as string))
    end as target_date_change_description,
  datediff(pop.target_live_date_latest, pop.target_live_date_prior) as target_live_date_diff_days,
  case when pop.estimated_quarterly_dollar_dbus_prior is null then 1
         when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then 1
         when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 0
         else -1 end as amount_increased,
  case when pop.estimated_quarterly_dollar_dbus_prior is null then 'New in pipeline'
         when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then concat('Increased by ', chr(36), cast(cast(pop.estimated_quarterly_dollar_dbus_latest - pop.estimated_quarterly_dollar_dbus_prior as bigint) as string), ' per quarter')
         when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 'No amount change'
         else concat('Decreased by ', chr(36), cast(cast(abs(pop.estimated_quarterly_dollar_dbus_latest - pop.estimated_quarterly_dollar_dbus_prior) as bigint) as string), ' per quarter')
    end as amount_change_description,
  pop.estimated_quarterly_dollar_dbus_latest - pop.estimated_quarterly_dollar_dbus_prior as change_amount,
  case 
    when (case when pop.estimated_quarterly_dollar_dbus_prior is null then 1 when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then 1 when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 0 else -1 end) < 0 
      or (case when pop.target_live_date_prior is null then 1 when pop.target_live_date_latest < pop.target_live_date_prior then 1 when pop.target_live_date_latest = pop.target_live_date_prior then 0 else -1 end) < 0 
      or (case when pop.stage_number_prior is null then 1 when pop.stage_number_latest > pop.stage_number_prior then 1 when pop.stage_number_latest = pop.stage_number_prior then 0 else -1 end) < 0 
      then 'Negative change' 
    when (case when pop.estimated_quarterly_dollar_dbus_prior is null then 1 when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then 1 when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 0 else -1 end) > 0 
      or (case when pop.target_live_date_prior is null then 1 when pop.target_live_date_latest < pop.target_live_date_prior then 1 when pop.target_live_date_latest = pop.target_live_date_prior then 0 else -1 end) > 0 
      or (case when pop.stage_number_prior is null then 1 when pop.stage_number_latest > pop.stage_number_prior then 1 when pop.stage_number_latest = pop.stage_number_prior then 0 else -1 end) > 0 
      then 'Positive change'
    else 'No change' end as change_type_label
from main.gtm_gold.use_case_pipeline_changes as pop
inner join main.gtm_silver.use_case_detail as d
  on pop.usecase_id = d.usecase_id
  and pop.estimated_quarterly_dollar_dbus_latest = d.estimated_quarterly_dollar_dbus
  and pop.stage_number_latest = d.stage_number
  and pop.target_live_fiscal_year_quarter = d.target_live_fiscal_year_quarter
where pop.period = '${period}'
  and pop.change_type <> 'no change'
  and d.Business_Unit = '${business_unit}'
  and d.sales_subregion_level_1 = '${region_level_1}'

-- COMMAND ----------

-- DBTITLE 1,forecast_usecases_filtered
CREATE OR REFRESH MATERIALIZED VIEW usecases_filtered AS
select ae.user_id, ae.ae_email, use_case_detail.usecase_id, usecase_name, account_name, estimated_monthly_dollar_dbus, target_onboarding_date, target_live_date
  , dateadd(day, 14, date_trunc('month',target_onboarding_date)) as target_onboarding_date_15 
  , dateadd(day, 14, date_trunc('month',target_live_date)) as target_live_date_15
  , datediff(target_live_date, target_onboarding_date) as total_ramping_days 
  , date_format(dateadd(year, +1, dateadd(month, -1, target_onboarding_date)), "'FY'yy'-Q'Q") as target_onboarding_date_fq
  , date_format(dateadd(year, +1, dateadd(month, -1, target_live_date)), "'FY'yy'-Q'Q") as target_live_date_fq
  , concat('<a href="https://databricks.lightning.force.com/lightning/r/UseCase__c/', use_case_detail.usecase_id, '/view" target="_blank">', usecase_name, '</a>') as usecase_url
  , coalesce(num_of_blockers, 0) as num_of_blockers
  , coalesce(blk.blocked_count, 0) as blocked_count
  , coalesce(blk.friction_count, 0) as friction_count
  , blk.blocker_details
  , nullif(trim(regexp_extract(demand_plan_next_steps, '(?s)\\[Risk Mitigation\\](.*?)(\\r?\\n\\s*\\r?\\n|$)', 1)), '') as mitigation_plan
  , nullif(trim(regexp_extract(demand_plan_next_steps, '(?s)\\[Acceleration\\](.*?)(\\r?\\n\\s*\\r?\\n|$)', 1)), '') as acceleration_plan
  , coalesce(mitigation_plan, acceleration_plan) as manager_notes
  , regexp_like(demand_plan_next_steps, '#keytechwin') as is_keytechwin
  , case
      when days_in_stage <= 30 or days_in_stage is null then '0-30 days'
      when days_in_stage > 30 and days_in_stage <= 60 then '31-60 days'
      when days_in_stage > 60 and days_in_stage <= 120 then '61-120 days'
      when days_in_stage > 120 then '120+ days'
    end as days_in_stage_bucket
  , date_diff(DAY, current_date(), last_day(target_live_date)) as days_to_go_live
  , case when days_to_go_live < 0 then true else false end as go_live_in_the_past
  , date_diff(DAY, current_date(), last_day(target_onboarding_date)) as days_to_onboarding
  , concat('<a href="', get(FILTER(use_case_detail.usecase_documents, doc -> doc.document_type = 'Eval Doc'), 0).document_link, '" target="_blank">Eval Doc</a>') as eval_doc_link
  , concat('<a href="', get(FILTER(use_case_detail.usecase_documents, doc -> doc.document_type = 'Onboarding Doc'), 0).document_link, '" target="_blank">Onboarding Doc</a>') as onboarding_doc_link
  , case 
      when days_to_go_live between 0 and 30 and stage_number < 5 then 'Go live < 30 / Not U5'
      when days_to_go_live between 0 and 30 and implementation_status = 'Red' then 'Go live < 30 days / Red'
      when days_to_go_live between 0 and 30 and implementation_status = 'Yellow' then 'Go live < 30 days / Yellow'
      else 'Ok'
    end as go_live_slippage_details
  , case
      when days_to_onboarding between 0 and 30 and stage_number < 5 and implementation_status = 'Red' then 'Onboarding < 30 / Red'
      when days_to_onboarding between 0 and 30 and stage_number < 5 and implementation_status = 'Yellow' then 'Onboarding < 30 / Yellow'
      when days_to_onboarding between 0 and 30 and stage_number <= 3 then 'Onboarding < 30 / <=U3'
      else 'Ok'
    end as onboarding_slippage_details
  , flatten(array(
      case when implementation_status is null then array('Health status not defined') else array() end,
      case when days_to_go_live < 0 then array('Go live date in the past') else array() end,
      case when days_to_onboarding < 0 and stage_number < 5 then array('Past Onboarding date / not U5') else array() end,
      case when days_to_go_live between 0 and 30 and implementation_status = 'Red' then array('Go live < 30 days / Red') else array() end,
      case when eval_doc_link is null and estimated_monthly_dollar_dbus >= 10000 and stage_number between 2 and 4 and eval_path like '%Guided POC%' then array('Eval doc required') else array() end,
      case when onboarding_doc_link is null and estimated_monthly_dollar_dbus >= 10000 and stage_number between 4 and 4 then array('Onboarding doc required') else array() end
    )) as hygiene_rules
  , case when array_size(hygiene_rules) = 0 then false else true end as has_hygiene_issues
  , case when onboarding_slippage_details = 'Ok' then false else true end as has_onboarding_slippage_risk
  , case when go_live_slippage_details = 'Ok' then false else true end as has_go_live_slippage_risk
  , case 
      when onboarding_slippage_details <> 'Ok' then 'Slippage risk with onboarding date'
      when go_live_slippage_details <> 'Ok' then 'Slippage risk with live date'
      else 'No Risk'
    end as slippage_risk
  , coalesce(pop.stage_advanced, 0) as stage_advanced
  , coalesce(pop.stage_change_description, 'No stage change') as stage_change_description
  , coalesce(pop.target_date_pulled_foreward, 0) as target_date_pulled_foreward
  , coalesce(pop.target_date_change_description, 'No target date change') as target_date_change_description
  , pop.target_live_date_diff_days
  , coalesce(pop.amount_increased, 0) as amount_increased
  , coalesce(pop.amount_change_description, 'No amount change') as amount_change_description
  , coalesce(pop.change_amount, 0) as change_amount
  , coalesce(pop.change_type_label, 'No change') as change_type_label
  , case when coalesce(pop.stage_advanced, 0) > 0 then 1 else 0 end as stage_advanced_count
  , case when coalesce(pop.stage_advanced, 0) < 0 then 1 else 0 end as stage_regressed_count
  , case when coalesce(pop.target_date_pulled_foreward, 0) > 0 then 1 else 0 end as live_date_advanced_count
  , case when coalesce(pop.target_date_pulled_foreward, 0) < 0 then 1 else 0 end as live_date_regressed_count
  , case when coalesce(pop.amount_increased, 0) > 0 then 1 else 0 end as amount_grew_count
  , case when coalesce(pop.amount_increased, 0) < 0 then 1 else 0 end as amount_shrank_count
   --Check hygiene issues and risks
  , case
      when go_live_in_the_past then named_struct('category', 'Hygiene', 'msg', 'Go live date in the past')
      when implementation_status is null then named_struct('category', 'Hygiene', 'msg', 'Health status not defined')
      when days_to_onboarding < 0 and stage_number < 5 then named_struct('category', 'Hygiene', 'msg', 'Past Onboarding date / not U5') 
      when days_to_go_live < 30 and stage_number < 5 then named_struct('category', 'Warning', 'msg', 'Go live < 30 / Not U5')
      when days_to_go_live < 30 and implementation_status = 'Red' then named_struct('category', 'Warning', 'msg', 'Go live < 30 days / Red')
      when days_to_go_live < 30 and implementation_status = 'Yellow' then named_struct('category', 'Warning', 'msg', 'Go live < 30 days / Yellow')
      when days_to_go_live < 30 then named_struct('category', 'Warning', 'msg', 'Go live < 30')
      when days_to_onboarding between 0 and 30 and stage_number < 5 and implementation_status = 'Red' then named_struct('category', 'Warning', 'msg', 'Onboarding < 30 / Red')
      when days_to_onboarding between 0 and 30 and stage_number < 5 and implementation_status = 'Yellow' then named_struct('category', 'Warning', 'msg', 'Onboarding < 30 / Yellow')
      when days_to_onboarding between 0 and 30 and stage_number <= 3 then named_struct('category', 'Warning', 'msg', 'Onboarding < 30 / <=U3')
      when days_to_onboarding between 0 and 30 and stage_number = 4 then named_struct('category', 'Warning', 'msg', 'Onboarding < 30 / U4')
      when days_to_go_live < 60 and stage_number = 5 then named_struct('category', 'Opportunity', 'msg', 'Go live < 60 / U5')
      when days_to_onboarding between 0 and 60 and stage_number between 2 and 3 then named_struct('category', 'Opportunity', 'msg', 'Tech win to accelerate')
    end as uco_info
  , coalesce(implementation_status, 'Unknown') as implementation_status
from main.gtm_silver.use_case_detail
inner join ae_list ae on use_case_detail.concatenated_emails like '%' || ae.ae_email || '%'
left outer join blockers as blk on blk.usecase_id = use_case_detail.usecase_id
left join use_case_pipeline_changes as pop
  on pop.usecase_id = use_case_detail.usecase_id
  and pop.estimated_quarterly_dollar_dbus_latest = use_case_detail.estimated_quarterly_dollar_dbus
  and pop.stage_number_latest = use_case_detail.stage_number
  and pop.target_live_fiscal_year_quarter = use_case_detail.target_live_fiscal_year_quarter
where use_case_detail.Business_Unit = '${business_unit}'
and sales_subregion_level_1 = '${region_level_1}'
and is_incremental = true --Excludes upgrades
and stage_number <= 5 --Filter out 'Disqualified', 'Lost' and 'Live' UCOs.
and coalesce(estimated_monthly_dollar_dbus, 0) > 0 -- Excludes zero-valued use cases.

-- COMMAND ----------

-- DBTITLE 1,forecast_asq_summary
CREATE OR REFRESH MATERIALIZED VIEW asq_summary AS
SELECT
  auc.usecase_id,
  array_join(collect_list(
    '<a href="https://databricks.lightning.force.com/lightning/r/ApprovalRequest__c/' || ar.approval_request_id || '/view">' || ar.approval_request_name || '</a>: ' || ar.approval_request_type || ' - ' || ar.owner_user_name || ' (' || ar.status || ')'
  ), '. ') AS ASQ_Summary_HTML
FROM main.gtm_silver.approval_request_detail ar
INNER JOIN main.gtm_silver.approved_use_case_lookup auc
  ON ar.approval_request_id = auc.approval_request_id
WHERE auc.usecase_id IN (SELECT usecase_id FROM usecases_filtered)
AND ar.business_unit = '${business_unit}'
AND ar.region_level_1 = '${region_level_1}'
GROUP BY auc.usecase_id

-- COMMAND ----------

-- DBTITLE 1,forecast_incremental_projections
CREATE OR REFRESH MATERIALIZED VIEW incremental_projections AS
select uco.user_id, uco.ae_email, uco.usecase_id, uco.account_name, d.fq, d.fy, d.q, d.m, d.cm, d.last_day_of_month
  , uco.target_onboarding_date, uco.target_onboarding_date_fq, uco.target_live_date, uco.target_live_date_fq, uco.target_onboarding_date_15, uco.target_live_date_15
  , uco.total_ramping_days, uco.estimated_monthly_dollar_dbus, uco.implementation_status, uco.usecase_url, uco.num_of_blockers 
  , (select latest_usage_date from snapshot_date) as latest_usage_date
  , datediff(latest_usage_date, target_onboarding_date_15) as current_ramping_days 
  ,case when d.m between uco.target_onboarding_date and uco.target_live_date then 1 else 0 end as is_onboarding
  ,case         
    when target_onboarding_date_15 > latest_usage_date then 0
    when latest_usage_date > target_live_date_15 then estimated_monthly_dollar_dbus
    else round(estimated_monthly_dollar_dbus * try_divide(datediff(latest_usage_date, target_onboarding_date_15), total_ramping_days)) 
  end as current_dbu_baseline 
  ,case
      when d.last_day_of_month < uco.target_onboarding_date_15 then 0
      when d.last_day_of_month > uco.target_live_date_15 then uco.estimated_monthly_dollar_dbus
      else round(uco.estimated_monthly_dollar_dbus * try_divide(datediff(d.last_day_of_month, uco.target_onboarding_date_15), uco.total_ramping_days))
    end as ramping_dbus
  , case 
    when d.last_day_of_month < latest_usage_date then 0
    when d.last_day_of_month < uco.target_onboarding_date_15 then 0
    when d.last_day_of_month > uco.target_live_date_15 then uco.estimated_monthly_dollar_dbus - current_dbu_baseline
    else round(uco.estimated_monthly_dollar_dbus * try_divide(datediff(d.last_day_of_month, uco.target_onboarding_date_15), uco.total_ramping_days)) - current_dbu_baseline
  end as ramping_dbus_from_baseline 
  , case 
      when latest_usage_date > m then ramping_dbus - ramping_dbus_from_baseline
      else 0
  end as dbus_generated
from usecases_filtered as uco
inner join dates as d

-- COMMAND ----------

-- DBTITLE 1,forecast_quarterly_projection_by_use_case
CREATE OR REFRESH MATERIALIZED VIEW quarterly_projection_by_use_case AS
select              
  i.user_id, i.usecase_id, i.fy, i.fq, i.q
  , sum(i.ramping_dbus) as quarterly_ramping_dbus    
  , sum(i.dbus_generated) as quarterly_dbus_generated
  , lag(max(i.ramping_dbus)) over (partition by i.user_id, i.usecase_id order by i.fq asc) as last_day_of_prev_quarter_dbus
from incremental_projections as i    
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_monthly_projection
CREATE OR REFRESH MATERIALIZED VIEW monthly_projection AS
select
  ip.user_id, ip.ae_email, ip.usecase_id, ip.account_name, ip.fy, ip.fq, ip.q, ip.m, ip.cm, ip.ramping_dbus, ip.current_dbu_baseline, ip.is_onboarding
  , f.last_closed_q, f.days_left_in_quarter, f.is_quarter_closed, f.is_current_fiscal_quarter
  , qp.last_day_of_prev_quarter_dbus    
  , greatest(qp.last_day_of_prev_quarter_dbus, ip.current_dbu_baseline) as quarter_dbu_baseline
  ,coalesce(
      case 
          when ip.ramping_dbus - greatest(qp.last_day_of_prev_quarter_dbus, ip.current_dbu_baseline) < 0 then 0
          else ip.ramping_dbus - greatest(qp.last_day_of_prev_quarter_dbus, ip.current_dbu_baseline)
      end, 0) as quarterly_incremental_dbus
  , coalesce(
      case when ip.implementation_status = 'Green' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_green 
  , coalesce(
      case when ip.implementation_status = 'Yellow' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_yellow 
  , coalesce(
      case when ip.implementation_status = 'Red' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_red
  , coalesce(
      case when ip.implementation_status = 'Unknown' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_unknown
from incremental_projections as ip
inner join quarterly_projection_by_use_case as qp
on ip.usecase_id = qp.usecase_id
and ip.fq = qp.fq
and ip.user_id = qp.user_id
inner join financial_quarters as f
on ip.fq = f.fq

-- COMMAND ----------

-- DBTITLE 1,forecast_monthly_projection_by_usecase
CREATE OR REFRESH MATERIALIZED VIEW monthly_projection_by_usecase AS
-- Use-case-grain view of monthly_projection: deduplicates the user_id dimension.
-- The metrics (ramping_dbus, quarterly_incremental_dbus, etc.) are identical across users
-- for a given (usecase_id, m) since they depend only on use-case-level attributes.
-- Consumer: "Forecasting Tool - UCOs (NEW)" query.
SELECT usecase_id, account_name, fy, fq, q, m, cm,
       ramping_dbus, current_dbu_baseline, is_onboarding,
       last_closed_q, days_left_in_quarter, is_quarter_closed, is_current_fiscal_quarter,
       last_day_of_prev_quarter_dbus, quarter_dbu_baseline,
       quarterly_incremental_dbus, dbus_in_pipeline_green, dbus_in_pipeline_yellow,
       dbus_in_pipeline_red, dbus_in_pipeline_unknown
FROM monthly_projection
QUALIFY ROW_NUMBER() OVER (PARTITION BY usecase_id, m ORDER BY user_id) = 1

-- COMMAND ----------

-- DBTITLE 1,forecast_account_quarterly_projection
CREATE OR REFRESH MATERIALIZED VIEW account_quarterly_projection AS
select p.user_id, p.account_name, p.ae_email, p.fy, p.fq, p.q, p.last_closed_q, p.days_left_in_quarter, p.is_quarter_closed, p.is_current_fiscal_quarter
  , coalesce(sum(p.quarterly_incremental_dbus), 0) as quarterly_incremental_dbus
  , coalesce(sum(p.dbus_in_pipeline_green), 0) as dbus_in_pipeline_green 
  , coalesce(sum(p.dbus_in_pipeline_yellow), 0) as dbus_in_pipeline_yellow 
  , coalesce(sum(p.dbus_in_pipeline_red), 0) as dbus_in_pipeline_red
  , coalesce(sum(p.dbus_in_pipeline_unknown), 0) as dbus_in_pipeline_unknown
  , coalesce(sum(last_day_of_prev_quarter_dbus), 0) as last_day_of_prev_quarter_dbus
from monthly_projection as p
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_quarterly_projection
CREATE OR REFRESH MATERIALIZED VIEW quarterly_projection AS
select p.user_id, p.ae_email, p.fy, p.fq, p.q, p.last_closed_q, p.days_left_in_quarter, p.is_quarter_closed, p.is_current_fiscal_quarter
  , coalesce(sum(p.quarterly_incremental_dbus), 0) as quarterly_incremental_dbus
  , coalesce(sum(p.dbus_in_pipeline_green), 0) as dbus_in_pipeline_green 
  , coalesce(sum(p.dbus_in_pipeline_yellow), 0) as dbus_in_pipeline_yellow 
  , coalesce(sum(p.dbus_in_pipeline_red), 0) as dbus_in_pipeline_red
  , coalesce(sum(p.dbus_in_pipeline_unknown), 0) as dbus_in_pipeline_unknown
  , coalesce(sum(last_day_of_prev_quarter_dbus), 0) as last_day_of_prev_quarter_dbus
from account_quarterly_projection as p
group by all

-- COMMAND ----------

-- DBTITLE 1,forecast_account_union_individuals
CREATE OR REFRESH MATERIALIZED VIEW account_union_individuals AS
select
  'Org Forecast' as view_name
  , cast(NULL as string) as account_id
  , 'All' as account_name
  , f.user_id, f.ae_email, f.bu_plus_2_lead, f.bu_plus_3_lead, f.bu_plus_1_lead_email, f.bu_plus_2_lead_email, f.bu_plus_3_lead_email, f.sales_level, f.user_name
  , f.Business_Unit, f.Region_Level_1, f.Region_Level_2, f.Region_Level_3
  , f.fy, f.q, f.fq, f.fiscal_quarter_start_date    
  , f.dbu_actuals
  , f.dbu_dollars_t7d_avg, f.dbu_dollars_t28d_avg, f.dbu_dollars_t7d_avg_prev, f.dbu_dollars_t28d_avg_prev
  , f.fin_target, f.submitted_my_call, f.submitted_direct_field_consumption_forecast, f.current_ds_forecast, f.submitted_weighted_projection
  , f.forecast_change_pct
  , f.dbu_actuals_coalesced, f.dbu_actuals_or_forecast
  , f.dbu_dollars_t7d_avg_latest, f.dbu_dollars_t7d_adj
  , f.dbu_dollars_t28d_avg_latest, f.dbu_dollars_t28d_adj
  , f.dbu_dollars_t7d_avg_prev_latest, f.dbu_dollars_t7d_prev_adj
  , f.dbu_dollars_t28d_avg_prev_latest, f.dbu_dollars_t28d_prev_adj
  , f.dbu_actuals_current_quarter, f.t7d_proj_left_in_quarter, f.t28d_proj_left_in_quarter, f.target_t7d
  , coalesce(p.last_closed_q, 0) as last_closed_q
  , coalesce(p.days_left_in_quarter, 0) as days_left_in_quarter
  , coalesce(p.is_quarter_closed, false) as is_quarter_closed
  , coalesce(p.is_current_fiscal_quarter, false) as is_current_fiscal_quarter
  , coalesce(p.quarterly_incremental_dbus, 0) as quarterly_incremental_dbus
  , coalesce(p.dbus_in_pipeline_green, 0) as dbus_in_pipeline_green
  , coalesce(p.dbus_in_pipeline_yellow, 0) as dbus_in_pipeline_yellow
  , coalesce(p.dbus_in_pipeline_red, 0) as dbus_in_pipeline_red
  , coalesce(p.dbus_in_pipeline_unknown, 0) as dbus_in_pipeline_unknown
  , coalesce(p.last_day_of_prev_quarter_dbus, 0) as last_day_of_prev_quarter_dbus
from target_forecast_actuals as f
left outer join quarterly_projection as p
  on p.fy = f.fy and p.q = f.q and p.ae_email = f.ae_email

UNION ALL

select
  'Account Forecast' as view_name
  , f.account_id, f.account_name
  , f.user_id, f.ae_email, f.bu_plus_2_lead, f.bu_plus_3_lead, f.bu_plus_1_lead_email, f.bu_plus_2_lead_email, f.bu_plus_3_lead_email, f.sales_level, f.user_name
  , f.Business_Unit, f.Region_Level_1, f.Region_Level_2, f.Region_Level_3
  , f.fy, f.q, f.fq, f.fiscal_quarter_start_date    
  , f.dbu_actuals
  , f.dbu_dollars_t7d_avg, f.dbu_dollars_t28d_avg, f.dbu_dollars_t7d_avg_prev, f.dbu_dollars_t28d_avg_prev
  , f.fin_target, f.submitted_my_call, f.submitted_direct_field_consumption_forecast, f.current_ds_forecast, f.submitted_weighted_projection
  , f.forecast_change_pct
  , f.dbu_actuals_coalesced, f.dbu_actuals_or_forecast
  , f.dbu_dollars_t7d_avg_latest, f.dbu_dollars_t7d_adj
  , f.dbu_dollars_t28d_avg_latest, f.dbu_dollars_t28d_adj
  , f.dbu_dollars_t7d_avg_prev_latest, f.dbu_dollars_t7d_prev_adj
  , f.dbu_dollars_t28d_avg_prev_latest, f.dbu_dollars_t28d_prev_adj
  , f.dbu_actuals_current_quarter, f.t7d_proj_left_in_quarter, f.t28d_proj_left_in_quarter, f.target_t7d
  , coalesce(p.last_closed_q, 0) as last_closed_q
  , coalesce(p.days_left_in_quarter, 0) as days_left_in_quarter
  , coalesce(p.is_quarter_closed, false) as is_quarter_closed
  , coalesce(p.is_current_fiscal_quarter, false) as is_current_fiscal_quarter
  , coalesce(p.quarterly_incremental_dbus, 0) as quarterly_incremental_dbus
  , coalesce(p.dbus_in_pipeline_green, 0) as dbus_in_pipeline_green
  , coalesce(p.dbus_in_pipeline_yellow, 0) as dbus_in_pipeline_yellow
  , coalesce(p.dbus_in_pipeline_red, 0) as dbus_in_pipeline_red
  , coalesce(p.dbus_in_pipeline_unknown, 0) as dbus_in_pipeline_unknown
  , coalesce(p.last_day_of_prev_quarter_dbus, 0) as last_day_of_prev_quarter_dbus
from account_target_forecast_actuals as f
left outer join account_quarterly_projection as p
  on p.fy = f.fy and p.q = f.q and p.user_id = f.user_id and p.account_name = f.account_name

-- COMMAND ----------

-- DBTITLE 1,forecast_quarterly_summary_cte
CREATE OR REFRESH MATERIALIZED VIEW quarterly_summary AS
SELECT 
u.view_name, u.ae_email, u.account_id, u.account_name, u.sales_level, u.user_name, u.bu_plus_2_lead, u.bu_plus_3_lead, u.bu_plus_1_lead_email, u.bu_plus_2_lead_email, u.bu_plus_3_lead_email, u.Business_Unit, u.Region_Level_1, u.Region_Level_2, u.Region_Level_3, u.fy, u.fq, u.days_left_in_quarter, u.is_quarter_closed , u.is_current_fiscal_quarter
, u.fin_target, u.submitted_my_call 
, u.submitted_direct_field_consumption_forecast, u.current_ds_forecast, u.submitted_weighted_projection
, u.forecast_change_pct
, u.dbu_actuals_coalesced as dbu_actuals, u.dbu_actuals_or_forecast, u.dbu_actuals_current_quarter
, u.dbu_dollars_t7d_adj, u.dbu_dollars_t7d_prev_adj, u.t7d_proj_left_in_quarter, u.target_t7d
, u.dbu_dollars_t28d_adj, u.dbu_dollars_t28d_prev_adj, u.t28d_proj_left_in_quarter

, coalesce(lag(u.dbu_actuals_or_forecast) over (partition by u.user_id, u.account_id order by u.fq asc), 0) as dbu_actuals_or_forecast_prev_quarter
, coalesce(try_divide(u.fin_target - dbu_actuals_or_forecast_prev_quarter, dbu_actuals_or_forecast_prev_quarter), 0) as qoq_target_growth

, u.quarterly_incremental_dbus 
, u.dbus_in_pipeline_green, u.dbus_in_pipeline_yellow, u.dbus_in_pipeline_red, u.dbus_in_pipeline_unknown
, u.dbus_in_pipeline_green * CAST('${green_confidence_pct}' AS DECIMAL(10,4)) as dbus_in_pipeline_green_in_plan
, u.dbus_in_pipeline_yellow * CAST('${yellow_confidence_pct}' AS DECIMAL(10,4)) as dbus_in_pipeline_yellow_in_plan
, u.dbus_in_pipeline_red * CAST('${red_confidence_pct}' AS DECIMAL(10,4)) as dbus_in_pipeline_red_in_plan
, u.dbus_in_pipeline_unknown * CAST('${unknown_confidence_pct}' AS DECIMAL(10,4)) as dbus_in_pipeline_unknown_in_plan

, case when u.is_quarter_closed then 0 when u.is_current_fiscal_quarter then u.t7d_proj_left_in_quarter else dbu_actuals_or_forecast_prev_quarter  end as baseline
, case when u.is_quarter_closed then "Baseline" when u.is_current_fiscal_quarter then "Baseline (T7D Projection + OG)" else "Baseline (previous quarter's forecast) + OG" end as baseline_label

, case when u.is_quarter_closed then 0 else u.dbu_actuals_coalesced + u.t7d_proj_left_in_quarter end as t7d_flat_projection 
, case when u.is_quarter_closed then 0 else u.dbu_actuals_coalesced + u.t28d_proj_left_in_quarter end as t28d_flat_projection

, baseline * CAST('${best_case_qoq_organic_growth}' AS DECIMAL(10,4)) as best_case_proj_with_og_left_in_quarter
, dbus_in_pipeline_green_in_plan + dbus_in_pipeline_yellow_in_plan + dbus_in_pipeline_red_in_plan + dbus_in_pipeline_unknown_in_plan as best_case_pipe_left_in_quarter
, u.dbu_actuals_coalesced + best_case_proj_with_og_left_in_quarter + best_case_pipe_left_in_quarter + CAST('${best_case_adjustments}' AS DECIMAL(10,4)) as best_case_projection

, baseline * CAST('${worst_case_qoq_organic_growth}' AS DECIMAL(10,4)) as worst_case_proj_with_og_left_in_quarter
, dbus_in_pipeline_green_in_plan as worst_case_pipe_left_in_quarter
, u.dbu_actuals_coalesced + worst_case_proj_with_og_left_in_quarter + worst_case_pipe_left_in_quarter + CAST('${worst_case_adjustments}' AS DECIMAL(10,4)) as worst_case_projection

, u.submitted_my_call - u.fin_target as gap_my_call
, best_case_projection - u.fin_target as gap_best_case
, worst_case_projection - u.fin_target as gap_worst_case
, t7d_flat_projection - u.fin_target as gap_t7d_flat_projection
, t28d_flat_projection - u.fin_target as gap_t28d_flat_projection
, u.submitted_weighted_projection - u.fin_target as gap_weighted_projection
, u.current_ds_forecast - u.fin_target as gap_ds_forecast
, u.submitted_direct_field_consumption_forecast - u.fin_target as gap_directs_forecast
, u.submitted_my_call - u.submitted_direct_field_consumption_forecast as gap_my_call_vs_directs

, u.submitted_my_call - dbu_actuals_or_forecast_prev_quarter as qoq_delta_target 
, best_case_projection - dbu_actuals_or_forecast_prev_quarter as qoq_delta_best_case
, worst_case_projection - dbu_actuals_or_forecast_prev_quarter as qoq_delta_worst_case
, t7d_flat_projection - dbu_actuals_or_forecast_prev_quarter as qoq_delta_t7d_flat_projection
, t28d_flat_projection - dbu_actuals_or_forecast_prev_quarter as qoq_delta_t28d_flat_projection
, u.submitted_weighted_projection - dbu_actuals_or_forecast_prev_quarter as qoq_delta_weighted_projection
, u.current_ds_forecast - dbu_actuals_or_forecast_prev_quarter as qoq_delta_ds_forecast
, u.submitted_direct_field_consumption_forecast - dbu_actuals_or_forecast_prev_quarter as qoq_delta_directs_forecast

, coalesce(try_divide(u.submitted_my_call, u.fin_target), 0) as my_call_att
, coalesce(try_divide(qoq_delta_target, dbu_actuals_or_forecast_prev_quarter), 0) as my_call_qoq_perc
, coalesce(try_divide(u.submitted_direct_field_consumption_forecast, u.fin_target), 0) as directs_att
, coalesce(try_divide(qoq_delta_directs_forecast, dbu_actuals_or_forecast_prev_quarter), 0) as directs_qoq_perc
, coalesce(u.dbu_dollars_t7d_adj - u.dbu_dollars_t7d_prev_adj, 0) as t7d_change_dollar_dbu
, coalesce(u.dbu_dollars_t28d_adj - u.dbu_dollars_t28d_prev_adj, 0) as t28d_change_dollar_dbu
, coalesce(try_divide(t7d_change_dollar_dbu, u.dbu_dollars_t7d_prev_adj), 0) as t7d_change_perc
, coalesce(try_divide(t28d_change_dollar_dbu, u.dbu_dollars_t28d_prev_adj), 0) as t28d_change_perc

, format_number(try_divide(u.submitted_my_call, u.fin_target), '#.#%') || 
  " | " || format_number(try_divide(qoq_delta_target, dbu_actuals_or_forecast_prev_quarter), '#.#%') || 
  " | " || format_number(gap_my_call, '$,###.#') as my_call_text
, format_number(try_divide(u.submitted_direct_field_consumption_forecast, u.fin_target), '#.#%') ||
  " | " || format_number(try_divide(qoq_delta_directs_forecast, dbu_actuals_or_forecast_prev_quarter), '#.#%') ||
  " | " || format_number(gap_directs_forecast, '$,###.#') as directs_fct_text
, format_number(try_divide(gap_best_case, u.fin_target), '#.#%') || 
  " | " || format_number(try_divide(qoq_delta_best_case, dbu_actuals_or_forecast_prev_quarter), '#.#%') || 
  " | " || format_number(gap_best_case, '$,###.#') as best_case_text
, format_number(try_divide(worst_case_projection, u.fin_target), '#.#%') || 
  " | " || format_number(try_divide(qoq_delta_worst_case, dbu_actuals_or_forecast_prev_quarter), '#.#%') || 
  " | " || format_number(gap_worst_case, '$,###.#') as worst_case_text
, format_number(try_divide(submitted_weighted_projection, u.fin_target), '#.#%') || 
  " | " || format_number(try_divide(qoq_delta_weighted_projection, dbu_actuals_or_forecast_prev_quarter), '#.#%') || 
  " | " || format_number(gap_weighted_projection, '$,###.#') as weighted_proj_text
, format_number(try_divide(current_ds_forecast, u.fin_target), '#.#%') || 
  " | " || format_number(try_divide(qoq_delta_ds_forecast, dbu_actuals_or_forecast_prev_quarter), '#.#%') || 
  " | " || format_number(gap_ds_forecast, '$,###.#') as ds_forecast_text
, format_number(try_divide(t7d_flat_projection, u.fin_target), '#.#%') || 
  " | " || format_number(try_divide(qoq_delta_t7d_flat_projection, dbu_actuals_or_forecast_prev_quarter), '#.#%') || 
  " | " || format_number(gap_t7d_flat_projection, '$,###.#') as t7d_proj_text
, format_number(try_divide(t28d_flat_projection, u.fin_target), '#.#%') || 
  " | " || format_number(try_divide(qoq_delta_t28d_flat_projection, dbu_actuals_or_forecast_prev_quarter), '#.#%') || 
  " | " || format_number(gap_t28d_flat_projection, '$,###.#') as t28d_proj_text

from account_union_individuals as u

-- COMMAND ----------

-- DBTITLE 1,org_forecast
CREATE OR REFRESH MATERIALIZED VIEW org_forecast AS
SELECT * FROM quarterly_summary
WHERE view_name = 'Org Forecast'

-- COMMAND ----------

-- DBTITLE 1,account_forecast
CREATE OR REFRESH MATERIALIZED VIEW account_forecast AS
SELECT * FROM quarterly_summary
WHERE view_name = 'Account Forecast'