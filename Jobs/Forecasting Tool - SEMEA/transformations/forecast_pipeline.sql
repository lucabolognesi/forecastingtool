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
      when days_to_go_live >= 0 and stage_number < 5 and days_to_go_live < (5 - stage_number) * 30
        then concat('Go live in ', cast(days_to_go_live as string), ' days from stage ', stage)
      when days_to_go_live >= 0 and stage_number = 5 and implementation_status <> "Green" 
        then concat('Go live in ', cast(days_to_go_live as string), ' days from stage ', stage, " (", implementation_status, ")") 
      else 'Ok'
    end as go_live_slippage_details
  , case
      when days_to_onboarding >= 0 and stage_number < 4 and days_to_onboarding < (4 - stage_number) * 30
        then concat('Onboarding in ', cast(days_to_onboarding as string), ' days from stage ', stage)
      when days_to_onboarding >= 0 and stage_number = 4 and implementation_status <> "Green" 
        then concat('Onboarding in ', cast(days_to_go_live as string), ' days from stage ', stage, " (", implementation_status, ")") 
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

-- COMMAND ----------

-- DBTITLE 1,agg_okr_product
CREATE OR REFRESH MATERIALIZED VIEW agg_okr_product
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
    REPLACE(REPLACE(a.fiscal_year_quarter, CHR(39), ''), ' ', '-') AS fiscal_year_quarter,
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
    AND a.Business_Unit = '${business_unit}'
    AND a.sales_subregion_level_1 = '${region_level_1}'
    AND (dayofweek(a.usage_date) = 6
         OR a.usage_date = (SELECT MAX(usage_date) FROM main.gtm_gold.account_consumption_daily))
  GROUP BY
    a.horizontal_and_vertical_hierarchy_concatenated_emails,
    a.Business_Unit, a.sales_subregion_level_1, a.sales_subregion_level_2,
    a.deployable_account_name, a.fiscal_year,
    REPLACE(REPLACE(a.fiscal_year_quarter, CHR(39), ''), ' ', '-'),
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
  FROM main.gtm_gold.account_consumption_daily
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
  FROM main.gtm_gold.account_consumption_daily
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
WITH account_dims AS (
  SELECT DISTINCT
    deployable_account_name,
    horizontal_and_vertical_hierarchy_concatenated_emails,
    Business_Unit,
    sales_subregion_level_1,
    sales_subregion_level_2
  FROM main.gtm_gold.account_consumption_daily
  WHERE YEAR(usage_date) + CASE WHEN MONTH(usage_date) >= 2 THEN 1 ELSE 0 END >= 2026
    AND Business_Unit = '${business_unit}'
    AND sales_subregion_level_1 = '${region_level_1}'
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
WHERE (DAYOFWEEK(k.date) = 6 OR k.date = (SELECT MAX(date) FROM genie_kpis))

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

-- COMMAND ----------

-- DBTITLE 1,agg_ucos
-- NOTE: monthly_projection_by_usecase, usecases_filtered and asq_summary are now
-- produced by THIS pipeline (unqualified references => intra-pipeline dependencies).
-- Previously these were cross-schema reads that required the pipeline to run before
-- the separate refresh_agg_mvs job.
CREATE OR REFRESH MATERIALIZED VIEW agg_ucos
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
    SELECT usecase_id, m, ramping_dbus FROM monthly_projection_by_usecase
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
    FROM monthly_projection_by_usecase
    GROUP BY usecase_id, fq
  )
  PIVOT (SUM(quarterly_incremental_dbus) AS dbus FOR fq IN (
    'FY26-Q1' AS FY26_Q1, 'FY26-Q2' AS FY26_Q2, 'FY26-Q3' AS FY26_Q3, 'FY26-Q4' AS FY26_Q4,
    'FY27-Q1' AS FY27_Q1, 'FY27-Q2' AS FY27_Q2, 'FY27-Q3' AS FY27_Q3, 'FY27-Q4' AS FY27_Q4
  ))
),
usecases_filtered_dedup AS (
  SELECT *
  FROM usecases_filtered
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
LEFT JOIN asq_summary AS asq ON asq.usecase_id = c.usecase_id
WHERE c.Business_Unit = '${business_unit}'
  AND c.sales_subregion_level_1 = '${region_level_1}'