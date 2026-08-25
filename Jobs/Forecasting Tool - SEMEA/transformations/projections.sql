-- Databricks notebook source
-- DBTITLE 1,forecast_incremental_projections
CREATE OR REFRESH MATERIALIZED VIEW incremental_projections AS
select uco.user_id, uco.ae_email, uco.usecase_id, uco.account_id, uco.account_name, d.fq, d.fy, d.q, d.m, d.cm, d.last_day_of_month
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
  ip.user_id, ip.ae_email, ip.usecase_id, ip.account_id, ip.account_name, ip.fy, ip.fq, ip.q, ip.m, ip.cm, ip.ramping_dbus, ip.current_dbu_baseline, ip.is_onboarding
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
SELECT usecase_id, account_id, account_name, fy, fq, q, m, cm,
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
select p.user_id, p.account_id, p.account_name, p.ae_email, p.fy, p.fq, p.q, p.last_closed_q, p.days_left_in_quarter, p.is_quarter_closed, p.is_current_fiscal_quarter
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