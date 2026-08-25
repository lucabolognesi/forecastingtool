# Databricks notebook source
# DBTITLE 1,forecast_incremental_projections
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW incremental_projections AS
# MAGIC select uco.user_id, uco.ae_email, uco.usecase_id, uco.account_id, uco.account_name, d.fq, d.fy, d.q, d.m, d.cm, d.last_day_of_month
# MAGIC   , uco.target_onboarding_date, uco.target_onboarding_date_fq, uco.target_live_date, uco.target_live_date_fq, uco.target_onboarding_date_15, uco.target_live_date_15
# MAGIC   , uco.total_ramping_days, uco.estimated_monthly_dollar_dbus, uco.implementation_status, uco.usecase_url, uco.num_of_blockers 
# MAGIC   , (select latest_usage_date from snapshot_date) as latest_usage_date
# MAGIC   , datediff(latest_usage_date, target_onboarding_date_15) as current_ramping_days 
# MAGIC   ,case when d.m between uco.target_onboarding_date and uco.target_live_date then 1 else 0 end as is_onboarding
# MAGIC   ,case         
# MAGIC     when target_onboarding_date_15 > latest_usage_date then 0
# MAGIC     when latest_usage_date > target_live_date_15 then estimated_monthly_dollar_dbus
# MAGIC     else round(estimated_monthly_dollar_dbus * try_divide(datediff(latest_usage_date, target_onboarding_date_15), total_ramping_days)) 
# MAGIC   end as current_dbu_baseline 
# MAGIC   ,case
# MAGIC       when d.last_day_of_month < uco.target_onboarding_date_15 then 0
# MAGIC       when d.last_day_of_month > uco.target_live_date_15 then uco.estimated_monthly_dollar_dbus
# MAGIC       else round(uco.estimated_monthly_dollar_dbus * try_divide(datediff(d.last_day_of_month, uco.target_onboarding_date_15), uco.total_ramping_days))
# MAGIC     end as ramping_dbus
# MAGIC   , case 
# MAGIC     when d.last_day_of_month < latest_usage_date then 0
# MAGIC     when d.last_day_of_month < uco.target_onboarding_date_15 then 0
# MAGIC     when d.last_day_of_month > uco.target_live_date_15 then uco.estimated_monthly_dollar_dbus - current_dbu_baseline
# MAGIC     else round(uco.estimated_monthly_dollar_dbus * try_divide(datediff(d.last_day_of_month, uco.target_onboarding_date_15), uco.total_ramping_days)) - current_dbu_baseline
# MAGIC   end as ramping_dbus_from_baseline 
# MAGIC   , case 
# MAGIC       when latest_usage_date > m then ramping_dbus - ramping_dbus_from_baseline
# MAGIC       else 0
# MAGIC   end as dbus_generated
# MAGIC from usecases_filtered as uco
# MAGIC inner join dates as d

# COMMAND ----------

# DBTITLE 1,forecast_quarterly_projection_by_use_case
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW quarterly_projection_by_use_case AS
# MAGIC select              
# MAGIC   i.user_id, i.usecase_id, i.fy, i.fq, i.q
# MAGIC   , sum(i.ramping_dbus) as quarterly_ramping_dbus    
# MAGIC   , sum(i.dbus_generated) as quarterly_dbus_generated
# MAGIC   , lag(max(i.ramping_dbus)) over (partition by i.user_id, i.usecase_id order by i.fq asc) as last_day_of_prev_quarter_dbus
# MAGIC from incremental_projections as i    
# MAGIC group by all

# COMMAND ----------

# DBTITLE 1,forecast_monthly_projection
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW monthly_projection AS
# MAGIC select
# MAGIC   ip.user_id, ip.ae_email, ip.usecase_id, ip.account_id, ip.account_name, ip.fy, ip.fq, ip.q, ip.m, ip.cm, ip.ramping_dbus, ip.current_dbu_baseline, ip.is_onboarding
# MAGIC   , f.last_closed_q, f.days_left_in_quarter, f.is_quarter_closed, f.is_current_fiscal_quarter
# MAGIC   , qp.last_day_of_prev_quarter_dbus    
# MAGIC   , greatest(qp.last_day_of_prev_quarter_dbus, ip.current_dbu_baseline) as quarter_dbu_baseline
# MAGIC   ,coalesce(
# MAGIC       case 
# MAGIC           when ip.ramping_dbus - greatest(qp.last_day_of_prev_quarter_dbus, ip.current_dbu_baseline) < 0 then 0
# MAGIC           else ip.ramping_dbus - greatest(qp.last_day_of_prev_quarter_dbus, ip.current_dbu_baseline)
# MAGIC       end, 0) as quarterly_incremental_dbus
# MAGIC   , coalesce(
# MAGIC       case when ip.implementation_status = 'Green' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_green 
# MAGIC   , coalesce(
# MAGIC       case when ip.implementation_status = 'Yellow' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_yellow 
# MAGIC   , coalesce(
# MAGIC       case when ip.implementation_status = 'Red' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_red
# MAGIC   , coalesce(
# MAGIC       case when ip.implementation_status = 'Unknown' and not f.is_quarter_closed then quarterly_incremental_dbus else 0 end, 0) as dbus_in_pipeline_unknown
# MAGIC from incremental_projections as ip
# MAGIC inner join quarterly_projection_by_use_case as qp
# MAGIC on ip.usecase_id = qp.usecase_id
# MAGIC and ip.fq = qp.fq
# MAGIC and ip.user_id = qp.user_id
# MAGIC inner join financial_quarters as f
# MAGIC on ip.fq = f.fq

# COMMAND ----------

# DBTITLE 1,forecast_monthly_projection_by_usecase
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW monthly_projection_by_usecase AS
# MAGIC -- Use-case-grain view of monthly_projection: deduplicates the user_id dimension.
# MAGIC -- The metrics (ramping_dbus, quarterly_incremental_dbus, etc.) are identical across users
# MAGIC -- for a given (usecase_id, m) since they depend only on use-case-level attributes.
# MAGIC -- Consumer: "Forecasting Tool - UCOs (NEW)" query.
# MAGIC SELECT usecase_id, account_id, account_name, fy, fq, q, m, cm,
# MAGIC        ramping_dbus, current_dbu_baseline, is_onboarding,
# MAGIC        last_closed_q, days_left_in_quarter, is_quarter_closed, is_current_fiscal_quarter,
# MAGIC        last_day_of_prev_quarter_dbus, quarter_dbu_baseline,
# MAGIC        quarterly_incremental_dbus, dbus_in_pipeline_green, dbus_in_pipeline_yellow,
# MAGIC        dbus_in_pipeline_red, dbus_in_pipeline_unknown
# MAGIC FROM monthly_projection
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY usecase_id, m ORDER BY user_id) = 1

# COMMAND ----------

# DBTITLE 1,forecast_account_quarterly_projection
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW account_quarterly_projection AS
# MAGIC select p.user_id, p.account_id, p.account_name, p.ae_email, p.fy, p.fq, p.q, p.last_closed_q, p.days_left_in_quarter, p.is_quarter_closed, p.is_current_fiscal_quarter
# MAGIC   , coalesce(sum(p.quarterly_incremental_dbus), 0) as quarterly_incremental_dbus
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_green), 0) as dbus_in_pipeline_green 
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_yellow), 0) as dbus_in_pipeline_yellow 
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_red), 0) as dbus_in_pipeline_red
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_unknown), 0) as dbus_in_pipeline_unknown
# MAGIC   , coalesce(sum(last_day_of_prev_quarter_dbus), 0) as last_day_of_prev_quarter_dbus
# MAGIC from monthly_projection as p
# MAGIC group by all

# COMMAND ----------

# DBTITLE 1,forecast_quarterly_projection
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW quarterly_projection AS
# MAGIC select p.user_id, p.ae_email, p.fy, p.fq, p.q, p.last_closed_q, p.days_left_in_quarter, p.is_quarter_closed, p.is_current_fiscal_quarter
# MAGIC   , coalesce(sum(p.quarterly_incremental_dbus), 0) as quarterly_incremental_dbus
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_green), 0) as dbus_in_pipeline_green 
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_yellow), 0) as dbus_in_pipeline_yellow 
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_red), 0) as dbus_in_pipeline_red
# MAGIC   , coalesce(sum(p.dbus_in_pipeline_unknown), 0) as dbus_in_pipeline_unknown
# MAGIC   , coalesce(sum(last_day_of_prev_quarter_dbus), 0) as last_day_of_prev_quarter_dbus
# MAGIC from account_quarterly_projection as p
# MAGIC group by all