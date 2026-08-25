# Databricks notebook source
# DBTITLE 1,forecast_blockers
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW blockers AS
# MAGIC SELECT
# MAGIC   b.usecase_id,
# MAGIC   SUM(case when b.type = 'Blocked' or isnull(b.type) then 1 else 0 end) as blocked_count,
# MAGIC   SUM(case when b.type = 'Friction' then 1 else 0 end) as friction_count,
# MAGIC   ARRAY_JOIN(
# MAGIC     ARRAY_DISTINCT(FILTER(COLLECT_LIST(
# MAGIC       ARRAY_JOIN(
# MAGIC         FILTER(
# MAGIC           ARRAY(
# MAGIC             COALESCE('<a href="https://databrickinternal.ideas.aha.io/ideas/' || aha_item.aha_reference || '" target="_blank">' || aha_item.aha_reference || '</a>', ''),
# MAGIC             COALESCE(aha_item.aha_name, ''),
# MAGIC             COALESCE('<a href="https://brickster.databricks.com/brickroad/issues/' || b.brickroad_issue_id || '" target="_blank">' || b.brickroad_issue_id || '</a>', ''),
# MAGIC             COALESCE(br.brickroad_issue_title, ''),
# MAGIC             COALESCE(b.comment, '')
# MAGIC           ),
# MAGIC           x -> x != ''
# MAGIC         ),
# MAGIC         ' - '
# MAGIC       )
# MAGIC     ), x -> x IS NOT NULL AND x != '')),
# MAGIC     '. '
# MAGIC   ) AS blocker_details
# MAGIC FROM main.gtm_silver.blocker_detail b
# MAGIC LEFT JOIN main.gtm_silver.brickroad_issue_detail br
# MAGIC   ON b.brickroad_issue_sfdc_id = br.brickroad_issue_sfdc_id
# MAGIC   AND b.usecase_id = br.usecase_id
# MAGIC LATERAL VIEW OUTER EXPLODE(b.aha) AS aha_item
# MAGIC WHERE b.business_unit = '${business_unit}'
# MAGIC AND b.region_level_1 = '${region_level_1}'
# MAGIC GROUP BY b.usecase_id

# COMMAND ----------

# DBTITLE 1,forecast_use_case_pipeline_changes
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW use_case_pipeline_changes AS
# MAGIC select
# MAGIC   pop.usecase_id,
# MAGIC   pop.stage_number_latest,
# MAGIC   pop.stage_number_prior,
# MAGIC   pop.target_live_date_latest,
# MAGIC   pop.target_live_date_prior,
# MAGIC   pop.target_live_fiscal_year_quarter,
# MAGIC   pop.estimated_quarterly_dollar_dbus_latest,
# MAGIC   pop.estimated_quarterly_dollar_dbus_prior,
# MAGIC   case when pop.stage_number_prior is null then 1
# MAGIC          when pop.stage_number_latest > pop.stage_number_prior then 1
# MAGIC          when pop.stage_number_latest = pop.stage_number_prior then 0
# MAGIC          else -1 end as stage_advanced,
# MAGIC   case when pop.stage_number_prior is null then 'New in pipeline'
# MAGIC          when pop.stage_number_latest > pop.stage_number_prior then concat('Advanced from stage ', cast(pop.stage_number_prior as string))
# MAGIC          when pop.stage_number_latest = pop.stage_number_prior then 'No stage change'
# MAGIC          else concat('Regressed from stage ', cast(pop.stage_number_prior as string))
# MAGIC     end as stage_change_description,
# MAGIC   case when pop.target_live_date_prior is null then 1
# MAGIC          when pop.target_live_date_latest < pop.target_live_date_prior then 1
# MAGIC          when pop.target_live_date_latest = pop.target_live_date_prior then 0
# MAGIC          else -1 end as target_date_pulled_foreward,
# MAGIC   case when pop.target_live_date_prior is null then 'New in pipeline'
# MAGIC          when pop.target_live_date_latest < pop.target_live_date_prior then concat('Pulled forward by ', cast(abs(datediff(pop.target_live_date_latest, pop.target_live_date_prior)) as string), ' days from ', cast(pop.target_live_date_prior as string))
# MAGIC          when pop.target_live_date_latest = pop.target_live_date_prior then 'No target date change'
# MAGIC          else concat('Pushed back by ', cast(datediff(pop.target_live_date_latest, pop.target_live_date_prior) as string), ' days from ', cast(pop.target_live_date_prior as string))
# MAGIC     end as target_date_change_description,
# MAGIC   datediff(pop.target_live_date_latest, pop.target_live_date_prior) as target_live_date_diff_days,
# MAGIC   case when pop.estimated_quarterly_dollar_dbus_prior is null then 1
# MAGIC          when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then 1
# MAGIC          when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 0
# MAGIC          else -1 end as amount_increased,
# MAGIC   case when pop.estimated_quarterly_dollar_dbus_prior is null then 'New in pipeline'
# MAGIC          when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then concat('Increased by ', chr(36), cast(cast(pop.estimated_quarterly_dollar_dbus_latest - pop.estimated_quarterly_dollar_dbus_prior as bigint) as string), ' per quarter')
# MAGIC          when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 'No amount change'
# MAGIC          else concat('Decreased by ', chr(36), cast(cast(abs(pop.estimated_quarterly_dollar_dbus_latest - pop.estimated_quarterly_dollar_dbus_prior) as bigint) as string), ' per quarter')
# MAGIC     end as amount_change_description,
# MAGIC   pop.estimated_quarterly_dollar_dbus_latest - pop.estimated_quarterly_dollar_dbus_prior as change_amount,
# MAGIC   case 
# MAGIC     when (case when pop.estimated_quarterly_dollar_dbus_prior is null then 1 when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then 1 when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 0 else -1 end) < 0 
# MAGIC       or (case when pop.target_live_date_prior is null then 1 when pop.target_live_date_latest < pop.target_live_date_prior then 1 when pop.target_live_date_latest = pop.target_live_date_prior then 0 else -1 end) < 0 
# MAGIC       or (case when pop.stage_number_prior is null then 1 when pop.stage_number_latest > pop.stage_number_prior then 1 when pop.stage_number_latest = pop.stage_number_prior then 0 else -1 end) < 0 
# MAGIC       then 'Negative change' 
# MAGIC     when (case when pop.estimated_quarterly_dollar_dbus_prior is null then 1 when pop.estimated_quarterly_dollar_dbus_latest > pop.estimated_quarterly_dollar_dbus_prior then 1 when pop.estimated_quarterly_dollar_dbus_latest = pop.estimated_quarterly_dollar_dbus_prior then 0 else -1 end) > 0 
# MAGIC       or (case when pop.target_live_date_prior is null then 1 when pop.target_live_date_latest < pop.target_live_date_prior then 1 when pop.target_live_date_latest = pop.target_live_date_prior then 0 else -1 end) > 0 
# MAGIC       or (case when pop.stage_number_prior is null then 1 when pop.stage_number_latest > pop.stage_number_prior then 1 when pop.stage_number_latest = pop.stage_number_prior then 0 else -1 end) > 0 
# MAGIC       then 'Positive change'
# MAGIC     else 'No change' end as change_type_label
# MAGIC from main.gtm_gold.use_case_pipeline_changes as pop
# MAGIC inner join main.gtm_silver.use_case_detail as d
# MAGIC   on pop.usecase_id = d.usecase_id
# MAGIC   and pop.estimated_quarterly_dollar_dbus_latest = d.estimated_quarterly_dollar_dbus
# MAGIC   and pop.stage_number_latest = d.stage_number
# MAGIC   and pop.target_live_fiscal_year_quarter = d.target_live_fiscal_year_quarter
# MAGIC where pop.period = '${period}'
# MAGIC   and pop.change_type <> 'no change'
# MAGIC   and d.Business_Unit = '${business_unit}'
# MAGIC   and d.sales_subregion_level_1 = '${region_level_1}'

# COMMAND ----------

# DBTITLE 1,forecast_usecases_filtered
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW usecases_filtered AS
# MAGIC select ae.user_id, ae.ae_email, use_case_detail.usecase_id, usecase_name, use_case_detail.account_id, account_name, estimated_monthly_dollar_dbus, eval_path, coalesce(dsa_user_name, 'No') as dsa_user_name, coalesce(implementation_partner_name, 'No') as implementation_partner_name, target_onboarding_date, target_live_date
# MAGIC   , dateadd(day, 14, date_trunc('month',target_onboarding_date)) as target_onboarding_date_15 
# MAGIC   , dateadd(day, 14, date_trunc('month',target_live_date)) as target_live_date_15
# MAGIC   , datediff(target_live_date, target_onboarding_date) as total_ramping_days 
# MAGIC   , date_format(dateadd(year, +1, dateadd(month, -1, target_onboarding_date)), "'FY'yy'-Q'Q") as target_onboarding_date_fq
# MAGIC   , date_format(dateadd(year, +1, dateadd(month, -1, target_live_date)), "'FY'yy'-Q'Q") as target_live_date_fq
# MAGIC   , concat('<a href="https://databricks.lightning.force.com/lightning/r/UseCase__c/', use_case_detail.usecase_id, '/view" target="_blank">', usecase_name, '</a>') as usecase_url
# MAGIC   , coalesce(num_of_blockers, 0) as num_of_blockers
# MAGIC   , coalesce(blk.blocked_count, 0) as blocked_count
# MAGIC   , coalesce(blk.friction_count, 0) as friction_count
# MAGIC   , blk.blocker_details
# MAGIC   , nullif(trim(regexp_extract(demand_plan_next_steps, '(?s)\\[Risk Mitigation\\](.*?)(\\r?\\n\\s*\\r?\\n|$)', 1)), '') as mitigation_plan
# MAGIC   , nullif(trim(regexp_extract(demand_plan_next_steps, '(?s)\\[Acceleration\\](.*?)(\\r?\\n\\s*\\r?\\n|$)', 1)), '') as acceleration_plan
# MAGIC   , to_date(nullif(regexp_extract(demand_plan_next_steps, '#planned_tech_win_date\\s+(\\d{1,2}-[A-Z]{3}-\\d{4})', 1), ''), 'dd-MMM-yyyy') as planned_techwin_date
# MAGIC   , coalesce(
# MAGIC       try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{4}-\\d{1,2}-\\d{1,2})', 1), 'yyyy-M-d')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{4}/\\d{2}/\\d{2})', 1), 'yyyy/MM/dd')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{4}_\\d{2}_\\d{2})', 1), 'yyyy_MM_dd')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{2}/\\d{2}/\\d{4})', 1), 'dd/MM/yyyy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{1,2}/\\d{1,2}/\\d{4})', 1), 'M/d/yyyy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{2}/\\d{2}/\\d{2})\\b', 1), 'dd/MM/yy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{2}/\\d{2}/\\d{2})\\b', 1), 'MM/dd/yy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{2}-\\d{2}-\\d{4})', 1), 'dd-MM-yyyy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{8})\\b', 1), 'yyyyMMdd')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{6})_', 1), 'yyMMdd')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{2}\\.\\d{2}\\.\\d{4})', 1), 'MM.dd.yyyy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{2}\\.\\d{2}\\.\\d{2})\\b', 1), 'dd.MM.yy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?i)^(\\d{1,2}\\s[A-Za-z]+\\s\\d{4})', 1), 'd MMMM yyyy')
# MAGIC     , try_to_date(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?i)^(\\d{1,2}\\s[A-Za-z]{3}\\s\\d{4})', 1), 'd MMM yyyy')
# MAGIC     , try_to_date(concat(replace(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?i)^([A-Z]{3}\\s*-\\s*\\d{1,2})\\b', 1), ' ', ''), '-', year(current_date())), 'MMM-d-yyyy')
# MAGIC     , try_to_date(concat(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?i)^(\\d{1,2}-[A-Za-z]{3})\\b', 1), '-', year(current_date())), 'd-MMM-yyyy')
# MAGIC     , try_to_date(concat(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{2}/\\d{2})[:\\s]', 1), '-', year(current_date())), 'dd/MM-yyyy')
# MAGIC     , try_to_date(concat(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{1,2}/\\d{2})[:\\s]', 1), '-', year(current_date())), 'd/MM-yyyy')
# MAGIC     , try_to_date(concat(regexp_extract(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '^(\\d{1,2}/\\d{2})[:\\s]', 1), '-', year(current_date())), 'M/dd-yyyy')
# MAGIC   ) as next_steps_last_updated_date
# MAGIC   , case when next_steps_last_updated_date is null or datediff(current_date(), next_steps_last_updated_date) > 30 then true else false end as next_steps_stale
# MAGIC   , coalesce(mitigation_plan, acceleration_plan) as manager_notes
# MAGIC   , regexp_like(demand_plan_next_steps, '#keytechwin') as is_keytechwin
# MAGIC   , case
# MAGIC       when days_in_stage <= 30 or days_in_stage is null then '0-30 days'
# MAGIC       when days_in_stage > 30 and days_in_stage <= 60 then '31-60 days'
# MAGIC       when days_in_stage > 60 and days_in_stage <= 120 then '61-120 days'
# MAGIC       when days_in_stage > 120 then '120+ days'
# MAGIC     end as days_in_stage_bucket
# MAGIC   , date_diff(DAY, current_date(), last_day(target_live_date)) as days_to_go_live
# MAGIC   , case when days_to_go_live < 0 then true else false end as go_live_in_the_past
# MAGIC   , date_diff(DAY, current_date(), last_day(target_onboarding_date)) as days_to_onboarding
# MAGIC   , concat('<a href="', get(FILTER(use_case_detail.usecase_documents, doc -> doc.document_type = 'Eval Doc'), 0).document_link, '" target="_blank">Eval Doc</a>') as eval_doc_link_tmp
# MAGIC   , case 
# MAGIC       when eval_doc_link_tmp is not null then eval_doc_link_tmp 
# MAGIC       when estimated_monthly_dollar_dbus >= 10000 and stage_number between 2 and 4 and eval_path like '%Guided POC%' then 'Required' 
# MAGIC       when eval_path is null then "Unknown"
# MAGIC       else 'Not Required' end eval_doc_link
# MAGIC   , concat('<a href="', get(FILTER(use_case_detail.usecase_documents, doc -> doc.document_type = 'Onboarding Doc'), 0).document_link, '" target="_blank">Onboarding Doc</a>') as onboarding_doc_link
# MAGIC   , case 
# MAGIC       when days_to_go_live >= 0 and stage_number < 5 and days_to_go_live < (5 - stage_number) * 30
# MAGIC         then concat('Go live in ', cast(days_to_go_live as string), ' days from stage ', stage)
# MAGIC       when days_to_go_live >= 0 and stage_number = 5 and implementation_status <> "Green" and days_to_go_live < 30
# MAGIC         then concat('Go live in ', cast(days_to_go_live as string), ' days from stage ', stage, " (", implementation_status, ")") 
# MAGIC       else 'Ok'
# MAGIC     end as go_live_slippage_details
# MAGIC   , case
# MAGIC       when days_to_onboarding >= 0 and stage_number < 4 and days_to_onboarding < (4 - stage_number) * 30
# MAGIC         then concat('Onboarding in ', cast(days_to_onboarding as string), ' days from stage ', stage)
# MAGIC       when days_to_onboarding >= 0 and stage_number = 4 and implementation_status <> "Green" and days_to_onboarding < 30
# MAGIC         then concat('Onboarding in ', cast(days_to_onboarding as string), ' days from stage ', stage, " (", implementation_status, ")") 
# MAGIC       else 'Ok'
# MAGIC     end as onboarding_slippage_details
# MAGIC   , flatten(array(
# MAGIC       case when implementation_status is null then array('Health status not defined') else array() end,
# MAGIC       case when days_to_go_live < 0 then array('Go live date in the past') else array() end,
# MAGIC       case when days_to_onboarding < 0 and stage_number < 5 then array('Past Onboarding date / not U5') else array() end,
# MAGIC       case when days_to_go_live between 0 and 30 and implementation_status = 'Red' then array('Go live < 30 days / Red') else array() end,
# MAGIC       case when eval_doc_link = 'Required' then array('Eval doc required') else array() end,
# MAGIC       case when onboarding_doc_link is null and estimated_monthly_dollar_dbus >= 10000 and stage_number between 4 and 4 then array('Onboarding doc required') else array() end
# MAGIC     )) as hygiene_rules
# MAGIC   , case when array_size(hygiene_rules) = 0 then false else true end as has_hygiene_issues
# MAGIC   , case when onboarding_slippage_details = 'Ok' then false else true end as has_onboarding_slippage_risk
# MAGIC   , case when go_live_slippage_details = 'Ok' then false else true end as has_go_live_slippage_risk
# MAGIC   , case 
# MAGIC       when onboarding_slippage_details <> 'Ok' then 'Slippage risk with onboarding date'
# MAGIC       when go_live_slippage_details <> 'Ok' then 'Slippage risk with live date'
# MAGIC       else 'No Risk'
# MAGIC     end as slippage_risk
# MAGIC   , coalesce(pop.stage_advanced, 0) as stage_advanced
# MAGIC   , coalesce(pop.stage_change_description, 'No stage change') as stage_change_description
# MAGIC   , coalesce(pop.target_date_pulled_foreward, 0) as target_date_pulled_foreward
# MAGIC   , coalesce(pop.target_date_change_description, 'No target date change') as target_date_change_description
# MAGIC   , pop.target_live_date_diff_days
# MAGIC   , coalesce(pop.amount_increased, 0) as amount_increased
# MAGIC   , coalesce(pop.amount_change_description, 'No amount change') as amount_change_description
# MAGIC   , coalesce(pop.change_amount, 0) as change_amount
# MAGIC   , coalesce(pop.change_type_label, 'No change') as change_type_label
# MAGIC   , case when coalesce(pop.stage_advanced, 0) > 0 then 1 else 0 end as stage_advanced_count
# MAGIC   , case when coalesce(pop.stage_advanced, 0) < 0 then 1 else 0 end as stage_regressed_count
# MAGIC   , case when coalesce(pop.target_date_pulled_foreward, 0) > 0 then 1 else 0 end as live_date_advanced_count
# MAGIC   , case when coalesce(pop.target_date_pulled_foreward, 0) < 0 then 1 else 0 end as live_date_regressed_count
# MAGIC   , case when coalesce(pop.amount_increased, 0) > 0 then 1 else 0 end as amount_grew_count
# MAGIC   , case when coalesce(pop.amount_increased, 0) < 0 then 1 else 0 end as amount_shrank_count
# MAGIC   , coalesce(implementation_status, 'Unknown') as implementation_status
# MAGIC from main.gtm_silver.use_case_detail
# MAGIC inner join ae_list ae on use_case_detail.concatenated_emails like '%' || ae.ae_email || '%'
# MAGIC left outer join blockers as blk on blk.usecase_id = use_case_detail.usecase_id
# MAGIC left join use_case_pipeline_changes as pop
# MAGIC   on pop.usecase_id = use_case_detail.usecase_id
# MAGIC   and pop.estimated_quarterly_dollar_dbus_latest = use_case_detail.estimated_quarterly_dollar_dbus
# MAGIC   and pop.stage_number_latest = use_case_detail.stage_number
# MAGIC   and pop.target_live_fiscal_year_quarter = use_case_detail.target_live_fiscal_year_quarter
# MAGIC where use_case_detail.Business_Unit = '${business_unit}'
# MAGIC and sales_subregion_level_1 = '${region_level_1}'
# MAGIC and is_incremental = true --Excludes upgrades
# MAGIC and stage_number <= 5 --Filter out 'Disqualified', 'Lost' and 'Live' UCOs.
# MAGIC and coalesce(estimated_monthly_dollar_dbus, 0) > 0 -- Excludes zero-valued use cases.

# COMMAND ----------

# DBTITLE 1,forecast_asq_summary
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW asq_summary AS
# MAGIC SELECT
# MAGIC   auc.usecase_id,
# MAGIC   array_join(collect_list(
# MAGIC     '<a href="https://databricks.lightning.force.com/lightning/r/ApprovalRequest__c/' || ar.approval_request_id || '/view">' || ar.approval_request_name || '</a>: ' || ar.approval_request_type || ' - ' || ar.owner_user_name || ' (' || ar.status || ')'
# MAGIC   ), '. ') AS ASQ_Summary_HTML
# MAGIC FROM main.gtm_silver.approval_request_detail ar
# MAGIC INNER JOIN main.gtm_silver.approved_use_case_lookup auc
# MAGIC   ON ar.approval_request_id = auc.approval_request_id
# MAGIC WHERE auc.usecase_id IN (SELECT usecase_id FROM usecases_filtered)
# MAGIC AND ar.business_unit = '${business_unit}'
# MAGIC AND ar.region_level_1 = '${region_level_1}'
# MAGIC GROUP BY auc.usecase_id