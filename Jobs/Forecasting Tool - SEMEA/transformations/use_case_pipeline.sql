-- Databricks notebook source
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
            COALESCE('<a href="https://brickster.databricks.com/brickroad/issues/' || b.brickroad_issue_id || '" target="_blank">' || b.brickroad_issue_id || '</a>', ''),
            COALESCE(br.brickroad_issue_title, ''),
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
LEFT JOIN main.gtm_silver.brickroad_issue_detail br
  ON b.brickroad_issue_sfdc_id = br.brickroad_issue_sfdc_id
  AND b.usecase_id = br.usecase_id
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
select ae.user_id, ae.ae_email, use_case_detail.usecase_id, usecase_name, use_case_detail.account_id, account_name, estimated_monthly_dollar_dbus, eval_path, coalesce(dsa_user_name, 'No') as dsa_user_name, coalesce(implementation_partner_name, 'No') as implementation_partner_name, target_onboarding_date, target_live_date
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
  , to_date(nullif(regexp_extract(demand_plan_next_steps, '#planned_tech_win_date\\s+(\\d{1,2}-[A-Z]{3}-\\d{4})', 1), ''), 'dd-MMM-yyyy') as planned_techwin_date
  , coalesce(
      -- Date inside brackets: [... DD MonthName YYYY ...] (e.g. [FG - 20 August 2026])
      try_to_date(regexp_extract(demand_plan_next_steps, '(?i)\\[.*?(\\d{1,2}\\s[A-Za-z]+\\s\\d{4}).*?\\]', 1), 'd MMMM yyyy')
    , try_to_date(regexp_extract(demand_plan_next_steps, '(?i)\\[.*?(\\d{1,2}\\s[A-Za-z]{3}\\s\\d{4}).*?\\]', 1), 'd MMM yyyy')
    , try_to_date(regexp_extract(demand_plan_next_steps, '(?i)\\[.*?(\\d{1,2}-[A-Za-z]{3}-\\d{4}).*?\\]', 1), 'd-MMM-yyyy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{4}-\\d{1,2}-\\d{1,2})', 1), 'yyyy-M-d')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{4}/\\d{2}/\\d{2})', 1), 'yyyy/MM/dd')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{4}_\\d{2}_\\d{2})', 1), 'yyyy_MM_dd')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{2}/\\d{2}/\\d{4})', 1), 'dd/MM/yyyy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{1,2}/\\d{1,2}/\\d{4})', 1), 'M/d/yyyy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{1,2}/\\d{1,2}/\\d{2})(?!\\d)', 1), 'd/M/yy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{1,2}/\\d{1,2}/\\d{2})(?!\\d)', 1), 'M/d/yy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{2}-\\d{2}-\\d{4})', 1), 'dd-MM-yyyy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{8})\\b', 1), 'yyyyMMdd')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{6})_', 1), 'yyMMdd')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{2}\\.\\d{2}\\.\\d{4})', 1), 'MM.dd.yyyy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{2}\\.\\d{2}\\.\\d{2})\\b', 1), 'dd.MM.yy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '(?i)^(\\d{1,2}\\s[A-Za-z]+\\s\\d{4})', 1), 'd MMMM yyyy')
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '(?i)^(\\d{1,2}\\s[A-Za-z]{3}\\s\\d{4})', 1), 'd MMM yyyy')
    , try_to_date(concat(replace(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '(?i)^([A-Z]{3}\\s*-\\s*\\d{1,2})\\b', 1), ' ', ''), '-', year(current_date())), 'MMM-d-yyyy')
    , try_to_date(concat(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '(?i)^(\\d{1,2}-[A-Za-z]{3})\\b', 1), '-', year(current_date())), 'd-MMM-yyyy')
    , try_to_date(concat(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{2}/\\d{2})[:\\s]', 1), '-', year(current_date())), 'dd/MM-yyyy')
    , try_to_date(concat(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{1,2}/\\d{2})[:\\s]', 1), '-', year(current_date())), 'd/MM-yyyy')
    , try_to_date(concat(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{1,2}/\\d{2})[:\\s]', 1), '-', year(current_date())), 'M/dd-yyyy')
    -- dd-MM-yy with hyphens (e.g. 18-08-26)
    , try_to_date(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '^(\\d{1,2}-\\d{1,2}-\\d{2})(?!\\d)', 1), 'd-M-yy')
    -- Month abbreviation + day without year (e.g. Jun 11, Aug 3)
    , try_to_date(concat(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '(?i)^([A-Za-z]{3}\\s+\\d{1,2})\\b', 1), ' ', year(current_date())), 'MMM d yyyy')
    -- Full month name + day without year (e.g. July 31, August 3)
    , try_to_date(concat(regexp_extract(regexp_replace(regexp_replace(demand_plan_next_steps, '^#[^\\n]*\\n', ''), '(?<=\\d)(st|nd|rd|th)\\b', ''), '(?i)^([A-Za-z]{4,9})\\s+(\\d{1,2})\\b', 0), ' ', year(current_date())), 'MMMM d yyyy')
  ) as next_steps_last_updated_date
  , case when next_steps_last_updated_date is null or datediff(current_date(), next_steps_last_updated_date) > 30 then true else false end as next_steps_stale
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
  , concat('<a href="', get(FILTER(use_case_detail.usecase_documents, doc -> doc.document_type = 'Eval Doc'), 0).document_link, '" target="_blank">Eval Doc</a>') as eval_doc_link_tmp
  , case 
      when eval_doc_link_tmp is not null then eval_doc_link_tmp 
      when estimated_monthly_dollar_dbus >= 10000 and stage_number between 2 and 4 and eval_path like '%Guided POC%' then 'Required' 
      when eval_path is null then "Unknown"
      else 'Not Required' end eval_doc_link
  , concat('<a href="', get(FILTER(use_case_detail.usecase_documents, doc -> doc.document_type = 'Onboarding Doc'), 0).document_link, '" target="_blank">Onboarding Doc</a>') as onboarding_doc_link
  , case 
      when days_to_go_live >= 0 and stage_number < 5 and days_to_go_live < (5 - stage_number) * 30
        then concat('Go live in ', cast(days_to_go_live as string), ' days from stage ', stage)
      when days_to_go_live >= 0 and stage_number = 5 and implementation_status <> "Green" and days_to_go_live < 30
        then concat('Go live in ', cast(days_to_go_live as string), ' days from stage ', stage, " (", implementation_status, ")") 
      else 'Ok'
    end as go_live_slippage_details
  , case
      when days_to_onboarding >= 0 and stage_number < 4 and days_to_onboarding < (4 - stage_number) * 30
        then concat('Onboarding in ', cast(days_to_onboarding as string), ' days from stage ', stage)
      when days_to_onboarding >= 0 and stage_number = 4 and implementation_status <> "Green" and days_to_onboarding < 30
        then concat('Onboarding in ', cast(days_to_onboarding as string), ' days from stage ', stage, " (", implementation_status, ")") 
      else 'Ok'
    end as onboarding_slippage_details
  , flatten(array(
      case when implementation_status is null then array('Health status not defined') else array() end,
      case when days_to_go_live < 0 then array('Go live date in the past') else array() end,
      case when days_to_onboarding < 0 and stage_number < 5 then array('Past Onboarding date / not U5') else array() end,
      case when days_to_go_live between 0 and 30 and implementation_status = 'Red' then array('Go live < 30 days / Red') else array() end,
      case when eval_doc_link = 'Required' then array('Eval doc required') else array() end,
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