with owners as
(select dim_case_id
       , dim_managing_org_id
       , completed_dt as event_dt
       , 'CUA' as org_type
  from dhs_core.fact_case_cua_referral
  where completed_dt is not null
  and called_back_ind = 'N'
 union all
 select fwpss.dim_work_product_id as dim_case_id
      , dmo.dim_managing_org_id
      , fwpss.initiated_date as event_dt
       , 'DHS' as org_type
  from dhs_core.FACT_WORK_PROD_STATUS_SUMMARY fwpss
       cross join
       dhs_core.dim_managing_org dmo
     where fwpss.work_product_type_cd = 'CASE'
     and dmo.org_type = 'DHS' and cua_cd = 'NAPP'
 union all
 -- find closed gaps
 select fwpss.dim_work_product_id as dim_case_id
      , dmo.dim_managing_org_id
      , closed_date + 1/86400 as event_dt
       , 'CLOSED' as org_type
  from dhs_core.FACT_WORK_PROD_STATUS_SUMMARY fwpss
       cross join
       dhs_core.dim_managing_org dmo
     where fwpss.work_product_type_cd = 'CASE'
      and fwpss.closed_date is not null
      and dmo.org_type = 'N/A'
)
select dim_case_id
      , event_dt as eff_dt
      , nvl(lead(event_dt) over (partition by dim_case_id order by event_dt) - 1/86400,to_date('12/31/9999','mm/dd/yyyy')) as end_dt
      , dim_managing_org_id
      --, org_type
      , sysdate as last_modified_dt
from owners
where dim_case_id = 151465