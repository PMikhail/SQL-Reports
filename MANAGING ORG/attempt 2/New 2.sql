select dim_case_id, dim_managing_org_id, count(*)
from DHS_CORE.FACT_CASE_CUA_REFERRAL
where called_back_ind = 'N'
and completed_dt is not null
group by dim_case_id, dim_managing_org_id
having count(*) > 1

select *
from fact_case_cua_referral
where called_back_ind = 'N'
and completed_dt is not null 
and dim_case_id = 151465

and (dim_managing_org_id <> 0 and dim_managing_org_id <> 9 )