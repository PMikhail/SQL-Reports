select *
from (
select count(*) over(partition by dim_case_id order by completed_dt) as cnt, tmo.*
from fact_case_cua_referral tmo
where called_back_ind = 'N'
and completed_dt is not null
)
where cnt > 1

select fwps.* from fact_work_prod_status_summary fwps
where dim_work_product_id = 207186

select * from temp_managing_org
where dim_case_id = 202314
order by eff_dt



select *
 from fact_case_cua_referral
where dim_case_id = 151465--207186
and called_back_ind = 'N'
order by completed_dt

select * from fact_work_prod_status_summary
where dim_work_product_id = 151465


select * from reporting.fact_case_status
where dim_case_id = 151465
order by eff_dt