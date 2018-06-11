select *
from (
select count(*) over(partition by dim_case_id, end_dt order by eff_dt) as cnt, tmo.*
from temp_managing_org tmo
)
where cnt > 1

select status_cd, fwps.* from fact_work_prod_status fwps
inner join dim_status ds
on ds.dim_status_id = fwps.dim_status_id
where dim_work_product_id = 202314
order by eff_dt

select * from temp_managing_org
where dim_case_id = 202314
order by eff_dt

select * from fact_case_cua_referral
where dim_case_id = 202314