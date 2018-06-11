select dim_work_product_id
     , status_cd
     , eff_dt
     , eff_dt_status
     , end_dt
     , end_dt_status
from fact_work_prod_status fwps
inner join dim_status ds
on ds.dim_status_id = fwps.dim_status_id
where fwps.work_product_type_cd = 'CASE'
and dim_work_product_id = 209491
order by dim_work_product_id desc, eff_dt

select * from fact_work_prod_status fwps

select * from fact_work_prod_status_summary where dim_work_product_id = 127432