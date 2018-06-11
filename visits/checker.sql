select * from reporting.fact_visit_party where dim_child_id = 311432 order by end_dt desc

select * from reporting.dim_case where dim_case_id = 78275

select * from reporting.dim_case where case_no = 191406

select * from reporting.dim_child where dim_child_id = 311432

select * from reporting.fact_child_service where dim_child_id = 311432 order by end_dt desc

select full_name, hier.* 
from reporting.fact_case_child_status_hier hier 
inner join reporting.dim_worker dwork on
dwork.dim_worker_id = worker_dim_worker_id
where dim_child_id = 514959
order by end_dt desc

select 

select * from reporting.fact_case_child_status where dim_child_id = 311432 order by end_dt desc

select * from reporting.dim_status where dim_status_id = 33