select count(*)
from reporting.fact_case_child_status_hier hier
inner join reporting.dim_child dchild on
dchild.dim_child_id = hier.dim_child_id
inner join reporting.dim_status stat on
HIER.DIM_STATUS_ID = stat.dim_status_id
inner join reporting.dim_position pos on
POS.DIM_POSITION_ID = HIER.WORKER_DIM_POSITION_ID
where status_cd = 'CO'
and POS.CUA_CD_ID = 0
and to_date('8/26/2015 11:59:59 PM', 'mm/dd/yyyy hh:mi:ss PM') between hier.eff_dt and hier.end_dt
--order by child_id


select count(*)
from reporting.fact_case_child_status fccs
left join REPORTING.FACT_CASE_CHILD_STATUS_HIER hier on
hier.dim_child_id = fccs.dim_child_id
left join reporting.dim_child dchild on
dchild.dim_child_id = hier.dim_child_id
left join reporting.dim_status stat on
fccs.DIM_STATUS_ID = stat.dim_status_id
left join reporting.dim_position pos on
POS.DIM_POSITION_ID = HIER.WORKER_DIM_POSITION_ID
where status_cd = 'CO'
and POS.CUA_CD_ID = 0
and 20150826 between fccs.dim_date_id_end_of_day_eff and fccs.dim_date_id_end_of_day_end
--and to_date('8/26/2015 11:59:59 PM', 'mm/dd/yyyy hh:mi:ss PM') between hier.eff_dt and hier.end_dt

select *
from reporting.fact_case_child_status
where dim_child_id = 281159268  
order by end_dt desc

select *
from reporting.dim_child
where child_id = '650692-D'
--or child_id = '650250-B'  