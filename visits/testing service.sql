select count(*) 
from (
select child_id, nvl(service_group_name, 'No Service') as service_name,
DCHILD.FIRST_NM || ' ' || dchild.last_nm as child_full_nm
from reporting.fact_case_child_status fccs
inner join reporting.dim_child dchild
on dchild.dim_child_id = fccs.dim_child_id
inner join reporting.dim_status stat
on stat.dim_status_id = fccs.dim_status_id
left join 
(
    select * from
    (
        select fcs.dim_child_id, service_group_name,
        row_number() over(partition by fcs.dim_child_id order by service_group_rank) as rn
        from reporting.fact_child_service fcs
        inner join reporting.dim_service dserv
        on dserv.dim_service_id = fcs.dim_service_id
        where to_date('8/26/2015', 'mm/dd/yyyy') between fcs.eff_dt and fcs.end_dt
        and current_flag = 'Y'
    )
   -- where rn = 1
) serv
on serv.dim_child_id = fccs.dim_child_id
where status_cd in ('CO','CS')
and 20150826 between fccs.dim_date_id_end_of_day_eff and fccs.dim_date_id_end_of_day_end
)



select distinct service_group_rank, service_group_name
from reporting.dim_service
where service_group_name = 'Aftercare-Nonplacement'
or service_group_name = 'Placement'

select distinct service_group_rank, service_group_name
from reporting.dim_service
order by service_group_rank

select * from reporting.dim_child


select distinct nvl(service_group_name, 'No Service') as service
from reporting.fact_child_service fcs
inner join reporting.dim_child dchild on
dchild.dim_child_id = fcs.dim_child_id
inner join reporting.dim_service dserv on
dserv.dim_service_id = fcs.dim_service_id
where to_date('8/26/2015', 'mm/dd/yyyy') between eff_dt and end_dt
and current_flag = 'Y'
order by service


select count(*)
from reporting.fact_child_service fcs
where to_date('8/26/2015', 'mm/dd/yyyy') between eff_dt and end_dt
and current_flag = 'Y'

select distinct nvl(service_group_name, 'No Service') from reporting.dim_service

select 