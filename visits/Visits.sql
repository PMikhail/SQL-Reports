select * from
(
    select (to_char(dcase.case_no) || '-' || dchild.suffix) as child_id, dcase.case_name, 
    (dchild.first_nm || ' ' || dchild.last_nm) as child_full_nm, dchild.brth_dt,  
    trunc(months_between(sysdate, dchild.brth_dt)/12) as age,
    fvp.start_dt, fvp.end_dt, service_group_name, worker_dim_worker_id,
    supervisor_dim_worker_id, admin_dim_worker_id, director_dim_worker_id
    from reporting.fact_visit_party fvp
    inner join reporting.dim_case dcase on
    fvp.dim_case_id = dcase.dim_case_id
    inner join reporting.dim_child dchild on
    dchild.dim_child_id = fvp.dim_child_id
    inner join reporting.dim_date_visit ddv on
    ddv.dim_date_id_visit = fvp.dim_date_id_visit
    inner join reporting.fact_case_child_status_hier hier on
    hier.dim_child_id = fvp.dim_child_id and sysdate between hier.eff_dt and hier.end_dt
    inner join reporting.dim_status stat on
    hier.dim_status_id = stat.dim_status_id
    inner join 
    (
        select service_group_name, dim_child_id from 
        (
            select x.dim_child_id, dserv.service_group_name
            , row_number() over(partition by dim_child_id order by service_group_rank) as rn
            from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
            where sysdate between eff_dt and end_dt
        )where rn = 1
    ) sname on sname.dim_child_id = fvp.dim_child_id
    where start_dt is not null
    and status_cd = 'CO'
)
where --child_id = '630200-A'
sysdate between start_dt and end_dt;
order by start_dt;

select * from reporting.dim_child where last_nm = 'Darby' and first_nm = 'Duron'; --875197

select distinct(dim_date_id_visit) from reporting.fact_visit_party where dim_child_id = 875197 and start_dt > to_date('6/1/2015', 'mm/dd/yyyy');

select distinct(dim_date_id_visit), fvp.*
from reporting.fact_visit_party fvp; 

select * from (
select min(service_group_rank)
from reporting.fact_child_service fcs
inner join reporting.dim_service dserv on
fcs.dim_service_id = dserv.dim_service_id
where dim_child_id = 875197
and end_dt >= sysdate);

select service_group_name from (
select x.dim_child_id, dserv.service_group_name
, row_number() over(partition by dim_child_id order by service_group_rank) as rn
from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
where sysdate between eff_dt and end_dt
)
where rn = 1;

select hier.*
from reporting.fact_case_child_status_hier hier
inner join reporting.dim_status stat on
STAT.DIM_STATUS_ID = hier.dim_status_id
where status_cd = 'CO'
and director_dim_worker_id = 152
and sysdate between eff_dt and end_dt