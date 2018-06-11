select wwork.full_name as worker_name, hh.*
from 
(
    select (to_char(dcase.case_no) || '_' || dchild.suffix) as child_id, dcase.case_name,
    (dchild.first_nm || ' ' || dchild.last_nm) as child_full_nm, dchild.brth_dt,
    trunc(months_between(sysdate, dchild.brth_dt)/12) as age, service_group_name,
    hier.worker_dim_worker_id, row_number() over(partition by fvp.dim_child_id order by start_dt) as rowdate, 
    fvp.end_dt as last_visit, fvp.dim_child_id
    from reporting.fact_visit_party fvp
    full outer join
    (
        select service_group_name, dim_child_id from 
            (
                select x.dim_child_id, dserv.service_group_name
                , row_number() over(partition by dim_child_id order by service_group_rank) as rn
                from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
                where sysdate between eff_dt and end_dt
            )where rn = 1
    ) plc on plc.dim_child_id = fvp.dim_child_id

    inner join reporting.fact_case_child_status_hier hier on
    hier.dim_child_id = fvp.dim_child_id and sysdate between hier.eff_dt and hier.end_dt
    inner join reporting.dim_status stat on
    stat.dim_status_id = hier.dim_status_id
    inner join reporting.dim_child dchild on
    dchild.dim_child_id = fvp.dim_child_id
    inner join reporting.dim_case dcase on
    dcase.dim_case_id = fvp.dim_case_id
    where status_cd = 'CO'
    and dim_visit_status_id <> 2
    and plc.dim_child_id is null
    or fvp.dim_child_id is null
    -- qeue: dim_assignment_type_id connects hier and dim_assignment_type
    order by fvp.dim_child_id
    --where dim_child_id = 875197
) hh
inner join reporting.dim_worker wwork on
wwork.dim_worker_id = worker_dim_worker_id  
where rowdate = 1;

select assign.*, assigntype.* from reporting.fact_case_child_status_assign assign
inner join reporting.dim_case dcase on
assign.dim_case_id = dcase.dim_case_id
inner join REPORTING.DIM_ASSIGNMENT_TYPE assigntype on
ASSIGNTYPE.DIM_ASSIGNMENT_TYPE_ID = assign.dim_assignment_type_id
where case_no = 228722
and sysdate between assign.eff_dt and assign.end_dt;

select assign.*, assigntype.* from reporting.fact_case_child_status_assign assign
inner join reporting.dim_case dcase on
assign.dim_case_id = dcase.dim_case_id
inner join REPORTING.DIM_ASSIGNMENT_TYPE assigntype on
ASSIGNTYPE.DIM_ASSIGNMENT_TYPE_ID = assign.dim_assignment_type_id
where queue_long_desc like 'Unknown'
--and sysdate between assign.eff_dt and assign.end_dt