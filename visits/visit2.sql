/*
        FIRST ATTEMPT WITH UNMATCHING AGGREGATION FROM EXCEL REPORT
        BELOW THIS IS A BETTER ATTEMPT
*/
select count(*)
from
(
select dwork.full_name as dir_name, awork.full_name as admin_name, 
swork.full_name as supervisor_name, wwork.full_name as worker_name, 
hh.end_dt as visit_dt, dim_child_id 
from 
(
select row_number() over(partition by fvp.dim_child_id order by start_dt) as rowdate, fvp.end_dt,
hier.worker_dim_worker_id, hier.supervisor_dim_worker_id, hier.admin_dim_worker_id,
hier.director_dim_worker_id, fvp.dim_child_id
from reporting.fact_visit_party fvp

left join
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
where status_cd = 'CO'
and dim_visit_status_id <> 2 
-- queue: dim_assignment_type_id connects hier and dim_assignment_type
order by fvp.dim_child_id
--where dim_child_id = 875197
) hh
inner join reporting.dim_worker wwork on
wwork.dim_worker_id = worker_dim_worker_id 
inner join reporting.dim_worker swork on
swork.dim_worker_id = supervisor_dim_worker_id 
inner join reporting.dim_worker awork on
awork.dim_worker_id = admin_dim_worker_id
inner join reporting.dim_worker dwork on 
dwork.dim_worker_id = director_dim_worker_id
where rowdate = 1
);

select * from reporting.dim_worker where dim_worker_id = 0;

select * from reporting.fact_case_child_status_hier
where sysdate between eff_dt and end_dt; --298977



/*
    FOLLOWING IS ATTEMPT TO MATCH WITH COUNTS FROM EXCEL REPORT OF VISITS
    FOLLOWING IS ATTEMPT TO MATCH WITH COUNTS FROM EXCEL REPORT OF VISITS
    FOLLOWING IS ATTEMPT TO MATCH WITH COUNTS FROM EXCEL REPORT OF VISITS
*/

--Query to get visits and
select * from
    (
        select fvp.*, row_number() over(partition by fvp.dim_child_id order by fvp.end_dt) as rn
        from reporting.fact_visit_party fvp
        where fvp.end_dt < SYSDATE
    )
where rn = 1;


select * from reporting.dim_child;

--Query to get the children needing visits
select * from 
(
select 
visits.dim_child_id,
(dchild.first_nm || ' ' || dchild.last_nm) as child_full_nm,
(to_char(dcase.case_no) || '-' || dchild.suffix) as child_id
from reporting.fact_case_child_status_hier hier 
inner join reporting.dim_status stat on
hier.dim_status_id = stat.dim_status_id and status_cd = 'CO'
left join
(
    select * from
        (
            select fvp.*, row_number() over(partition by fvp.dim_child_id order by fvp.end_dt) as rn
            from reporting.fact_visit_party fvp
            where fvp.end_dt < SYSDATE
        )
    where rn = 1
) visits on
visits.dim_child_id = hier.dim_child_id
inner join
(
    select atype.dim_assignment_type_id                 --To filter on queues. 
    from reporting.dim_assignment_type atype
    where queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN'
) atype on
atype.dim_assignment_type_id = hier.dim_assignment_type_id
inner join reporting.dim_case dcase on
hier.dim_case_id = dcase.dim_case_id
inner join 
(
    select case_no, suffix, brth_dt, dim_child_id, last_nm, first_nm
    from reporting.dim_child dchild
) dchild on
dchild.dim_child_id = hier.dim_child_id
where sysdate between hier.eff_dt and hier.end_dt)
where child_id = '149758-G';
and dchild.case_no = '149758' and dchild.suffix = 'G';



select * from
(
    select fvp.* --row_number() over(partition by fvp.dim_child_id order by fvp.end_dt) as rn
    from reporting.fact_visit_party fvp
) fvp
where dim_child_id = 691619

right join reporting.dim_child dchild on
fvp.dim_child_id = dchild.dim_child_id
where rn = 1

select * from reporting.fact_visit_party where dim_child_id = 723882 order by end_dt desc

select status_cd, end_dt, fccs.dim_child_id from reporting.fact_case_child_status fccs
inner join reporting.dim_status stat on
FCCS.DIM_STATUS_ID = stat.dim_status_id
where fccs.dim_child_id = 723882


select * from reporting.dim_child dchild
where dchild.case_no = '279888' and dchild.suffix = 'C' --723882

select * from reporting.fact_case_child_status
where dim_child_id = 691619
order by end_dt desc

/*
    Where do I begin? it's always the hardest part of expression.
    There are so many emotions that I haven't realized or understood, which
    all requires meditation and reflection. Clarity and peace are necessary
    for understanding myself and my environment. 