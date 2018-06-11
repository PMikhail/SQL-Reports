select sup_name, sup_last_name, visit_status_incomplete, visit_status_complete, count(*)
from
(
select nvl(service_group_name, 'No Service') as service_type, case_name, child_id, child_full_nm,
birth_dt, age, age_end_month, dir_name, adm_name, sup_name, wrk_name,
case when num_visits < 4 then 'Incomplete' else null end as visit_status_incomplete,
case when num_visits >= 4 then 'Complete' else null end as visit_status_complete, last_visit,
dir_last_name, adm_last_name, sup_last_name, wrk_last_name, queue_cd
from
(
    select case_name, child_id, child_full_nm, to_char(trunc(plc.brth_dt), 'mm/dd/yyyy') as birth_dt, age_end_month, service_group_name,
    dwork.full_name as dir_name, dwork.last_name as dir_last_name, last_visit,
    awork.full_name as adm_name, awork.last_name as adm_last_name, swork.full_name as sup_name,
    swork.last_name as sup_last_name, wwork.full_name as wrk_name, wwork.last_name as wrk_last_name, queue_cd,
    trunc(months_between(sysdate, plc.brth_dt)/12) as age,
    nvl(visits_num, 0) as num_visits    
    --case when visit_diff > 7 then 'Incomplete' else 'Complete' end as visit_status
    from reporting.fact_case_child_status_hier hier
    left join
    (--FACT_VISIT_PARTY to count visits in month
        select *from
        (
            select dim_child_id, count(end_dt) as visits_num
            from reporting.fact_visit_party fvp
            where extract(month from fvp.end_dt) = extract(month from sysdate)
            and extract(year from end_dt) = extract(year from sysdate)
            --and to_date(end_dt, 'mm/yyyy') = to_date(sysdate,'mm/yyyy')
            group by dim_child_id
        )
    ) visits on
    visits.dim_child_id = hier.dim_child_id
    inner join
    (
        select * from
        (
            select dim_child_id, trunc(end_dt) as last_visit,
            row_number() over(partition by dim_child_id order by end_dt desc) as rn
            from reporting.fact_visit_party fvp
            where end_dt < SYSDATE
        )
        where rn = 1
    ) lastv on
    lastv.dim_child_id = hier.dim_child_id
    inner join
    (--get DIM_CHILD and SERVICE
        select child_full_nm, age_end_month, child_id, brth_dt, dchild.dim_child_id,
        service_group_name
        from
        (--get DIM_CHILD
            select child_id, brth_dt, dim_child_id,
            (first_nm || ' ' || last_nm) as child_full_nm,
            (trunc((months_between(SYSDATE, brth_dt)+1)/12)) as age_end_month
            from reporting.dim_child
        ) dchild
        inner join
        (--get SERVICE
            select * from
            (
                select fcs.dim_child_id, dserv.service_group_name,
                row_number() over(partition by fcs.dim_child_id order by service_group_rank) as rn
                from reporting.fact_child_service fcs
                inner join reporting.dim_service dserv on
                dserv.dim_service_id = fcs.dim_service_id
                where SYSDATE between eff_dt and end_dt
                and serv_cd = 'I1BG'
            )
            where rn = 1
        ) serv on
        serv.dim_child_id = dchild.dim_child_id
    ) plc on
    hier.dim_child_id = plc.dim_child_id
    inner join reporting.dim_case dcase on
    dcase.dim_case_id = hier.dim_case_id
    inner join reporting.dim_assignment_type atype on
    ATYPE.DIM_ASSIGNMENT_TYPE_ID = HIER.DIM_ASSIGNMENT_TYPE_ID
    and (queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN')
    inner join reporting.dim_status dstat on
    DSTAT.DIM_STATUS_ID = HIER.DIM_STATUS_ID and status_cd = 'CO'        
    inner join reporting.dim_position dpos on
    dpos.dim_position_id = hier.worker_dim_position_id
    inner join reporting.dim_worker wwork on
    wwork.dim_worker_id = worker_dim_worker_id 
    inner join reporting.dim_worker swork on
    swork.dim_worker_id = supervisor_dim_worker_id 
    inner join reporting.dim_worker awork on
    awork.dim_worker_id = admin_dim_worker_id
    inner join reporting.dim_worker dwork on 
    dwork.dim_worker_id = director_dim_worker_id
    where sysdate between hier.eff_dt and hier.end_dt
)
)
where adm_name = 'Jennifer S Kaba'
group by sup_name, sup_last_name, visit_status_incomplete, visit_status_complete
order by sup_last_name asc


/*
select * from reporting.dim_child dchild
left join
(
    select dim_child_id, count(end_dt) as num_visits
    from reporting.fact_visit_party
    where extract(month from fvp.end_dt) = extract(month from sysdate)--to_date('7/15/2015', 'mm/dd/yyyy'))
    group by dim_child_id
    
) fvp on
fvp.dim_child_id = dchild.dim_child_id
where child_id = '618408-C'

select fvp.end_dt from reporting.dim_child dchild
left join reporting.fact_visit_party fvp on
fvp.dim_child_id = dchild.dim_child_id
where child_id = '618408-C'
order by end_dt desc

*/