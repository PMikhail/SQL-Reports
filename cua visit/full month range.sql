select sum( case when visit_status = 'Complete' then 1 else 0 end) / count(*) * 100 as visit_ratio, cua_cd_id

from
(
select nvl(service_group_name, 'No Service') as service_type, case_name, child_id, child_full_nm, dim_child_id,
birth_dt, age, age_end_month, number_of_months, adm_name, sup_name, wrk_name, visit_status, dir_worker_id,
nvl(last_visit, 'No Visits') as last_visit, adm_last_name, sup_last_name, wrk_last_name, adopt_dt, dir_name, cua_cd_id
from
(
    select case_name, child_id, child_full_nm, to_char(trunc(dchild.brth_dt), 'mm/dd/yyyy') as birth_dt, age_end_month, service_group_name,
    to_char(trunc(visits.end_dt), 'mm/dd/yyyy') as last_visit, dwork.last_name as director_last_name,
    dwork.dim_worker_id as dir_worker_id,
    awork.full_name as adm_name, awork.last_name as adm_last_name, swork.full_name as sup_name,
    swork.last_name as sup_last_name, wwork.full_name as wrk_name, wwork.last_name as wrk_last_name, queue_cd, adopt_dt,
    trunc(months_between(sysdate, dchild.brth_dt)/12) as age, number_of_months, hier.dim_child_id,
    'CUA' || substr(to_char(pos.cua_cd_id), -1) || ' - ' || dwork.full_name  as dir_name, pos.cua_cd_id,
    case when dpos.cua_cd_id <> 0 and number_of_months < 1 then 'Complete' else 'Incomplete' end as visit_status
    from reporting.fact_case_child_status_hier hier
    left join
    (--FACT_VISIT_PARTY FILTERED BY LAST_VISIT
        select * from
        (
            select dim_child_id, end_dt,
            row_number() over(partition by dim_child_id order by end_dt desc) as rn,
            trunc(months_between(to_date('5/31/2015','mm/dd/yyyy'), end_dt)) as number_of_months
            from reporting.fact_visit_party fvp
            where fvp.end_dt < to_date('5/31/2015','mm/dd/yyyy')
        )
        where rn = 1
    ) visits on
    visits.dim_child_id = hier.dim_child_id
    inner join
    (--get DIM_CHILD
        select child_id, brth_dt, dim_child_id,
        (first_nm || ' ' || last_nm) as child_full_nm,
        (trunc((months_between(to_date('5/31/2015','mm/dd/yyyy'), brth_dt)+1)/12)) as age_end_month
        from reporting.dim_child
    ) dchild on
    dchild.dim_child_id = hier.dim_child_id
    left join
    (--get SERVICE
        select * from
        (
            select fcs.dim_child_id, dserv.service_group_name,
            row_number() over(partition by fcs.dim_child_id order by service_group_rank) as rn
            from reporting.fact_child_service fcs
            inner join reporting.dim_service dserv on
            dserv.dim_service_id = fcs.dim_service_id
            where to_date('5/31/2015','mm/dd/yyyy') between eff_dt and end_dt
        )
        where rn = 1
    ) serv on
    hier.dim_child_id = serv.dim_child_id
    left join
    (
        select dim_child_id, dt as adopt_dt--, dim_case_id
        from reporting.fact_adoption adopt
        inner join reporting.dim_date ddate on
        adopt.dim_date_id_finalization = ddate.dim_date_id
    ) adopt on
    adopt.dim_child_id = hier.dim_child_id
    left join reporting.dim_position pos on
    POS.DIM_POSITION_ID = HIER.DIRECTOR_DIM_POSITION_ID
    inner join reporting.dim_case dcase on
    dcase.dim_case_id = hier.dim_case_id
    inner join reporting.dim_assignment_type atype on
    ATYPE.DIM_ASSIGNMENT_TYPE_ID = HIER.DIM_ASSIGNMENT_TYPE_ID
    inner join reporting.dim_status dstat on
    DSTAT.DIM_STATUS_ID = HIER.DIM_STATUS_ID        
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
    where ( (hier.EFF_DT <= to_date('5/1/2015','mm/dd/yyyy')
            And hier.END_DT >= to_date('5/1/2015','mm/dd/yyyy')) Or
            (hier.EFF_DT >= to_date('5/1/2015','mm/dd/yyyy') And
            hier.EFF_DT <= to_date('5/31/2015','mm/dd/yyyy')) )
    and (queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN')
    and status_cd = 'CO'
    and adopt_dt is null
)
where cua_cd_id <> 0
)
group by cua_cd_id
order by cua_cd_id
--where visit_status = 'Complete'
--where dir_name like '%Deegan'
--group by dhs_cua, dir_name, dir_last_name, visit_status
--order by dhs_cua, dir_last_name