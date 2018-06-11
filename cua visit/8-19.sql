select count(*)
from
(
select nvl(service_group_name, 'No Service') as service_type, number_of_months, queue_cd, adopt_dt, last_visit, dim_child_id, visit_status
from
(
    select age_end_month, service_group_name, to_char(trunc(visits.end_dt), 'mm/dd/yyyy') as last_visit, queue_cd, adopt_dt, number_of_months, hier.dim_child_id,
    case when cua_cd_id <> 0 and number_of_months < 1 then 'Complete' else 'Incomplete' end as visit_status
    from reporting.fact_case_child_status_hier hier
    left join
    (--FACT_VISIT_PARTY FILTERED BY LAST_VISIT
        select * from
        (
            select dim_child_id, end_dt,
            row_number() over(partition by dim_child_id order by end_dt desc) as rn,
            trunc(months_between(to_date('5/31/2015', 'mm/dd/yyyy'), end_dt)) as number_of_months
            from reporting.fact_visit_party fvp
            where fvp.end_dt <= to_date('5/31/2015', 'mm/dd/yyyy')
        )
        where rn = 1
    ) visits on
    visits.dim_child_id = hier.dim_child_id
    inner join
    (--get DIM_CHILD
        select child_id, brth_dt, dim_child_id,
        (first_nm || ' ' || last_nm) as child_full_nm,
        (trunc((months_between(to_date('5/31/2015', 'mm/dd/yyyy'), brth_dt)+1)/12)) as age_end_month
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
            where to_date('5/31/2015', 'mm/dd/yyyy') between eff_dt and end_dt
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
    POS.DIM_POSITION_ID = HIER.worker_dim_position_id
    inner join reporting.dim_case dcase on
    dcase.dim_case_id = hier.dim_case_id
    inner join reporting.dim_assignment_type atype on
    ATYPE.DIM_ASSIGNMENT_TYPE_ID = HIER.DIM_ASSIGNMENT_TYPE_ID
    inner join reporting.dim_status dstat on
    DSTAT.DIM_STATUS_ID = HIER.DIM_STATUS_ID        
    where to_date('5/31/2015', 'mm/dd/yyyy') between hier.eff_dt and hier.end_dt
--    ( (hier.eff_dt < to_date('5/1/2015', 'mm/dd/yyyy') and hier.end_dt > to_date('5/1/2015', 'mm/dd/yyyy')) or
--    (hier.eff_dt > to_date('5/1/2015', 'mm/dd/yyyy') and hier.eff_dt < to_date('5/31/2015', 'mm/dd/yyyy')) )
--    and (queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN')
    and status_cd = 'CO'
    and cua_cd_id <> 0
    --and cua_cd_id <> 0
)

)
where visit_status = 'Incomplete'
--where dir_name like '%Deegan'
--group by dhs_cua, dir_name, dir_last_name, visit_status
--order by dhscua, dir_last_name

