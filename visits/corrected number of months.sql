select count(*)
from
(
select substr(child_id,1,6) as case_no, substr(child_id, -1) as suffix, this.*
from
(
select hier_eff, nvl(service_group_name, 'No Service') as service_type, case_name, child_id, child_full_nm,
birth_dt, age, age_end_month, number_of_months, adm_name, sup_name, visit_status, dhs_cua, dir_name,
nvl(last_visit, 'No Visits') as last_visit, dir_last_name, adm_last_name, sup_last_name, queue_cd , adopt_dt
from
(
    select hier.eff_dt as hier_eff, case_name, child_id, child_full_nm, to_char(trunc(dchild.brth_dt), 'mm/dd/yyyy') as birth_dt, age_end_month, service_group_name,
    to_char(trunc(visits.end_dt), 'mm/dd/yyyy') as last_visit, dwork.last_name as dir_last_name,
    awork.full_name as adm_name, awork.last_name as adm_last_name, swork.full_name as sup_name,
    swork.last_name as sup_last_name, queue_cd,  ddate.dt as adopt_dt,
    trunc(months_between(sysdate, dchild.brth_dt)/12) as age, number_of_months,
    case when pos.cua_cd_id = 0 then 'DHS' else 'CUA' end as dhs_cua,
    case when pos.cua_cd_id = 0 then 'DHS - ' || dwork.full_name else 'CUA' || substr(to_char(pos.cua_cd_id), -1) || ' - ' || dwork.full_name  end as dir_name,
    case when dpos.cua_cd_id <> 0 and number_of_months < 1 then 'Complete'  
       when dpos.cua_cd_id = 0 and number_of_months < 1 and age_end_month <= 5 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name = 'No Service' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name = 'Subsidy' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name like '%Hospital%' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name like '%Runaway' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name = 'Placement baby' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name = 'Counseling-Nonplacement' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name like '%Day%' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name like 'Zero%' and number_of_months < 1 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name = 'Inhome' and number_of_months < 3 and age_end_month > 5 then 'Complete'
       when dpos.cua_cd_id = 0 and service_group_name like 'Aftercare%' and number_of_months < 3 and age_end_month > 5 then 'Complete'              
       when dpos.cua_cd_id = 0 and service_group_name = 'Placement' and number_of_months <= 5 and age_end_month > 5 then 'Complete'
       else 'Incomplete'
    end as visit_status
    from reporting.fact_case_child_status_hier hier
    left join
    (--FACT_VISIT_PARTY FILTERED BY LAST_VISIT
--        select * from
--        (
--            select dim_child_id, end_dt,
--            row_number() over(partition by dim_child_id order by end_dt desc) as rn,
--            ( (to_char(sysdate,'yyyy') - to_char(end_dt,'yyyy')) * 12 +
--            to_char(sysdate,'mm') - to_char(end_dt,'mm') ) as number_of_months
--            from reporting.fact_visit_party fvp
--            inner join reporting.dim_form_source frmsrc on
--            frmsrc.dim_form_source_id = fvp.dim_form_source_id
--            where fvp.end_dt < SYSDATE
--            and ((system_name = 'ECMS' and form_cd in ('SPNIV','SPNOV'))
--            or (system_name = 'VISIT2'))
--        )
--        where rn = 1
         select dim_child_id, end_dt,
            ( (to_char(sysdate,'yyyy') - to_char(end_dt,'yyyy')) * 12 +
            to_char(sysdate,'mm') - to_char(end_dt,'mm') ) as number_of_months
          from (            
            select dim_child_id, max(end_dt) as end_dt
            from reporting.fact_visit_party fvp
            inner join reporting.dim_form_source frmsrc on
            frmsrc.dim_form_source_id = fvp.dim_form_source_id
            where fvp.end_dt < SYSDATE
            and ((system_name = 'ECMS' and form_cd in ('SPNIV','SPNOV'))
            or (system_name = 'VISIT2'))
            group by dim_child_id
          )
    ) visits on
    visits.dim_child_id = hier.dim_child_id
    inner join
    (--get DIM_CHILD
        select child_id, brth_dt, dim_child_id,
        (first_nm || ' ' || last_nm) as child_full_nm,
        (trunc((months_between(SYSDATE, brth_dt)+1)/12)) as age_end_month
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
            where SYSDATE between eff_dt and end_dt
        )
        where rn = 1
    ) serv on
    hier.dim_child_id = serv.dim_child_id
--    left join
--    (
--        select dim_child_id, dt as adopt_dt--, dim_case_id
--        from reporting.fact_adoption adopt
--        inner join reporting.dim_date ddate on
--        adopt.dim_date_id_finalization = ddate.dim_date_id
--    ) adopt on
--    adopt.dim_child_id = hier.dim_child_id
    left join
    reporting.fact_adoption adopt
        on adopt.dim_child_id = hier.dim_child_id
    left join 
        reporting.dim_date ddate on
          adopt.dim_date_id_finalization = ddate.dim_date_id
    left join reporting.dim_position pos on
    POS.DIM_POSITION_ID = HIER.WORKER_DIM_POSITION_ID --changed from director to worker
    inner join reporting.dim_case dcase on
    dcase.dim_case_id = hier.dim_case_id
    inner join reporting.dim_assignment_type atype on
    ATYPE.DIM_ASSIGNMENT_TYPE_ID = HIER.DIM_ASSIGNMENT_TYPE_ID
    inner join reporting.dim_status dstat on
    DSTAT.DIM_STATUS_ID = HIER.DIM_STATUS_ID        
    left join reporting.dim_position dpos on
    dpos.dim_position_id = hier.worker_dim_position_id
    left join reporting.dim_worker dwork on
    dwork.dim_worker_id = worker_dim_worker_id 
    left join reporting.dim_worker swork on
    swork.dim_worker_id = supervisor_dim_worker_id 
    left join reporting.dim_worker awork on
    awork.dim_worker_id = admin_dim_worker_id
--    left join reporting.dim_worker dwork on 
--    dwork.dim_worker_id = director_dim_worker_id
    where to_date('8/19/2015 11:59:59 PM', 'mm/dd/yyyy hh:mi:ss PM') between hier.eff_dt and hier.end_dt
    and (queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN')
    and status_cd = 'CO'
)
--where dir_name = 'Darlene E Adams'
) this
--and child_id = '278011-A'
--and adopt_dt is null
)
where dir_name like 'DHS%'
--order by child_id
--214194-D
--278198-A


select * from reporting.dim_child where child_id = '223610-D'--396163

select * from reporting.dim_child where dim_child_id = 709514

select cua_cd_id from reporting.dim_child dchild
inner join reporting.fact_case_child_status_hier hier on
hier.dim_child_id = dchild.dim_child_id
inner join reporting.fact_visit_party fvp on
fvp.dim_child_id = hier.dim_child_id
inner join reporting.dim_position dpos on
hier.worker_dim_position_id = dpos.dim_position_id
where hier.dim_child_id = 466815
order by hier.end_dt

select * from reporting.fact_visit_party where dim_child_id = 396163

select * from reporting.fact_case_child_status_hier where dim_child_id = 396163 order by end_dt desc

where child_id = '278011-A'
order by end_dt desc
--dim_child_id = 709514

select * from reporting.fact_visit_party where dim_child_id = 709514

select * from reporting.fact_visit_party
where dim_child_id = 7829
order by end_dt desc