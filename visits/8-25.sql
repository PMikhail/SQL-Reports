select dhs_cua, count(*)
from
(
    select service_type, case_name, child_id, child_full_nm, birth_dt, age, age_end_month,
    number_of_months, adm_name, sup_name, wrk_name, dhs_cua, dir_name, dir_worker_id, last_visit,
    dir_last_name, adm_last_name, sup_last_name, wrk_last_name, queue_cd, adopt_dt,
    visit_complete, visit_incomplete
    from
    (
        select nvl(service_group_name, 'No Service') as service_type, case_name, child_id, child_full_nm,
        birth_dt, age, age_end_month, number_of_months, adm_name, sup_name, wrk_name, dhs_cua, dir_name, dir_worker_id,
        nvl(last_visit, 'No Visits') as last_visit, dir_last_name, adm_last_name, sup_last_name, wrk_last_name, queue_cd, adopt_dt, visit_complete,
        case when visit_complete = 1 then 0 else 1 end as visit_incomplete
        from
        (
            select case_name, child_id, child_full_nm, to_char(trunc(dchild.brth_dt), 'mm/dd/yyyy') as birth_dt, age_end_month, service_group_name,
            to_char(trunc(visits.end_dt), 'mm/dd/yyyy') as last_visit, dwork.dim_worker_id as dir_worker_id,
            awork.full_name as adm_name, awork.last_name as adm_last_name, swork.full_name as sup_name,
            swork.last_name as sup_last_name, wwork.full_name as wrk_name, wwork.last_name as wrk_last_name, queue_cd, adopt_dt,
            trunc(months_between(sysdate, dchild.brth_dt)/12) as age, number_of_months,
            case when dpos.cua_cd_id <> 0 then 'CUA' || substr(to_char(pos.cua_cd_id), -1) else dwork.last_name end as dir_last_name,
            case when pos.cua_cd_id = 0 then 'DHS' else 'CUA' end as dhs_cua,
            case when pos.cua_cd_id = 0 then 'DHS - ' || dwork.full_name else 'CUA' || substr(to_char(pos.cua_cd_id), -1) || ' - ' || dwork.full_name  end as dir_name,
            case when dpos.cua_cd_id <> 0 and number_of_months < 1 then 1  
               when dpos.cua_cd_id = 0 and number_of_months < 1 and age_end_month <= 5 then 1
               when dpos.cua_cd_id = 0 and service_group_name = 'No Service' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name = 'Subsidy' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name like '%Hospital%' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name like '%Runaway' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name = 'Placement baby' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name = 'Counseling-Nonplacement' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name like '%Day%' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name like 'Zero%' and number_of_months < 1 then 1
               when dpos.cua_cd_id = 0 and service_group_name = 'Inhome' and number_of_months < 3 and age_end_month > 5 then 1
               when dpos.cua_cd_id = 0 and service_group_name like 'Aftercare%' and number_of_months < 3 and age_end_month > 5 then 1              
               when dpos.cua_cd_id = 0 and service_group_name = 'Placement' and number_of_months <= 5 and age_end_month > 5 then 1
               else 0
            end as visit_complete
            from reporting.fact_case_child_status_hier hier
            left join
            (--FACT_VISIT_PARTY FILTERED BY LAST_VISIT
                select * from
                (
                    select dim_child_id, end_dt,
                    row_number() over(partition by dim_child_id order by end_dt desc) as rn,
                    trunc(months_between(SYSDATE, end_dt)) as number_of_months
                    from reporting.fact_visit_party fvp
                    where fvp.end_dt < SYSDATE
                )
                where rn = 1
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
            where sysdate between hier.eff_dt and hier.end_dt
            and (queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN')
            and status_cd = 'CO'
        )
        --where dir_name = 'Darlene E Adams'
    )
    --where visit_status = 'Complete'
    --12773
    --12664
)
group by dhs_cua

to_date(