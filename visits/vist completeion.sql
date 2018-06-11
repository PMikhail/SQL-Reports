select count(*)
from
(
    select nvl(service_group_name, 'No Service') as service_type, case_name, child_id, child_full_nm,
    birth_dt, age, age_end_month, last_visit, number_of_months, dir_name, adm_name, sup_name, wrk_name,
    dir_last_name, adm_last_name, sup_last_name, wrk_last_name
    from
    (
        select case_name, child_id, child_full_nm, to_char(trunc(dchild.brth_dt), 'mm/dd/yyyy') as birth_dt, age_end_month, service_group_name,
        to_char(trunc(visits.end_dt), 'mm/dd/yyyy') as last_visit, dwork.full_name as dir_name, dwork.last_name as dir_last_name,
        awork.full_name as adm_name, awork.last_name as adm_last_name, swork.full_name as sup_name,
        swork.last_name as sup_last_name, wwork.full_name as wrk_name, wwork.last_name as wrk_last_name,
        trunc(months_between(sysdate, dchild.brth_dt)/12) as age, number_of_months,
        case when cua_cd_id <> 0 and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and number_of_months < 1 and age_end_month <= 5 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'No Service' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Subsidy' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name like '%Hospital%' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name like '%Runaway' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Placement baby' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Counseling-Nonplacement' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name like '%Day%' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name like 'Zero%' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Inhome' and number_of_months < 3 and age_end_month > 5 then 'Complete'
            when cua_cd_id = 0 and service_group_name like 'Aftercare%' and number_of_months < 3 and age_end_month > 5 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Placement' and number_of_months <= 5 and age_end_month > 5 then 'Complete'
            else 'Incomplete'
        end as visit_status
        /*
        case when dpos.cua_cd_id <> 0 and number_of_months < 1
            or dpos.cua_cd_id = 0 and number_of_months < 1
            or dpos.cua_cd_id = 0 and number_of_months < 3 and age_end_month > 5
            or dpos.cua_cd_id = 0 and number_of_months <= 5 and age_end_month > 5 
                and service_group_name = 'Placement'
            then 'Complete'
            else 'Incomplete'
        end as visit_completion
        */
        --visits.dim_child_id, dchild.*, HIER.DIM_CHILD_ID
        from reporting.fact_case_child_status_hier hier
        left join
        (   /*
                FACT_VISIT_PARTY filtered by lastvist as of reporting date (SYSDATE)
            */
            select * from
                (
                    select dim_child_id, end_dt, dcase.case_name,
                    row_number() over(partition by fvp.dim_child_id order by fvp.end_dt desc) as rn,
                    trunc(months_between(SYSDATE, end_dt)) as number_of_months
                    from reporting.fact_visit_party fvp
                    inner join reporting.dim_case dcase on
                    dcase.dim_case_id = fvp.dim_case_id
                    where fvp.end_dt < SYSDATE
                )
            where rn = 1
        ) visits on
        visits.dim_child_id = hier.dim_child_id
        inner join
        (   /*
                DIM_ASSIGNMENT_TYPE to filter children in CUA, NAPP (N/A), and UNKNOWN
            */
            select atype.dim_assignment_type_id , queue_cd ,assignment_type_cd                
            from reporting.dim_assignment_type atype
            where queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN'
        ) atype on
        atype.dim_assignment_type_id = hier.dim_assignment_type_id
        inner join 
        (   /*
                get necessary info from DIM_CHILD
            */
            select case_no, suffix, brth_dt, last_nm, first_nm, dim_child_id,
            (dchild.case_no || '-' || dchild.suffix) as child_id,
            (dchild.first_nm || ' ' || dchild.last_nm) as child_full_nm,
            (trunc((months_between(SYSDATE, brth_dt)+1)/12)) as age_end_month
            from reporting.dim_child dchild
        ) dchild on
        dchild.dim_child_id = hier.dim_child_id
        left join
        (
            select service_group_name, dim_child_id from 
                (
                    select x.dim_child_id, dserv.service_group_name
                    , row_number() over(partition by dim_child_id order by service_group_rank) as rn
                    from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
                    where SYSDATE between eff_dt and end_dt
                )where rn = 1
        ) plc on plc.dim_child_id = visits.dim_child_id
        inner join reporting.dim_status stat on
        hier.dim_status_id = stat.dim_status_id and status_cd = 'CO'
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
        where SYSDATE between hier.eff_dt and hier.end_dt
    )vv
    where visit_status = 'Incomplete'
)
where adm_name = 'Jascinth C Scott-Findley';
where child_id = '222658-C';



select count(*) from reporting.dim_position --17495

select count(*) from reporting.dim_worker --6466
        
case when reporting.dim_position.cua_cd_id <> 0 and number_of_months < 1
    or reporting.dim_position.cua_cd_id = 0 and number_of_months < 1
    or reporting.dim_position.cua_cd_id = 0 and number_of_months < 3 and age_end_month > 5
    or reporting.dim_position.cua_cd_id = 0 and number_of_months <= 5 and age_end_month > 5 
        and service_group_name = 'Placement'
    then 'Complete'
    else 'Incomplete'
end as visit_completion


select distinct assignment_type_cd from reporting.dim_assignment_type
where queue_cd <> ''

