select dhs_cua, dir_name, dir_last_name, visit_status--dir_name, visit_status, count(*)select DHS_CUA, dir_name, dir_last_name, visit_status
from
(
select visit_status, dhs_cua, dir_name, dir_last_name, child_id
from
(
    select dwork.last_name as dir_last_name, number_of_months, child_id,
    case when pos.cua_cd_id = 0 then 'DHS' else 'CUA' end as dhs_cua,
    case when pos.cua_cd_id = 0 then dwork.full_name else 'CUA' || substr(to_char(pos.cua_cd_id), -1) || ' - ' || dwork.full_name  end as dir_name,
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
        select dim_child_id, child_id,
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
    inner join reporting.dim_worker dwork on 
    dwork.dim_worker_id = director_dim_worker_id 
    where sysdate between hier.eff_dt and hier.end_dt
    and (queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN')
    and status_cd = 'CO'
)
--where dir_name = 'Darlene E Adams'
) top
--left join top b on a.child_id = b.child_id
order by dhs_cua desc, case when dir_name like 'CUA%' then dir_name else dir_last_name end



--12773
--12664