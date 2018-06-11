select count(*) from
(
    select nvl(service_group_name, 'No Service'), case_name, child_id, child_full_nm,
    brth_dt, age_end_month, end_dt, number_of_months
    from
    (
        select case_name, child_id, child_full_nm, brth_dt, age_end_month, service_group_name,
        visits.end_dt,
        trunc(months_between(to_date('7/29/2015', 'mm/dd/yyyy'), dchild.brth_dt)/12) as age, number_of_months,
        case when cua_cd_id <> 0 and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and number_of_months < 1 and age_end_month <= 5 then 'Complete'
            when cua_cd_id = 0 and service_group_name is null and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Subsidy' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and (service_group_name = 'Hospital' or service_group_name = 'DLQ Hospital') and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and (service_group_name = 'Runaway' or service_group_name = 'DLQ Runaway') and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Placement baby' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Counseling-Nonplacement' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and (service_group_name = 'Daycare-Nonplacement' or service_group_name = 'Daytreatment-Nonplacement') and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Zero Rate CUA' and number_of_months < 1 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Inhome' and number_of_months < 3 and age_end_month > 5 then 'Complete'
            when cua_cd_id = 0 and service_group_name = 'Aftercare-Nonplacement' and number_of_months < 3 and age_end_month > 5 then 'Complete'
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
                    trunc(months_between(to_date('7/29/2015', 'mm/dd/yyyy'), end_dt)) as number_of_months
                    --(('2015' - to_char(end_dt,'yyyy')) * 12) + ('08' - to_char(end_dt,'MM')) as number_of_months
                    from reporting.fact_visit_party fvp
                    inner join reporting.dim_case dcase on
                    dcase.dim_case_id = fvp.dim_case_id
                    where fvp.end_dt < to_date('7/29/2015', 'mm/dd/yyyy')
                )
            where rn = 1
        ) visits on
        visits.dim_child_id = hier.dim_child_id
        inner join
        (   /*
                DIM_ASSIGNMENT_TYPE to filter children in CUA, NAPP (N/A), and UNKNOWN
            */
            select atype.dim_assignment_type_id                  
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
            (trunc(months_between(to_date('7/29/2015', 'mm/dd/yyyy'), brth_dt)/12)) as age_end_month
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
                    where to_date('7/29/2015', 'mm/dd/yyyy') between eff_dt and end_dt
                )where rn = 1
        ) plc on plc.dim_child_id = visits.dim_child_id
        inner join reporting.dim_status stat on
        hier.dim_status_id = stat.dim_status_id and status_cd = 'CO'
        inner join reporting.dim_position dpos on
        dpos.dim_position_id = hier.worker_dim_position_id
        where to_date('7/29/2015', 'mm/dd/yyyy') between hier.eff_dt and hier.end_dt
    )vv
    where visit_status = 'Incomplete'
);
where child_id = '130235-H';
--where child_id = '149758-G';

select to_char(to_date('8/3/2015','mm/dd/yyyy'), 'M') - to_char(to_date('7/3/2015','mm/dd/yyyy'), 'M') 

select * from REPORTING.DIM_CASE where case_no = '130235';

select * from reporting.dim_child dchild where DCHILD.CASE_NO = '130235' --child_id = 51106

select * from reporting.dim_case where case_no = '130235'

select * from reporting.fact_visit_party where dim_case_id = 51106

select * from reporting.fact_case_child_status where dim_child_id = 51106