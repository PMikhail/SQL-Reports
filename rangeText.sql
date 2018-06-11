SELECT distinct range_type,case when RANGE_TYPE = 'CY' then 'Calendar Year'
            when RANGE_TYPE = 'CY-M' then 'Calendar Year - Month'
            when RANGE_TYPE = 'CY-Q' then 'Calendar Year - Quarter'
            when RANGE_TYPE = 'FY' then 'Fiscal Year'
            when RANGE_TYPE = 'FY-Q' then 'Fiscal Year - Quarter' end as RANGE_TEXT
FROM REPORTING.CONF_DATE_RANGE;

Select Distinct RANGE_NAME From reporting.conf_date_range 
where range_name > 'CY2011'
and range_name not like 'FY%'
order by 1;

--Code for STATUS TIME SERIES test comparison
select fccs.dim_child_id, range_name
from reporting.fact_case_child_status fccs
inner join reporting.dim_status dstat on
fccs.dim_status_id = dstat.dim_status_id
inner join --reporting.conf_date_range RR on

(
    select *
    from reporting.conf_date_range
    where range_name >= 'FY2013' --rangeStart
    and range_name <= 'FY2015' --rangeEnd
    and range_type = 'FY'--rangeType
) RR on 
RR.start_dt <= case when fccs.end_dt > sysdate
then sysdate else fccs.end_dt end
and RR.end_dt >= fccs.eff_dt
where status_long_desc = 'Open for CYD Service'
or status_long_desc = 'Open for non-CYD Service';



----Code for DEP_DLQ TIME SERIES
/*
select count( fcs.dim_child_id) as total_children, 
    count( dim_child_id_cyd) as cyd_children,
    count( fcs.dim_child_id) - count( dim_child_id_cyd) as non_cyd_children, 
    range_name
from reporting.fact_child_service fcs
inner join --reporting.conf_date_range RR on
(
*/
select (count(dim_child_id_cyd) + count(dim_child_id_noncyd)) as total,
count(dim_child_id_cyd),
count(dim_child_id_noncyd),
range_name
from
(   
    select fccs.dim_child_id,
           case when status_cd = 'CO' then fccs.dim_child_id else null end as dim_child_id_cyd,
           case when status_cd = 'CS' then fccs.dim_child_id else null end as dim_child_id_noncyd,
           range_name
    from reporting.fact_case_child_status fccs
    inner join reporting.dim_status dstat on
    fccs.dim_status_id = dstat.dim_status_id
    inner join --reporting.conf_date_range RR on
    (
        select *
        from reporting.conf_date_range
        where range_name >= 'FY2013' --rangeStart
        and range_name <= 'FY2015' --rangeEnd
        and range_type = 'FY'--rangeType
    ) RR on 
    --((RR.start_dt >= fccs.eff_dt and RR.start_dt <= fccs.end_dt)
     --OR (RR.start_dt <= fccs.eff_dt and RR.end_dt >= fccs.eff_dt))
    RR.start_dt <= case when fccs.end_dt > sysdate
    then sysdate else fccs.end_dt end
    and RR.end_dt >= fccs.eff_dt
     --RR.end_dt <= fccs.end_dt
) CO
group by range_name
order by range_name;


select count(fccs.dim_child_id),
           range_name
    from reporting.fact_case_child_status fccs
    inner join reporting.dim_status dstat on
    fccs.dim_status_id = dstat.dim_status_id
    inner join --reporting.conf_date_range RR on
    (
        select *
        from reporting.conf_date_range
        where range_name >= 'FY2013' --rangeStart
        and range_name <= 'FY2015' --rangeEnd
        and range_type = 'FY'--rangeType
    ) RR on 
    --((RR.start_dt >= fccs.eff_dt and RR.start_dt <= fccs.end_dt)
     --OR (RR.start_dt <= fccs.eff_dt and RR.end_dt >= fccs.eff_dt))
    RR.start_dt <= case when fccs.end_dt > sysdate
    then sysdate else fccs.end_dt end
    and RR.end_dt >= fccs.eff_dt
    where status_cd = 'CO' or status_cd = 'CS'
    group by range_name
    order by range_name;



select * from reporting.conf_date_range
where start_dt >= to_date('7/1/2013', 'mm/dd/yyyy')
and end_dt <= to_date('7/1/2014', 'mm/dd/yyyy')
and range_type = 'FY';

select * from reporting.conf_date_range
where range_name >= 'FY2013'
and range_name <= 'FY2015'
and range_type = 'FY';

select count(fccs.dim_child_id)--, dstat.status_long_desc as status
from reporting.fact_case_child_status fccs
inner join reporting.dim_status dstat on
fccs.dim_status_id = dstat.dim_status_id
where (status_long_desc = 'Open for CYD Service') 
--or status_long_desc = 'Open for non-CYD Service')
and (((Trunc(FCCS.EFF_DT) <= To_Date('7/1/2013', 'mm/dd/yyyy'))
    And (Trunc(FCCS.END_DT) >= To_Date('7/1/2013', 'mm/dd/yyyy'))) Or
    ((Trunc(FCCS.EFF_DT) >= To_Date('7/1/2013', 'mm/dd/yyyy')) And
    (Trunc(FCCS.EFF_DT) <= To_Date('7/1/2014', 'mm/dd/yyyy'))))
    

--Code for PLACEMENT_CATEGORY TIME SERIES