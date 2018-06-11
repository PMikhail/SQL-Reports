select * from reporting.conf_date_range;

select * from reporting.conf_date_range
where start_dt = 'CY2011'.start_dt;

select Distinct RANGE_TYPE From reporting.conf_date_range Order By 1;

select Distinct RANGE_NAME From reporting.conf_date_range;

select range_name, count(distinct dim_child_id)
from reporting.fact_child_service fcs
inner join reporting.conf_date_range cdr on
cdr.start_dt <= case when fcs.end_dt > sysdate then sysdate else fcs.end_dt end and cdr.end_dt >= fcs.eff_dt
where range_type = 'FY'
group by range_name
order by 1;

select fccs.dim_child_id, range_name
from reporting.fact_case_child_status fccs
inner join reporting.dim_status dstat on
fccs.dim_status_id = dstat.dim_status_d
inner join reporting.conf_date_range cdr on
cdr.start_dt <= case when fccs.end_dt > sysdate 
then sysdate else fccs.end_dt end
and cdr.end_dt >= fccs.eff_dt
where status_long_desc = 'Open for CYD Service'
    or status_long_desc = 'Open for non-CYD Service'
and range_type = 'CY'
and cdr.start_dt = '@R'
and cdr.end_dt = '@Request.islRangeEnd~'
group by range_name;

select *
from reporting.conf_date_range
where range_name >= 'CY2011' and range_name <= 'CY2015'
and range_type = 'CY';  


select * from reporting.conf_date_range;
select count( fccs.dim_child_id) 
from reporting.fact_case_child_status fccs 
inner join reporting.dim_status dst on
fccs.dim_status_id = dst.dim_status_id
where status_cd = 'CO' and
(Trunc(fccs.EFF_DT) <= To_Date('7/16/2015', 'mm/dd/yyyy')
And Trunc(fccs.END_DT) >= To_Date('7/16/2015', 'mm/dd/yyyy')) Or
(Trunc(fccs.EFF_DT) >= To_Date('7/16/2015', 'mm/dd/yyyy') And
Trunc(fccs.EFF_DT) <= To_Date('7/16/2015', 'mm/dd/yyyy'));