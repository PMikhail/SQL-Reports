select count(fcs.dim_child_id) as total_children, 
    count(co.dim_child_id_cyd) as cyd_children,
    count(fcs.dim_child_id) - count(co.dim_child_id_cyd) as non_cyd_children, 
    range_name
from reporting.fact_child_service fcs
inner join --reporting.conf_date_range RR on
(
    select fccs.dim_child_id,
           case when  status_long_desc = 'Open for CYD Service' then fccs.dim_child_id else null end as dim_child_id_cyd,
            range_name, start_dt, RR.end_dt
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
    --where status_long_desc = 'Open for CYD Service'
) CO on fcs.dim_child_id = CO.dim_child_id
inner join reporting.dim_service dserv on
dserv.dim_service_id = fcs.dim_service_id
where (fcs.EFF_DT <= CO.start_dt
  And fcs.END_DT >= CO.start_dt) Or
  (fcs.EFF_DT >= CO.start_dt And
  fcs.EFF_DT <= CO.end_dt)
  group by range_name;
  
  
select fsc.dim_child_id, fsc.dim_shared_case_type_id 
from reporting.fact_shared_child fsc
where dim_shared_case_type_id <> 0;



select count(dim_child_id) 
from reporting.fact_child_service fcs
inner join reporting.dim_status dstat on
DSTAT.DIM_STATUS_ID = fcs.dim_status_id
where status_cd = 'CO' and
dim_shared_case_type_id <> 0;

select * from reporting.conf_date_range
where range_name >= 'FY2013'
and range_name <= 'FY2015'
and range_type = 'FY';

select distinct placement_category from reporting.dim_service;


select count(plcm_child),
    count(clinhome_child),
    count(s_child),
    count(p_child),
    count(unknown_child),
    count(other_child),
    count(non_child),
    range_name
    from
(select fcs.dim_child_id, placement_category, range_name,
    case when placement_category = 'PLCM' then fcs.dim_child_id else null end as plcm_child,
    case when placement_category = 'CH_INHOME' then fcs.dim_child_id else null end as clinhome_child,
    case when placement_category = 'S-?????' then fcs.dim_child_id else null end as s_child,
    case when placement_category = 'P-?????' then fcs.dim_child_id else null end as p_child,
    case when placement_category = 'UNKNOWN' then fcs.dim_child_id else null end as unknown_child,
    case when placement_category = 'OTHER' then fcs.dim_child_id else null end as other_child,
    case when placement_category = 'NON_PLCM' then fcs.dim_child_id else null end as non_child
from reporting.fact_child_service fcs
inner join --reporting.conf_date_range RR on
(
    select fccs.dim_child_id,
            range_name, start_dt, RR.end_dt
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
) CO on fcs.dim_child_id = CO.dim_child_id
inner join reporting.dim_service dserv on
dserv.dim_service_id = fcs.dim_service_id
where dep_dlq = 'DEP'
  And (fcs.EFF_DT <= CO.start_dt
  And fcs.END_DT >= CO.start_dt) Or
  (fcs.EFF_DT >= CO.start_dt And
  fcs.EFF_DT <= CO.end_dt))
  group by range_name
  order by range_name;


select dim_service_id
from reporting.dim_service
where placement_category = 'P-?????';

select count(dim_child_id)
from reporting.fact_child_service fcs
inner join reporting.dim_service dserv
on fcs.dim_service_id = dserv.dim_service_id
--where fcs.dim_service_id = 55 or
--fcs.dim_service_id = 56
where dep_dlq = 'DEP'
and (fcs.EFF_DT <= to_date('7/1/2012','mm/dd/yyyy')
  And fcs.END_DT >= to_date('7/1/2012','mm/dd/yyyy')) Or
  (fcs.EFF_DT >= to_date('7/1/2012','mm/dd/yyyy') And
  fcs.EFF_DT <= to_date('6/30/2013','mm/dd/yyyy'));

select * from reporting.fact_child_service;