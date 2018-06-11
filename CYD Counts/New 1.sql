select count(fcs.dim_child_id), range_name, count(DISTINCT fcs.dim_child_id)
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
  And ( (fcs.EFF_DT <= CO.start_dt
  And fcs.END_DT >= CO.start_dt) Or
  (fcs.EFF_DT >= CO.start_dt And
  fcs.EFF_DT <= CO.end_dt) )
  group by range_name
  order by range_name;
  
  
  

select count(fcs.dim_child_id)
from reporting.fact_child_service fcs
inner join
(
    select fccs.dim_child_id
    from reporting.fact_case_child_status fccs
    where (fccs.EFF_DT <= to_date('7/1/2012','mm/dd/yyyy')
      And fccs.END_DT >= to_date('7/1/2012','mm/dd/yyyy')) Or
      (fccs.EFF_DT >= to_date('7/1/2012','mm/dd/yyyy') And
      fccs.EFF_DT <= to_date('6/30/2014','mm/dd/yyyy'))
) FC on
FC.dim_child_id = fcs.dim_child_id
inner join reporting.dim_service dserv
on fcs.dim_service_id = dserv.dim_service_id
where placement_category = 'P-?????'
and (fcs.EFF_DT <= to_date('7/1/2012','mm/dd/yyyy')
  And fcs.END_DT >= to_date('7/1/2012','mm/dd/yyyy')) Or
  (fcs.EFF_DT >= to_date('7/1/2012','mm/dd/yyyy') And
  fcs.EFF_DT <= to_date('6/30/2014','mm/dd/yyyy'))
  And placement_category = 'P-?????'
  
select count(fcs.dim_child_id) --590391
from reporting.fact_child_service fcs
inner join reporting.dim_service dserv on
dserv.dim_service_id = fcs.dim_service_id
where dep_dlq = 'DEP'  


select count(dim_child_id_dep),
    count(dim_child_id_dlq)
from (select 
    case when dserv.dep_dlq = 'DEP' then fcs.dim_child_id else
    null end as dim_child_id_dep,
    case when dserv.dep_dlq = 'DLQ' then fcs.dim_child_id else
    null end as dim_child_id_dlq
from reporting.fact_child_service fcs
inner join reporting.dim_service dserv on
dserv.dim_service_id = fcs.dim_service_id
)