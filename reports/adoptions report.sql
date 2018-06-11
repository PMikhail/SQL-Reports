--476

select child_id
     , serv_cd
     , fcs.eff_dt
     , fcs.end_dt 
from fact_child_service fcs
inner join reporting.fact_case_child_status fccs
on FCCS.DIM_CHILD_ID = fcs.dim_child_id
inner join reporting.dim_status ds
on ds.dim_status_id = fccs.dim_status_id
inner join reporting.dim_child dc
on dc.dim_child_id = fcs.dim_child_id
left join fact_adoption fa
on fcs.dim_child_id = fa.dim_child_id
inner join dim_service ds
on DS.DIM_SERVICE_ID = fcs.dim_service_id
where (serv_cd = 'A1GB' or serv_cd = 'A1GG')
and status_cd = 'CO'

select * from reporting.dim_adoption_status


with final_adoption as(
        select dim_child_id
             , case when dim_date_id_finalization = 0 then to_date('1900/1/1', 'yyyy/mm/dd') 
                    else to_date(dim_date_id_finalization, 'yyyy/mm/dd') end as finalization_date
             --, to_date(FA.DIM_DATE_ID_FINALIZATION, 'yyyy/mm/dd') as dim_date_id_finalization
        from reporting.fact_adoption fa
  inner join reporting.dim_adoption_status das
          on das.dim_adoption_status_id = fa.dim_adoption_status_id 
       where record_status_shrt_desc = 'Finalized'
)
, open_children as (
        select child_id
             , fccs.dim_child_id
             , fccs.eff_dt as status_eff
             , fccs.end_dt as status_end
          from reporting.fact_case_child_status fccs
    inner join reporting.dim_status ds
            on ds.dim_status_id = fccs.dim_status_id
    inner join reporting.dim_child dc
            on dc.dim_child_id = fccs.dim_child_id
         where status_cd = 'CO'
)
, child_service as (
        select EFF_DT_SERVICE
             , end_dt_service
             , DIM_CHILD_ID
             , serv_cd
          from reporting.fact_child_service fcs
    inner join reporting.dim_service ds
            on fcs.dim_service_id = ds.dim_service_id
         where serv_cd like 'A%'
)
    select child_id
         , serv_cd
         , eff_dt_service
         , end_dt_service
         , status_end
         , nvl(finalization_date, to_date('1900/1/1', 'yyyy/mm/dd')) as finalization_date
      from open_children oc
inner join child_service cs
        on cs.dim_child_id = oc.dim_child_id
       and cs.eff_dt_service < oc.status_end
       and cs.end_dt_service > oc.status_eff
 left join final_adoption fa
        on fa.dim_child_id = oc.dim_child_id



select * from reporting.fact_adoption where dim_child_id = 129828 or dim_child_id = 47686639

select * from reporting.dim_child where child_id = '150920-A'

select count(*) from (
    select child_id
         , serv_cd
         , fcs.eff_dt_service
         , fcs.end_dt_service
         , fccs.end_dt as status_end
         , 
      from reporting.fact_case_child_status fccs
inner join reporting.fact_child_service fcs
        on fcs.dim_child_id = fccs.dim_child_id
       and fcs.eff_dt_service between fccs.eff_dt and fccs.end_dt
       and fcs.end_dt_service between fccs.eff_dt and fccs.end_dt
inner join reporting.dim_service ds
        on ds.dim_service_id = fcs.dim_service_id
inner join reporting.dim_status ds
        on ds.dim_status_id = fccs.dim_status_id
inner join reporting.dim_child dc
        on dc.dim_child_id = fcs.dim_child_id
  left join reporting.fact_adoption fa
        on fa.dim_child_id = fccs.dim_child_id
       and fa.
     where status_cd = 'CO'
       and (serv_cd = 'A1GB' or serv_cd = 'A1GG')
)


select * from fact_child_service

select * from fact_case_child_status



select distinct dim_date_id_cancel from fact_adoption order by 1



select * from dim_adoption_status

select * from REPORTING.FACT_CHILD_SERVICE

select * from fact_case_child_

select distinct serv_cd from reporting.dim_service order by 1