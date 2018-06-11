
select dim_provider_id, dim_document_type_id, eff_dt, end_dt, dim_yn_id_is_licensed
     , case when dim_yn_id_is_licensed = 1 then DD1.DIM_DATE_ID else 0 end as dim_date_id_licensed_from
     , case when dim_yn_id_is_licensed = 1 then DD2.DIM_DATE_ID else 0 end as dim_date_id_licensed_to
     , row_number() over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as rn
from 
(select dim_provider_id, dim_document_type_id, eff_dt, end_dt, dim_yn_id_is_licensed
     , nvl(lic_end_dt, next_lic_end_dt) as dim_lic_end_dt
     , nvl(lic_start_dt, prev_lic_start_dt) as dim_lic_eff_dt 
  from (
        select pl4.*
             , lag(lic_start_dt) over(partition by dim_provider_id, dim_document_type_id order by rn) as prev_lic_start_dt
             , lead(lic_end_dt) over(partition by dim_provider_id, dim_document_type_id order by rn) as next_lic_end_dt
            from (                
                select pl3.*
                     , case when prev_end_dt is null or prev_end_dt < eff_dt - 1 then eff_dt else null end as lic_start_dt
                     , case when end_dt > next_eff_dt then null -- overlapping license
                            when end_dt = to_date('12/31/9999', 'mm/dd/yyyy') then end_dt
                            when next_eff_dt is null or next_eff_dt > end_dt + 1 then end_dt else null end as lic_end_dt
                  from (                
                    select dim_provider_id, dim_document_type_id, eff_dt, end_dt, dim_yn_id_is_licensed 
                         , lag(end_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as prev_end_dt
                         , lag(eff_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as prev_eff_dt
                         , lead(end_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as next_end_dt
                         , lead(eff_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as next_eff_dt
                         , row_number() over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt desc ) as rn
                         from fact_provider_licensure fl
                       ) pl3
                 )pl4
                 where lic_start_dt is not null or lic_end_dt is not null
        )pl5
)pl6   
inner join dim_date dd1 --JOINS TO GET DIM_DATE_ID_LICENSED_FROM AND TO
on dd1.dt = dim_lic_eff_dt
inner join dim_date dd2
on dd2.dt = dim_lic_end_dt
order by dim_provider_id, dim_document_type_id, eff_dt    