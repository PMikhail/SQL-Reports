drop table temp_cont_rate

create table temp_cont_rate
as(
select * from (
select
      srv.id,
      rp.id rateId,
      dim_contract_id
    , dim_service_id
    , case when srv.eff_dt < dc.effective_date 
      then dc.effective_date else srv.eff_dt 
      end as eff_dt   
    , case when SRV.TERM_DT > DC.TERMINATION_DATE 
      then dc.termination_date else srv.term_dt 
      end as end_dt  
    , case when rate is null
      then 0 else age_from
      end as age_from    
    , case when rate is null
      then 999 else age_to
      end as age_to    
    , dim_provider_id
    , dim_agency_id
    , case when auth_req_ind is null then 0
            else 1
      end as dim_yn_id_auth_required    
    , case when rate is null
      then srv.rate_sp 
      else rate
      end as daily_rate    
    , case when rate is null
      then srv.rate_sp else max_maint_rate
      end as daily_rate_max_maint
from dhs_core.dim_contract dc
inner join staging.x_cont_srv srv
on srv.mdoc_no = dc.contract_document_id 
and srv. row_dlt_trnsct_id = 0
left join staging.x_cont_srv_rates_p1 rp
on rp.parnt_id = srv.id
and rp.row_dlt_trnsct_id = 0
inner join dhs_core.dim_service ds
on DS.SERV_CD = SRV.SERV_CD
and nvl(SRV.Rate_CAT, -1) = nvl(DS.Rate_CATEGORY, -1)
and nvl(SRV.EMGCY_IND, 'N') = DS.EMERGENCY_IND
)
where  end_dt > eff_dt
)

/*
inner join ( select srv.*
                  , age_from
                  , age_to
                  , max_maint_rate
                  , rt.parnt_id
                  , rt.id
               from staging.x_cont_srv srv
          left join staging.x_cont_srv_rates_p1 rt
                 on RT.PARNT_ID = srv.id
                and RT.ROW_DLT_TRNSCT_ID = 0 ) srv
on SRV.MDOC_NO = dc.CONTRACT_DOCUMENT_ID
and srv.row_dlt_trnsct_id = 0

inner join dhs_core.dim_service ds
on DS.SERV_CD = SRV.SERV_CD
and nvl(srv.rate_cat, -1) = nvl(ds.rate_category, -1)
and nvl(srv.emgcy_ind, 'N') = ds.emergency_ind
)*/

select count(*) from temp_cont_rate                
                
insert into DHS_CORE.FACT_CONTRACT_RATE1
select DIM_CONTRACT_ID
           , DIM_SERVICE_ID
           , EFF_DT
           , END_DT
           , AGE_FROM
           , AGE_TO
           , DIM_PROVIDER_ID
           , DIM_AGENCY_ID
           , DIM_YN_ID_AUTH_REQUIRED
           , DAILY_RATE
           , DAILY_RATE_MAX_MAINT
           , sysdate
    from temp_cont_rate
    
    
delete from fact_contract_rate1 where dim_contract_id is not null

select count(*) from dhs_core.fact_contract_rate1

select count(*) from temp_cont_rate
    
select count(end_dt) --71829
from dhs_core.temp_cont_rate

select count(*) from (
select distinct dim_contract_id, 
                dim_service_id, 
                age_from, end_dt
from temp_cont_rate
) --71648

select count(*) from fact_contract_rate1 --71317


select * 
from (
    select t.*, row_number() over (partition by dim_contract_id, dim_service_id, age_from, end_dt order by 1) as rn
    from temp_cont_rate t
    )
    where rn > 1
    
where dim_contract_id = 342500 

select * from temp_cont_rate
where dim_contract_id = 343514
and dim_service_id = 276
--where dim_contract_id = 343514


select * from temp_cont_rate
where dim_contract_id = 342163
and dim_service_id = 941

select * from staging.x_cont_srv_rates_p1
where id = 20768 or id = 20770

select * from staging.x_cont_srv
where id = 20767 or id = 20768 or id = 20769
