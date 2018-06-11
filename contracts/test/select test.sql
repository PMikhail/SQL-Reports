select count(*) from (
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
where   end_dt > eff_dt

select count(*) from dhs_core.fact_contract_rate1

select count(*) from temp_cont_rate