
select * from
(select row_number() over (partition by dim_contract_id, dim_service_id order by end_dt, age_from) as rn
, this.*
from
(select * from
        ( 
           select
              dc.dim_contract_id
            , ds.dim_service_id
            , case when srv.eff_dt < dc.effective_date 
              then dc.effective_date else srv.eff_dt 
              end as eff_dt   
            , case when SRV.TERM_DT > DC.TERMINATION_DATE 
              then dc.termination_date else srv.term_dt 
              end as end_dt  
            , case when rp.rate is null
              then 0 else rp.age_from
              end as age_from    
            , case when rp.rate is null
              then 999 else rp.age_to
              end as age_to    
            , dc.dim_provider_id
            , dc.dim_agency_id
            , case srv.auth_req_ind when 'Y' then 1
                                    when 'N' then 0
                                    else 1
              end as dim_yn_id_auth_required    
            , case when rp.rate is null
              then srv.rate_sp 
              else rp.rate
              end as daily_rate    
            , case when rp.rate is null
              then srv.rate_sp else rp.max_maint_rate
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
) this 
) 
where dim_contract_id = 340977
and dim_agency_id = 720
--order by eff_dt, end_dt, age_from, age_to
order by dim_contract_id, dim_service_id, eff_dt, end_dt, age_from, age_to


select * from dhs_core.dim_agency where prv_no = '6620AG0000'

select * from DHS_CORE.DIM_CONTRACT
where dim_contract_id = 340977

select * from staging.x_cont_srv srv
left join staging.x_cont_srv_rates_p1 rates
on rates.parnt_id = srv.id
and rates.row_dlt_trnsct_id = 0
where mdoc_no = 'MQ662001'
and srv.row_dlt_trnsct_id = 0


select mdoc_no, age_from, age_to, rate_sp, rate
from staging.x_cont_srv srv
right join( select * 
            from staging.x_cont_srv_rates_p1
            where age_to = 0 and row_dlt_trnsct_id = 0 ) rates
on rates.parnt_id = srv.id


select * from staging.x_cont_srv

select distinct age_from, age_to from staging.x_cont_srv_rates_p1
