select * from all_col_comments where table_name = 'FACT_CONTRACT_RATE1'

select * from all_col_comments where table_name = ''

select * from all_col_comments where table_name = 'X_CONT_SRV'

select * from all_col_comments where table_name = 'X_CONT_SRV_RATES_P1'

SELECT * FROM STAGING.X_CONT_SRV

select count(*) from fact_contract_rate1


SELECT * FROM DHS_CORE.DIM_SERVICE

SELECT COUNT(*) FROM USER_TAB_COLUMNS
WHERE TABLE_NAME = 'DIM_SERVICE'
AND TABLE_CATALOG = 'DHS_CORE'


select count(distinct provider_id)
from (
select
case when dp.prv_no is null then 0
    else dp.dim_provider_id
    end as provider_id
from staging.x_cont xc
left join dim_provider dp
on dp.prv_no = xc.prv_no
left join dim_agency da
on da.prv_no = xc.prv_no
where row_dlt_trnsct_id = 0
)

select distinct dim_provider_id
from dim_contract

select count(distinct prv_no)
from dim_provider

select distinct rate_sp
from staging.x_cont_srv
where row_dlt_trnsct_id = 0
order by 1


select *
from staging.x_cont_srv srv
left join staging.x_cont_srv_rates_p1 rates
on srv.id = rates.parnt_id
and rates.row_dlt_trnsct_id = 0
where srv.row_dlt_trnsct_id = 0
and 
order by 1


select COUNT(distinct dim_provider_id) from(
select
      dim_contract_id
    , dim_service_id
    , case when srv.eff_dt < dc.effective_date 
      then dc.effective_date else srv.eff_dt 
      end as eff_dt   
    , case when srv.term_dt > dc.termination_date 
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
    , nvl(auth_req_ind, 'N') dim_yn_id_auth_required    
    , case when rate is null
      then srv.rate_sp 
      else rate
      end as daily_rate    
    , case when rate is null
      then srv.rate_sp else max_maint_rate
      end as daily_rate_max_maint   
from dhs_core.dim_contract dc
left join staging.x_cont_srv srv
on srv. row_dlt_trnsct_id = 0
and srv.mdoc_no = dc.contract_document_id
inner join staging.x_cont_srv_rates_p1 rp
on rp.parnt_id = srv.id
and rp.row_dlt_trnsct_id = 0
left join dhs_core.dim_service ds
on DS.SERV_CD = SRV.SERV_CD
and nvl(SRV.Rate_CAT, -1) = nvl(DS.Rate_CATEGORY, -1)
and nvl(SRV.EMGCY_IND, 'N') = DS.EMERGENCY_IND
)
--31292 
--total
--right join staging.x_cont_srv_rates_p1 rates
--on rates.parnt_id = total.id
--and row_dlt_trnsct_id = 0
--where total.id is null 

select * from staging.x_cont_srv srv 
where not exists (select * from dhs_core.dim_service ds where DS.SERV_CD = SRV.SERV_CD
                                                        and nvl(SRV.RATE_CAT,-1) = nvl(DS.RATE_CATEGORY, -1)
                                                        and nvl(srv.emgcy_ind, 'N') = ds.emergency_ind)
                                                        --and SRV.COST_CENTER = DS.COST_CENTER)
                                                        --and SRV.Serv_TYPE = ds.serv_type)
                                                        --and srv.dep_dlq = ds.dep_dlq )
and SRV.ROW_DLT_TRNSCT_ID = 0
            
select *
from dim_provider
order by 1

select * from dim_contract
order by dim_provider_id
                                                                                    
select count(*)
from dhs_core.dim_contract dc
inner join staging.x_cont_srv srv
on srv.mdoc_no = dc.contract_document_id
and srv.row_dlt_trnsct_id = 0
left join staging.x_cont_srv_rates_p1 rp
on rp.parnt_id = srv.id
and rp.row_dlt_trnsct_id = 0
left join dhs_core.dim_service ds
on DS.SERV_CD = SRV.SERV_CD
and nvl(SRV.Rate_CAT, -1) = nvl(DS.Rate_CATEGORY, -1)
and nvl(SRV.EMGCY_IND, 'N') = DS.EMERGENCY_IND