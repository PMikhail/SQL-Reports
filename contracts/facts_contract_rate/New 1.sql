CREATE TABLE TEMP_CONT_RATE
AS (
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
inner join ( select *--nvl(serv_type, '_') s_type
--                 , nvl(rate_cat, -1) r_category
--                 , nvl(cost_center, '_') c_center
--                 , nvl(dep_dlq, '_') dd
--                 , srv.*
            from staging.x_cont_srv srv
            where row_dlt_trnsct_id = 0) srv
on srv.mdoc_no = dc.contract_document_id
inner join staging.x_cont_srv_rates_p1 rp
on rp.parnt_id = srv.id
--left join ( select * --vl(serv_type, '_') s_type 
----            , nvl(rate_category, -1) r_category
----            , nvl(cost_center, '_') c_center
----            , nvl(dep_dlq, '_') dd
----            , serv_cd
--            from dhs_core.dim_service ) ds
--on DS.SERV_CD = SRV.SERV_CD
--and nvl(SRV.Rate_CAT, -1) = nvl(DS.Rate_CATEGORY, -1)
where rp.row_dlt_trnsct_id = 0
) 


SELECT * FROM FACT_CONTRACT_RATE1

INSERT INTO DHS_CORE.FACT_CONTRACT_RATE1
    ( DIM_CONTRACT_ID
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
    , LAST_MODIFIED_DT )
SELECT TEMP.*, SYSDATE
FROM TEMP_FACT_RATE TEMP

SELECT COUNT(DIM_SERVICE_ID) FROM TEMP_FACT_RATE 