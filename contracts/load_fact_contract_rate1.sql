declare
 v_table_name varchar2(30) := 'FACT_CONTRACT_RATE1';
 v_step_name varchar2(30);
 v_start_time timestamp;
 v_end_time timestamp;
 v_rows NUMBER;
 v_notes varchar2(2000);
begin

v_start_time := current_timestamp;
v_step_name := 'MERGE';

merge into DHS_CORE.FACT_CONTRACT_RATE1 tgt
using (
        select * from
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
        where  end_dt > eff_dt --ignore cases when service doesn't overlap contract 
      ) src
           on (
                  TGT.DIM_CONTRACT_ID = src.dim_contract_id
              and TGT.DIM_SERVICE_ID = src.dim_service_id
              and tgt.age_from = src.age_from
              and TGT.END_DT = src.end_dt 
              )
when matched then update
    set      eff_dt = src.eff_dt
           , age_to = src.age_to
           , dim_provider_id = src.dim_provider_id
           , dim_agency_id = src.dim_agency_id
           , dim_yn_id_auth_required = src.dim_yn_id_auth_required
           , daily_rate = src.daily_rate
           , daily_rate_max_maint = src.daily_rate_max_maint
           , last_modified_dt = current_timestamp
    where (   tgt.age_to != src.age_to
           or tgt.dim_provider_id != src.dim_provider_id
           or tgt.dim_agency_id != src.dim_agency_id
           or tgt.dim_yn_id_auth_required != src.dim_yn_id_auth_required
           or tgt.daily_rate != src.daily_rate
           or tgt.daily_rate_max_maint != src.daily_rate_max_maint
           or nvl(tgt.eff_dt, to_date('1/1/1900', 'mm/dd/yyyy')) != nvl(src.eff_dt, to_date('1/1/1900', 'mm/dd/yyyy')) 
           )
when not matched then
    insert ( DIM_CONTRACT_ID
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
           , LAST_MODIFIED_DT
           )
    values ( src.DIM_CONTRACT_ID
           , src.DIM_SERVICE_ID
           , src.EFF_DT
           , src.END_DT
           , src.AGE_FROM
           , src.AGE_TO
           , src.DIM_PROVIDER_ID
           , src.DIM_AGENCY_ID
           , src.DIM_YN_ID_AUTH_REQUIRED
           , src.DAILY_RATE
           , src.DAILY_RATE_MAX_MAINT
           , current_timestamp );
          
          
v_rows := SQL%ROWCOUNT;
v_end_time := current_timestamp;

insert into DHS_CORE.ETL_LOG (
             table_name
           , step_name
           , start_time
           , end_time
           , rows_affected
           , notes )
  values ( v_table_name
         , v_step_name
         , v_start_time
         , v_end_time
         , v_rows
         , null );
        
        
commit;


exception
  when others then
    v_notes := to_char(SQLCODE)||' '||substr(SQLERRM,1,1000)||' '||substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,500);
    v_end_time := current_timestamp;
    insert into DHS_CORE.ETL_LOG (
              table_name
            , step_name
            , start_time
            , end_time
            , rows_affected
            , notes)
    values (  v_table_name
            , v_step_name
            , v_start_time
            , v_end_time
            , v_rows
            , v_notes );
    commit;
    raise;
   
end;

