declare
 v_table_name varchar2(30) := 'DIM_CONTRACT';
 v_step_name varchar2(30);
 v_start_time timestamp;
 v_end_time timestamp;
 v_rows NUMBER;
 v_notes varchar2(2000);
begin

v_start_time := current_timestamp;
v_step_name := 'MERGE';

merge into DHS_CORE.DIM_CONTRACT tgt
using ( select
            type
          , case when type = 'CUA' then 'CUA'
                 when type = 'UMBR' then 'Umbrella'
                 when type = 'INDV' then 'Individual'
                 when type = 'SPEC' then 'Special'
                 when type is null then 'VDR Contract'
                 else type -- Comments did not explain other types
            end as type_description
          , mdoc_no
          , fin_no
          , to_number(extract(year from (add_months(xc.eff_dt, 6))), '9999') as fiscal
          , nvl(contr_no, 'N/A') as contr_no
          , xc.eff_dt as eff_dt
          , term_dt
          , dep_dlq as dd
          , case when dp.prv_no is null then
                    ( select dim_agency_id
                        from dhs_core.dim_agency da
                       where da.prv_no = xc.prv_no )
                 when dim_agency_id is null then 0
                 else dp.dim_agency_id
            end as agency_id
          , case when dp.prv_no is null then 0
                else dp.dim_provider_id
            end as provider_id
          , conf_dt
          , contr_limit
          , fund as funds
          , class
          , indx_cd
          , mdoc_tot_amt
          , case_limit as limits
          , cntr_cua_cd
          , case when cntr_cua_cd is null then
                    ( select dim_managing_org_id
                        from dhs_core.dim_managing_org
                       where org_type = 'DHS' )
              else
                    ( select dim_managing_org_id
                        from dhs_core.dim_managing_org
                       where cua_bsns_cd = cntr_cua_cd )
            end as managing_org
      from staging.x_cont xc
 left join dhs_core.dim_provider dp
        on dp.prv_no = xc.prv_no
     where xc.row_dlt_trnsct_id = 0
        ) src
           on ( tgt.contract_document_id = src.mdoc_no )
when matched then update
    set      contract_type_cd = src.type
           , contract_type_desc = src.type_description
           , finance_contract_id = src.fin_no
           , fiscal_year = src.fiscal
           , contract_num = src.contr_no
           , effective_date = src.eff_dt
           , termination_date = src.term_dt
           , dep_dlq = src.dd
           , dim_agency_id = src.agency_id
           , dim_provider_id = src.provider_id
           , conformed_date = src.conf_dt
           , contract_limit = src.contr_limit
           , fund = src.funds
           , contract_class = src.class
           , city_index_cd = src.indx_cd
           , mdoc_total_amt = src.mdoc_tot_amt
           , case_limit = src.limits
           , cua_cd = src.cntr_cua_cd
           , dim_managing_org_id = src.managing_org
           , last_modified_dt = current_timestamp
 where (     tgt.fiscal_year != src.fiscal
          or tgt.contract_num != src.contr_no
          or tgt.effective_date != src.eff_dt
          or tgt.termination_date != src.term_dt
          or tgt.dim_agency_id != src.agency_id
          or tgt.dim_provider_id != src.provider_id
          or tgt.dim_managing_org_id != src.managing_org
          or nvl(contract_type_cd, '_') != nvl(src.type, '_')
          or nvl(contract_type_desc, '_') != nvl(src.type_description, '_')
          or nvl(finance_contract_id, '_') != nvl(src.fin_no, '_')
          or nvl(dep_dlq, '_') != nvl(src.dd, '_')
          or nvl(conformed_date, to_date('1/1/1900', 'mm/dd/yyyy')) != nvl(src.conf_dt, to_date('1/1/1900', 'mm/dd/yyyy'))
          or nvl(contract_limit, -1) != nvl(src.contr_limit, -1)
          or nvl(fund, '_') != nvl(src.funds, '_')
          or nvl(contract_class, '_') != nvl(src.class, '_')
          or nvl(city_index_cd, '_') != nvl(src.indx_cd, '_')
          or nvl(mdoc_total_amt, '_') != nvl(src.mdoc_tot_amt, '_')
          or nvl(case_limit, '_') != nvl(src.limits, '_')
          or nvl(cua_cd, '_') != nvl(src.cntr_cua_cd, '_') )
when not matched then
    insert ( DIM_CONTRACT_ID
           , CONTRACT_TYPE_CD
           , CONTRACT_TYPE_DESC
           , CONTRACT_DOCUMENT_ID
           , FINANCE_CONTRACT_ID
           , FISCAL_YEAR
           , CONTRACT_NUM
           , EFFECTIVE_DATE
           , TERMINATION_DATE
           , DEP_DLQ
           , DIM_AGENCY_ID
           , DIM_PROVIDER_ID
           , CONFORMED_DATE
           , CONTRACT_LIMIT
           , FUND
           , CONTRACT_CLASS
           , CITY_INDEX_CD
           , MDOC_TOTAL_AMT
           , CASE_LIMIT
           , CUA_CD
           , DIM_MANAGING_ORG_ID
           , LAST_MODIFIED_DT )
  values (
             DHS_CORE.DIM_CONTRACT_ID_SEQ.NEXTVAL
           , src.type
           , src.type_description
           , src.mdoc_no
           , src.fin_no
           , src.fiscal
           , src.contr_no
           , src.eff_dt
           , src.term_dt
           , src.dd
           , src.agency_id
           , src.provider_id
           , src.conf_dt
           , src.limits
           , src.funds
           , src.class
           , src.indx_cd
           , src.mdoc_tot_amt
           , src.limits
           , src.cntr_cua_cd
           , src.managing_org
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

