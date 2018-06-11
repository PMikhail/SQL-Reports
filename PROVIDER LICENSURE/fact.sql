declare
 v_table_name varchar2(30) := 'FACT_PROVIDER_LICENSURE';
 v_step_name varchar2(30);
 v_start_time timestamp;
 v_end_time timestamp;
 v_rows NUMBER;
 v_notes varchar2(2000);
begin

v_start_time := current_timestamp;
v_step_name := 'MERGE';

merge into DHS_CORE.fact_provider_licensure1 tgt
using(   select pl.*
              , case when dim_yn_id_is_licensed = 1
                     then DD1.DIM_DATE_ID 
                     else 0
                end as dim_date_id_licensed_from
              , case when dim_yn_id_is_licensed = 1
                     then dd2.dim_date_id
                     else 0
                end as dim_date_id_licensed_to
            from(
                select    dim_provider_id
                        , dim_document_type_id
                        , implied_start_date_ind
                        , dd1.dim_date_id as dim_date_id_approval_eff
                        , dd1.dim_date_id as dim_date_id_approval_end
                        , dim_date_id_eff as eff_dt
                        , dim_document_set_id_missing
                        , dim_document_set_id_noncomp
                        , dim_yn_id_is_licensed
                        , case when dim_date_id_end = ( lead(dim_date_id_end) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff, dim_date_id_end) )
                               then lead(dim_date_id_eff) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff)  - 1
                               else dim_date_id_end
                          end as end_dt --will also be as dim_date_id_licensed_to
                  from (
                        select     prvdr_no
                                 , dim_provider_id
                                 , dim_document_type_id
                                 , case when pl.apprvl_actual_end_dt is not null 
                                        then pl.apprvl_actual_end_dt--to_number(to_char(pl.apprvl_actual_end_dt, 'yyyymmdd'))
                                        when pl.apprvl_actual_end_dt is null and pl.apprvl_end_dt is not null
                                        then pl.apprvl_end_dt--to_number(to_char(pl.apprvl_end_dt, 'yyyymmdd'))
                                        else to_date('12/31/9999', 'MM/DD/YYYY')
                                   end as dim_date_id_end
                                 , case when pl.apprvl_effctv_dt is null and pl.apprvl_actual_end_dt is null
                                        then null
                                        when pl.apprvl_effctv_dt is null and pl.apprvl_actual_end_dt is not null
                                        then to_date('1/1/1900', 'MM/DD/YYYY')
                                        else pl.apprvl_effctv_dt
                                   end as dim_date_id_eff 
                                 , case when pl.apprvl_effctv_dt is null and pl.apprvl_actual_end_dt is not null
                                        then 'Y' else 'N'
                                   end as implied_start_date_ind
                                 , DS2.DIM_DOCUMENT_SET_ID as dim_document_set_id_missing
                                 , ds1.dim_document_set_id as dim_document_set_id_noncomp
                                 , case when ds1.num_docs = 0 and ds2.num_docs = 0 then 1
                                        else 0
                                   end as dim_yn_id_is_licensed
                        from ( select distinct awtng_dcmntn
                                             , nn_cmplnc_rson
                                             , nn_cmplnc_oth
                                             , prvdr_no
                                             , dcmnt_cd
                                             , apprvl_effctv_dt
                                             , apprvl_end_dt
                                             , apprvl_actual_end_dt 
                                         from staging.prvdr_lcnc ) pl
                        left join dim_document_type dt
                        on pl.dcmnt_cd = dt.document_cd
                        left join dim_provider dp
                        on dp.prv_no = pl.prvdr_no
                        left join DHS_CORE.DIM_DOCUMENT_SET ds1
                        on nvl(ds1.doc_cd_list,'_') = nvl(pl.nn_cmplnc_rson,'_')
                        and nvl(ds1.other_text, '_') = nvl(substr(pl.nn_cmplnc_oth, 1, 50), '_')
                        inner JOIN DHS_CORE.DIM_DOCUMENT_SET DS2 on nvl(ds2.doc_cd_list,'_') = nvl(pl.AWTNG_DCMNTN,'_') 
                                                ) pl
                    INNER JOIN DIM_DATE DD1
                    ON DD1.DT = PL.dim_date_id_eff
                    INNER JOIN DIM_DATE DD2
                    ON DD2.DT = PL.dim_date_id_end ) pl
            inner join dim_date dd1
            on dd1.dt = eff_dt
            inner join dim_date dd2
            on dd2.dt = end_dt 
        )SRC
         ON ( 
              tgt.dim_provider_id = src.dim_provider_id
          and tgt.dim_document_type_id = src.dim_document_type_id
          and tgt.end_dt = src.end_dt
           )
when matched then update
        set eff_dt = src.eff_dt 
          , dim_date_id_approval_eff = src.dim_date_id_approval_eff
          , dim_date_id_approval_end = src.dim_date_id_approval_end
          , implied_start_date_ind = src.implied_start_date_ind
          , dim_yn_id_is_licensed = src.dim_yn_id_is_licensed
          , dim_document_set_id_missing = src.dim_document_set_id_missing
          , dim_document_set_id_noncomp = src.dim_document_set_id_noncomp
          , last_modified_dt = current_timestamp
          , dim_date_id_licenced_from = src.dim_date_id_licensed_from
          , dim_date_id_licenced_to = src.dim_date_id_licensed_to
where (
             tgt.eff_dt != src.eff_dt
          or tgt.dim_date_id_approval_eff != src.dim_date_id_approval_eff
          or tgt.dim_date_id_approval_end != src.dim_date_id_approval_end
          or tgt.implied_start_date_ind != src.implied_start_date_ind
          or nvl(tgt.dim_yn_id_is_licensed, -2) != nvl(src.dim_yn_id_is_licensed, -2)
          or tgt.dim_document_set_id_missing != src.dim_document_set_id_missing
          or tgt.dim_document_set_id_noncomp != src.dim_document_set_id_noncomp
          or tgt.dim_date_id_licenced_from != src.dim_date_id_licensed_from
          or tgt.dim_date_id_licenced_to != src.dim_date_id_licensed_to
      )
when not matched then
    insert (  DIM_DOCUMENT_TYPE_ID
            , DIM_PROVIDER_ID
            , EFF_DT
            , END_DT
            , DIM_DATE_ID_APPROVAL_EFF
            , DIM_DATE_ID_APPROVAL_END
            , IMPLIED_START_DATE_IND
            , DIM_YN_ID_IS_LICENSED
            , DIM_DOCUMENT_SET_ID_MISSING
            , DIM_DOCUMENT_SET_ID_NONCOMP
            , LAST_MODIFIED_DT
            , DIM_DATE_ID_LICENCED_FROM
            , DIM_DATE_ID_LICENCED_TO )
   values (
              src.DIM_DOCUMENT_TYPE_ID
            , src.DIM_PROVIDER_ID
            , src.EFF_DT
            , src.END_DT
            , src.DIM_DATE_ID_APPROVAL_EFF
            , src.DIM_DATE_ID_APPROVAL_END
            , src.IMPLIED_START_DATE_IND
            , src.DIM_YN_ID_IS_LICENSED
            , src.DIM_DOCUMENT_SET_ID_MISSING
            , src.DIM_DOCUMENT_SET_ID_NONCOMP
            , CURRENT_TIMESTAMP
            , src.DIM_DATE_ID_LICENSED_FROM
            , src.DIM_DATE_ID_LICENSED_TO );
      

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
