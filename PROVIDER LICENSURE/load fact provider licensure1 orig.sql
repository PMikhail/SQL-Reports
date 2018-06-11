declare
 v_table_name varchar2(30) := 'FACT_PROVIDER_LICENSURE_ETL';
 v_step_name varchar2(30);
 v_start_time timestamp;
 v_end_time timestamp;
 v_rows number;
 v_notes varchar2(2000);
begin
dhs_core.p_etl.truncate_dhs_core(v_table_name);

v_start_time := current_timestamp;
v_step_name := 'INSERT';

insert into dhs_core.fact_provider_licensure_etl tgt
     ( 
     
     )
using(   select pl2.*
              , case when dim_yn_id_is_licensed = 1
                     then dd11.dim_date_id 
                     else 0
                end as dim_date_id_licensed_from
              , case when dim_yn_id_is_licensed = 1
                     then dd12.dim_date_id
                     else 0
                end as dim_date_id_licensed_to
            from(
                select    dim_provider_id
                        , dim_document_type_id
                        , implied_start_date_ind
                        , dd1.dim_date_id as dim_date_id_approval_eff
                        , dd2.dim_date_id as dim_date_id_approval_end
                        , dim_date_id_eff as eff_dt
                        , dim_document_set_id_missing
                        , dim_document_set_id_noncomp
                        , dim_yn_id_is_licensed
                        , case when dim_date_id_end = ( lead(dim_date_id_eff) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff, dim_date_id_end) )
                               then lead(dim_date_id_eff) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff)  - 1/86400
                               else dim_date_id_end + 1/86400
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
                                 , ds2.dim_document_set_id as dim_document_set_id_missing
                                 , ds1.dim_document_set_id as dim_document_set_id_noncomp
                                 , case when ds1.num_docs = 0 and ds2.num_docs = 0 then 1
                                        else 0
                                   end as dim_yn_id_is_licensed
                        from ( select distinct awtng_dcmntn         --HAD DUPLICATE ROWS WITH SAME EXACT DATA, EXCEPT ROW_CRTN_USR AND ROW_CRTN_DT
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
                        left join dhs_core.dim_document_set ds1
                        on nvl(ds1.doc_cd_list,'_') = nvl(pl.nn_cmplnc_rson,'_')
                        and nvl(ds1.other_text, '_') = nvl(substr(pl.nn_cmplnc_oth, 1, 50), '_')
                        inner join dhs_core.dim_document_set ds2 on nvl(ds2.doc_cd_list,'_') = nvl(pl.awtng_dcmntn,'_') 
                                                ) pl1
                    inner join dim_date dd1  --JOINS TO GET DIM_DATE_ID_APPROVAL_EFF AND END
                    on dd1.dt = pl1.dim_date_id_eff
                    inner join dim_date dd2
                    on dd2.dt = pl1.dim_date_id_end ) pl2
            inner join dim_date dd11 --JOINS TO GET DIM_DATE_ID_LICENSED_FROM AND TO
            on dd11.dt = eff_dt
            inner join dim_date dd12
            on dd12.dt = end_dt 
        )src
         on ( 
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
    insert (  dim_document_type_id
            , dim_provider_id
            , eff_dt
            , end_dt
            , dim_date_id_approval_eff
            , dim_date_id_approval_end
            , implied_start_date_ind
            , dim_yn_id_is_licensed
            , dim_document_set_id_missing
            , dim_document_set_id_noncomp
            , last_modified_dt
            , dim_date_id_licenced_from
            , dim_date_id_licenced_to )
   values (
              src.dim_document_type_id
            , src.dim_provider_id
            , src.eff_dt
            , src.end_dt
            , src.dim_date_id_approval_eff
            , src.dim_date_id_approval_end
            , src.implied_start_date_ind
            , src.dim_yn_id_is_licensed
            , src.dim_document_set_id_missing
            , src.dim_document_set_id_noncomp
            , current_timestamp
            , src.dim_date_id_licensed_from
            , src.dim_date_id_licensed_to );
      

v_rows := sql%rowcount;
v_end_time := current_timestamp;

insert into dhs_core.etl_log (
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
    v_notes := to_char(sqlcode)||' '||substr(sqlerrm,1,1000)||' '||substr(dbms_utility.format_error_backtrace(),1,500);
    v_end_time := current_timestamp;
    insert into dhs_core.etl_log (
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
