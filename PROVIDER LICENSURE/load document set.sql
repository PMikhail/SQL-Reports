declare
 v_table_name varchar2(30) := 'DIM_DOCUMENT_SET';
 v_step_name varchar2(30);
 v_start_time timestamp;
 v_end_time timestamp;
 v_rows number;
 v_notes varchar2(2000);
begin

v_start_time := current_timestamp;
v_step_name := 'MERGE';

merge into dhs_core.dim_document_set tgt
using (select
          --subquery used to count num_docs easily 
          nvl((length(doc_cd_list) - length(replace(doc_cd_list, '-'))), 0) as num_docs 
        , this.* from
         ( select    
               case when nn_cmplnc_rson || '|' like '%-MMD|%' then 'Y'
                     when nn_cmplnc_rson || '|' like '%-MEDDOC|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-MMD|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-MEDDOC|%' then 'Y'
                else 'N' end as medical_flag
              , case when nn_cmplnc_rson || '|' like '%-CAC|%' then 'Y'
                     when nn_cmplnc_rson || '|' like '%-MCAC|%' then 'Y'
                     when nn_cmplnc_rson || '|' like '%-CHLDABUS|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-CAC|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-MCAC|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-CHLDABUS|%' then 'Y'
                else 'N' end as child_abuse_flag
              , case when nn_cmplnc_rson || '|' like '%-CC|%' then 'Y'
                     when nn_cmplnc_rson || '|' like '%-MCC|%' then 'Y'
                     when nn_cmplnc_rson || '|' like '%-CRIMCLR|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-CC|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-MCC|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-CRIMCLR|%' then 'Y'
                else 'N' end as criminal_clearance_flag
              , case when nn_cmplnc_rson || '|' like '%-LTH|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-LTH|%' then 'Y'
                else 'N' end as training_hours_flag
              , case when nn_cmplnc_rson || '|' like '%-FCM|%' then 'Y'
                     when nn_cmplnc_rson || '|' like '%-MFCM|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-FCM|%' then 'Y'
                     when awtng_dcmntn || '|' like '%-MFCM|%' then 'Y'
                else 'N' end as fbi_clearance_flag
              , case when nn_cmplnc_rson || '|' like '%-OTH|%' then 'Y'
                else 'N' end as other_flag
              , substr(nn_cmplnc_oth, 1, 50) as other_text    
              , case when awtng_dcmntn is null then nn_cmplnc_rson else awtng_dcmntn
                end as doc_cd_list
            from ( select distinct pl.nn_cmplnc_rson, pl.nn_cmplnc_oth  
                      from staging.prvdr_lcnc pl ) one
            full outer join ( select distinct awtng_dcmntn 
                                from staging.prvdr_lcnc 
                               where awtng_dcmntn is not null ) two --OMIT NULL AWTNG_DCMNTN TO PREVENT DUPLICATE NULL SETS (APPRVOED SET)
            on one.nn_cmplnc_rson = two.awtng_dcmntn
            ) this
      ) src
         on (    tgt.num_docs = src.num_docs
             and tgt.other_flag = src.other_flag   
             and nvl(tgt.other_text, '_') = nvl(src.other_text, '_')
             and nvl(tgt.doc_cd_list, '_') = nvl(src.doc_cd_list, '_') )
when matched then update
    set       tgt.medical_flag = src.medical_flag
            , tgt.child_abuse_flag = src.child_abuse_flag
            , tgt.criminal_clearance_flag = src.criminal_clearance_flag
            , tgt.training_hours_flag = src.training_hours_flag
            , tgt.fbi_clearance_flag = src.fbi_clearance_flag
            , last_modified_dt = current_timestamp
  where (
            tgt.medical_flag != src.medical_flag
         or tgt.child_abuse_flag != src.child_abuse_flag
         or tgt.criminal_clearance_flag != src.criminal_clearance_flag
         or tgt.training_hours_flag != src.training_hours_flag
         or tgt.fbi_clearance_flag != src.fbi_clearance_flag
          )
when not matched then
 insert (   dim_document_set_id
          , num_docs
          , medical_flag
          , child_abuse_flag
          , criminal_clearance_flag
          , training_hours_flag
          , fbi_clearance_flag
          , other_flag
          , other_text
          , last_modified_dt
          , doc_cd_list )
 values (
            dhs_core.dim_document_set_id_seq.nextval
          , src.num_docs
          , src.medical_flag
          , src.child_abuse_flag
          , src.criminal_clearance_flag
          , src.training_hours_flag
          , src.fbi_clearance_flag
          , src.other_flag
          , src.other_text
          , current_timestamp
          , src.doc_cd_list );
          

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