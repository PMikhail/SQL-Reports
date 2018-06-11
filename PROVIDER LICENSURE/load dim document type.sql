declare
 v_table_name varchar2(30) := 'DIM_DOCUMENT_TYPE';
 v_step_name varchar2(30);
 v_start_time timestamp;
 v_end_time timestamp;
 v_rows number;
 v_notes varchar2(2000);
begin

v_start_time := current_timestamp;
v_step_name := 'MERGE';

merge into dhs_core.dim_document_type tgt
using ( select
             distinct dcmnt_cd as document_cd --distinct substr(regexp_substr(pl.dcmnt_cd, '-[^,]+'),2) as document_cd
           , lkup.descr as document_name
        from staging.prvdr_lcnc pl
  inner join staging.lookups lkup
          on pl.dcmnt_cd = lkup.cd
      ) src
         on ( src.document_cd = tgt.document_cd )  
when matched then update
    set     document_name = src.document_name
          , last_modified_dt = current_timestamp
  where ( 
            tgt.document_name != src.document_name
         or nvl(tgt.document_cd, '_') != nvl(src.document_cd, '_') )
when not matched then
   insert(  dim_document_type_id
          , document_cd
          , document_name
          , last_modified_dt )
  values ( dhs_core.dim_document_type_id_seq.nextval
         , src.document_cd
         , src.document_name
         , current_timestamp );
                  
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
