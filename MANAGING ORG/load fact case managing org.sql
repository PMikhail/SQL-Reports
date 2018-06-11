declare
 v_table_name varchar2(30) := 'FACT_CASE_MANAGING_ORG';
 v_step_name varchar2(30);
 v_start_time timestamp;
 v_end_time timestamp;
 v_rows number;
 v_notes varchar2(2000);
begin
dhs_core.p_etl.truncate_dhs_core(v_table_name);

v_start_time := current_timestamp;
v_step_name := 'INSERT';

insert into dhs_core.fact_case_managing_org_etl
        ( dim_case_id
        , dim_managing_org_id
        , eff_dt
        , end_dt
        , last_modified_dt
        )
with
case_summary as
(
    select dim_work_product_id
         , 9 as dim_managing_org_id
         , initiated_date as eff_dt
         , closed_date as end_dt
    from fact_work_prod_status_summary
    where work_product_type_cd = 'CASE'
    and trunc(initiated_date) <> trunc(closed_date)
    order by dim_work_product_id
)
, case_summary2 as
(
    select cs.*
         , lead(eff_dt) over(partition by dim_work_product_id order by eff_dt) as next_eff_dt
    from case_summary cs
)
, case_full_slice as
(
    select cs.*
         , 1 as time_slice_type
     from case_summary cs
    union all
    select dim_work_product_id
         , 0 as dim_managing_org_id
         , end_dt + 1/86400 as eff_dt
         , next_eff_dt - 1/86400 as end_dt
         , 3 as time_slice_type
     from case_summary2
    where next_eff_dt > end_dt + 1/86400
    union all
    -- get missing before first assignment
    select a.dim_work_product_id
         , 0 as dim_managing_org_id
         , to_date('1/1/1900', 'mm/dd/yyyy') as eff_dt
         , case when b.min_eff_dt is null then to_date('12/31/9999', 'mm/dd/yyyy')
                else b.min_eff_dt - 1/86400 end as end_dt
         , 2 as time_slice_type         
     from (
            select distinct dim_work_product_id
            from case_summary
          ) a
     left join(
            select dim_work_product_id
               , min(eff_dt) as min_eff_dt
            from case_summary
            group by dim_work_product_id
              ) b
     on b.dim_work_product_id = a.dim_work_product_id
     union all
     -- get missing after last assignment
     select b.dim_work_product_id
          , 0 as dim_managing_org_id
          , b.max_end_dt + 1/86400 as eff_dt
          , to_date('12/31/9999', 'mm/dd/yyyy') as end_dt
          , 4 as time_slice_type
     from (
            select dim_work_product_id
                 , max(end_dt) as max_end_dt
             from case_summary
             group by dim_work_product_id
             having max(end_dt) < to_date('12/31/9999', 'mm/dd/yyyy')
          ) b
)
, case_referral as
(
    select dim_case_id as dim_work_product_id
         , dim_managing_org_id
         , completed_dt
         , null as end_dt
         , 5 as time_slice_type
    from fact_case_cua_referral fccr
    where called_back_ind = 'N'
      and completed_dt is not null 
)
, referral_slice as
(
    select cfs.dim_work_product_id
         , cr.dim_managing_org_id
         , completed_dt as eff_dt
         , cfs.end_dt
         , cr.time_slice_type
    from case_full_slice cfs
    inner join case_referral cr
    on cr.dim_work_product_id = cfs.dim_work_product_id
    and completed_dt between cfs.eff_dt and cfs.end_dt
)
, joined as
(
    select *
    from case_full_slice cfs
    union all
    select *
    from referral_slice
)
, date_slice as
(
    select jn.*
         , lag(eff_dt) over(partition by dim_work_product_id order by eff_dt) as prev_eff
         , lag(end_dt) over(partition by dim_work_product_id order by eff_dt) as prev_end
         , lead(end_dt) over(partition by dim_work_product_id order by eff_dt) as next_end
         , lead(eff_dt) over(partition by dim_work_product_id order by eff_dt) as next_eff
    from joined jn
)
select dim_work_product_id as dim_case_id
     , dim_managing_org_id
     , to_char(eff_date, 'mm/dd/yyyy hh24:mi:ss') as eff_dt
     , to_char(end_date, 'mm/dd/yyyy hh24:mi:ss') as end_dt
 from (
        select dim_work_product_id
             , dim_managing_org_id
             , trunc(eff_dt) as eff_date
             , case when next_eff is not null then trunc(next_eff) - 1/86400
                    else trunc(prev_end) end as end_date
        from date_slice
        where time_slice_type <> 2 
--        and (dim_work_product_id = 209491             TEST CASES 
--             or dim_work_product_id = 212319
--             or dim_work_product_id = 207186)
        order by dim_work_product_id, eff_dt
      );

v_rows := SQL%ROWCOUNT;
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
        , null );

commit;

v_start_time := current_timestamp;
v_step_name := 'ANALYZE';
v_rows := NULL;

dhs_core.p_etl.analyze_dhs_core(v_table_name);
dhs_core.p_etl.switch_synonyms(v_table_name, 'FACT_CASE_MANAGING_ORG');

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
        , null );
        

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
