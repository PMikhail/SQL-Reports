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
    select *
    from (
        select dim_work_product_id
             , trunc(initiated_date) as eff_dt
             , nvl(trunc(closed_date), to_date('12/31/9999', 'mm/dd/yyyy')) as end_dt
             , initiated_date as case_open_dt
        from dhs_core.fact_work_prod_status_summary
        where work_product_type_cd = 'CASE'
        order by dim_work_product_id
        )
    where eff_dt <> end_dt
)
, case_summary2 as
(
    select cs.*
         , lead(eff_dt) over(partition by dim_work_product_id order by eff_dt) as next_eff_dt
    from case_summary cs
)
, case_full_slice as
(
    select cs.dim_work_product_id
         , dmo.dim_managing_org_id
         , cs.eff_dt
         , cs.end_dt
         , cs.case_open_dt
         , 1 as time_slice_type
     from case_summary cs
          cross join
          ( select dim_managing_org_id
            from dhs_core.dim_managing_org
            where org_type = 'DHS' and cua_cd = 'NAPP'
          ) dmo
    union all
    select dim_work_product_id
         , 9 as dim_managing_org_id
         , end_dt + 1/86400 as eff_dt
         , next_eff_dt - 1/86400 as end_dt
         , null as case_open_dt
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
         , null as case_open_dt
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
          , null as case_open_dt
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
         , lead(completed_dt) over(partition by dim_case_id order by completed_dt) as next_completed_dt
    from dhs_core.fact_case_cua_referral fccr
    where called_back_ind = 'N'
      and completed_dt is not null
)
, referral_slice as
(
    select dim_work_product_id
         , dim_managing_org_id
         , completed_dt as eff_dt
         , nvl(next_completed_dt - 1/86400,to_date('12/31/9999', 'mm/dd/yyyy')) as end_dt
    from case_referral cfs
    union all
    -- get missing before first assignment
    select a.dim_work_product_id
         , 0 as dim_managing_org_id
         , to_date('1/1/1900', 'mm/dd/yyyy') as eff_dt
         , min(completed_dt) - 1/86400 as end_dt
     from case_referral a
     group by  a.dim_work_product_id
)
 select cfs.dim_work_product_id as dim_case_id
   , case when rs.dim_managing_org_id = 0 then cfs.dim_managing_org_id
          when cfs.dim_managing_org_id = 0 then cfs.dim_managing_org_id
          when rs.eff_dt < cfs.case_open_dt then cfs.dim_managing_org_id
          else rs.dim_managing_org_id end as dim_managing_org_id
   , greatest(cfs.eff_dt,rs.eff_dt) as eff_dt
   , least(cfs.end_dt,rs.end_dt) as end_dt
   , current_timestamp
   -- debug
   --, cfs.time_slice_type
  from case_full_slice cfs
      inner join
      referral_slice rs
          on rs.dim_work_product_id = cfs.dim_work_product_id
          and rs.eff_dt <= cfs.end_dt
          and rs.end_dt >= cfs.eff_dt
-- debug
--where cfs.dim_work_product_id = 151465
order by 3;

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
