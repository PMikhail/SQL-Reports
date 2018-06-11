with
case_summary as
(
    select *
    from (
        select dim_work_product_id
             , 9 as dim_managing_org_id
             , trunc(initiated_date) as eff_dt
             , nvl(trunc(closed_date), to_date('12/31/9999', 'mm/dd/yyyy')) as end_dt
        from fact_work_prod_status_summary
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
     , eff_date as eff_dt
     , end_date as end_dt
--     , case when end_date = to_date('12/31/9999', 'mm/dd/yyyy') to_date('12/31/9999 ', 'mm/dd/yyyy hh24:mi:ss')
--            else end_date end as end_dt
     , current_timestamp as last_modified_dt
 from (
        select dim_work_product_id
             , dim_managing_org_id
             , trunc(eff_dt) as eff_date
             , case when next_eff is not null then trunc(next_eff) - 1/86400                    
                    else trunc(end_dt) end as end_date
        from date_slice
        where time_slice_type <> 2 
--        and dim_work_product_id = 207186 --209491             --TEST CASES 
--             or dim_work_product_id = 127432)
--             or dim_work_product_id = 212319
--             or dim_work_product_id = 207186
--             or dim_work_product_id = 8614)
        order by dim_work_product_id, eff_dt
      )
where end_date > eff_date