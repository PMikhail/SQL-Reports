--select * from staging.x_nrmb_chld
--where row_dlt_trnsct_id = 0 --7358
--and case_no = 192194
--
with 
base as
(
    select nrmb.*
         , lag(nr_term_dt1) over(partition by case_id, nr_reason_cd order by nr_eff_dt1, nr_term_dt1) as prev_term_dt
         , lead(nr_eff_dt1) over(partition by case_id, nr_reason_cd order by nr_eff_dt1, nr_term_dt1) as next_eff_dt
    from(
        select distinct case_no || sfx as case_id
             , nr_reason_cd
             , nr_eff_dt  as nr_eff_dt1
             , nr_term_dt as nr_term_dt1
         from staging.x_nrmb_chld
         where row_dlt_trnsct_id = 0
    ) nrmb
    order by case_id, nr_reason_cd, nr_eff_dt1
)
, leads as (        --count = 7271
    select bb.*
         , lag(nr_eff_dt) over(partition by case_id  order by nr_eff_dt, nr_term_dt) as prev_eff_dt
         , lag(nr_term_dt) over(partition by case_id order by nr_eff_dt, nr_term_dt) as prev_term_dt
         , lead(nr_eff_dt) over(partition by case_id order by nr_eff_dt, nr_term_dt) as next_eff_dt
         , lead(nr_term_dt) over(partition by case_id order by nr_eff_dt, nr_term_dt) as next_term_dt
         , lag(nr_reason_cd) over(partition by case_id order by nr_eff_dt, nr_term_dt) as prev_reason_cd
         , lead(nr_reason_cd) over(partition by case_id order by nr_eff_dt, nr_term_dt) as next_reason_cd
    from (
        select distinct case_id
             , nr_reason_cd
             , nvl(nr_eff_dt2, prev_eff_dt2) as nr_eff_dt
             , nvl(nr_term_dt2, next_term_dt2) as nr_term_dt
         from (
         select case_id
              , nr_reason_cd
              , nr_eff_dt1
              , nr_term_dt1
              , nr_eff_dt2
              , nr_term_dt2
              , lag(nr_eff_dt2) over(partition by case_id, nr_reason_cd order by nr_eff_dt1, nr_term_dt2) as prev_eff_dt2
              , lead(nr_term_dt2) over(partition by case_id, nr_reason_cd order by nr_eff_dt1, nr_term_dt2) as next_term_dt2              
         from (
                select case_id
                     , nr_reason_cd
                     , nr_eff_dt1
                     , nr_term_dt1
                     , case when prev_term_dt = nr_eff_dt1 - 1 and next_eff_dt = nr_term_dt1 + 1 then null
                            when prev_term_dt = nr_eff_dt1 - 1 then null
                            else nr_eff_dt1 end as nr_eff_dt2
                     , case when nr_term_dt1 <> to_date('12/31/9999', 'mm/dd/yyyy') 
                                and next_eff_dt = nr_term_dt1 + 1 and prev_term_dt = nr_eff_dt1 - 1 then null
                            when nr_term_dt1 = next_eff_dt - 1 then null 
                            else nr_term_dt1 end as nr_term_dt2 
                from base
              ) aa
         where nr_eff_dt2 is not null or nr_term_dt2 is not null
         order by case_id, nr_eff_dt1
             )
        ) bb
)--debug
--select * from base2 where case_id = '165054C'
--select * from leads where case_id = '154203F' order by nr_eff_dt
, case_one as (
    select leads.*
         , 1 as half
      from leads
     where (next_eff_dt = nr_eff_dt and next_term_dt > nr_term_dt)
     union
     select leads.*
         , 2 as half
      from leads
     where prev_eff_dt = nr_eff_dt and prev_term_dt < nr_term_dt --covers 1 count = 17
)
, case_one2 as (
    select case_one.*
         --, prev_term_dt
         , case when half = 1 then nr_eff_dt
                when half = 2 then prev_term_dt + 1
                end as eff_dt
         , nr_term_dt as end_dt
         , case when half = 1 and nr_reason_cd = next_reason_cd then nr_reason_cd
                when half = 1 then nr_reason_cd || ', ' || next_reason_cd
                else nr_reason_cd 
                end as final_reason_cd       
    from case_one
)
, case_two as (
    select leads.*
         , 1 as half
      from leads
     where nr_term_dt = next_term_dt and nr_eff_dt < next_eff_dt -- covers 2 count= 3
    union
    select leads.*
         , 2 as helf
      from leads
     where nr_term_dt = prev_term_dt and nr_eff_dt > prev_eff_dt
)
, case_two2 as (
    select case_two.*
         , nr_eff_dt as eff_dt
         , case when half = 1 then next_eff_dt - 1
                when half = 2 then nr_term_dt
                end as end_dt
         , case when half = 2 then prev_reason_cd || ', ' || nr_reason_cd
                else nr_reason_cd
                end as final_reason_cd
    from case_two
)
, first_half as (
    select case_id
         , nr_eff_dt
         , nr_term_dt
         , eff_dt
         , end_dt
         , nr_reason_cd
         , final_reason_cd
         , prev_eff_dt
         , prev_term_dt
         , next_eff_dt
         , next_term_dt
         , prev_reason_cd
         , next_reason_cd
      from case_one2
    union
    select case_id
         , nr_eff_dt
         , nr_term_dt
         , eff_dt
         , end_dt
         , nr_reason_cd
         , final_reason_cd
         , prev_eff_dt
         , prev_term_dt
         , next_eff_dt
         , next_term_dt
         , prev_reason_cd
         , next_reason_cd
      from case_two2
)
, first_half2 as (
    select nvl(fh.case_id, lds.case_id) as case_id
         , nvl(fh.eff_dt, lds.nr_eff_dt) as nr_eff_dt
         , nvl(fh.end_dt, lds.nr_term_dt) as nr_term_dt
         , nvl(fh.final_reason_cd, lds.nr_reason_cd) as nr_reason_cd
         , nvl(fh.prev_eff_dt, lds.prev_eff_dt) as prev_eff_dt
         , nvl(fh.prev_term_dt, lds.prev_term_dt) as prev_term_dt
         , nvl(fh.next_eff_dt, lds.next_eff_dt) as next_eff_dt
         , nvl(fh.next_term_dt, lds.next_term_dt) as next_term_dt
         , nvl(fh.prev_reason_cd, lds.prev_reason_cd) as prev_reason_cd
         , nvl(fh.next_reason_cd, lds.next_reason_cd) as next_reason_cd
      from leads lds
      full outer join first_half fh
        on fh.case_id = lds.case_id
       and fh.nr_eff_dt = lds.nr_eff_dt
       and fh.nr_term_dt = lds.nr_term_dt
       and fh.nr_reason_cd = lds.nr_reason_cd
--  where ot.case_id is null or lds.case_id is null
--     or ot.nr_eff_dt is null or lds.nr_eff_dt is null
--     or ot.nr_term_dt is null or lds.nr_term_dt is null
--     or ot.nr_reason_cd is null or lds.nr_reason_cd is null
)
, case_three as (   -- covers case 3    --count = 96 total
    select fh.*
         , 0 as ending
      from first_half2 fh
     where ( (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
         and (next_term_dt > nr_eff_dt and next_term_dt < nr_term_dt) )
        or ( (nr_eff_dt > prev_eff_dt and nr_eff_dt < prev_term_dt)
         and (nr_term_dt > prev_eff_dt and nr_term_dt < prev_term_dt) )
    union all
    select fh.*
         , 1 as ending
      from first_half2 fh
     where (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
         and (next_term_dt > nr_eff_dt and next_term_dt < nr_term_dt)
)
, case_three2 as (
    select case_three.*
         , case when ending = 1 then next_term_dt + 1
                else nr_eff_dt end as eff_dt
         , case when ending = 0 and prev_term_dt < nr_eff_dt or prev_term_dt is null then next_eff_dt - 1
                when ending = 1 then nr_term_dt
                else nr_term_dt end as end_dt
         , case when ending = 1 then nr_reason_cd || ', ' || next_reason_cd 
                else nr_reason_cd
                end as final_reason_cd
      from case_three
    --where case_id = '192194C'
  order by case_id, nr_eff_dt
) 
, case_four as (  -- covers case 4  --count = 36 total
    select fh.*
         , 0 as middle
      from first_half2 fh
     where ( (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
       and (nr_term_dt > next_eff_dt and nr_term_dt < next_term_dt) )
       or (prev_eff_dt < nr_eff_dt
       and (prev_term_dt > nr_eff_dt and prev_term_dt < nr_term_dt) )
    union all
    select fh.*
         , 1 as middle
      from first_half2 fh
     where (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
       and (nr_term_dt > next_eff_dt and nr_term_dt < next_term_dt)
)
, case_four2 as (
    select case_four.*
         , case when middle = 1 then next_eff_dt
                when middle = 0 and (next_eff_dt < nr_term_dt and prev_term_dt < nr_eff_dt or prev_term_dt is null) then nr_eff_dt
                when middle = 0 and prev_term_dt > nr_eff_dt then prev_term_dt + 1
                end as eff_dt
         , case when middle = 1 then nr_term_dt 
                when middle = 0 and next_eff_dt < nr_term_dt and prev_term_dt < nr_eff_dt or prev_term_dt is null then next_eff_dt - 1
                when middle = 0 and prev_term_dt > nr_eff_dt then nr_term_dt
                end as end_dt
         , case when middle = 1 then nr_reason_cd || ', ' || next_reason_cd 
                else nr_reason_cd
                end as final_reason_cd
    from case_four
    --where case_id = '152472C'
    order by case_id, nr_eff_dt
)
, three_four as (
    select case_id
         , nr_eff_dt
         , nr_term_dt
         , eff_dt
         , end_dt
         , nr_reason_cd
         , final_reason_cd
    from case_three2
    union
    select case_id
         , nr_eff_dt
         , nr_term_dt
         , eff_dt
         , end_dt
         , nr_reason_cd
         , final_reason_cd
    from case_four2
)
, result as (
    select nvl(tf.case_id, fh.case_id) as case_id
     , nvl(tf.eff_dt, fh.nr_eff_dt) as eff_dt
     , nvl(tf.end_dt, fh.nr_term_dt) as end_dt
     , nvl(tf.final_reason_cd, fh.nr_reason_cd) as nr_reason_cd
    from first_half2 fh
    full outer join three_four tf
    on tf.case_id = fh.case_id
    and tf.nr_eff_dt = fh.nr_eff_dt
    and tf.nr_term_dt = fh.nr_term_dt
    and tf.nr_reason_cd = fh.nr_reason_cd
    --order by case_id, eff_dt
)
--select * from result 
--where case_id = '154203F'
----or case_id = '175165B'
--or case_id = '139906K'
--order by case_id, eff_dt
select distinct nr_reason_cd from (
    select result.*
         , row_number() over(partition by case_id order by eff_dt) as rn
    from result
              )
--where rn > 4

--debug
--where --case_id = '139906K'
--or case_id = '152472C'
--orcase_id = '165054C'
--or case_id = '266941A' 
--order by case_id, nr_eff_dt

 
 
