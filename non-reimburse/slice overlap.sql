select count(*) from staging.x_nrmb_chld --7793


with 
leads as
(
    select nrmb.*
             , lag(nr_eff_dt) over(partition by case_id order by nr_eff_dt, nr_term_dt) as prev_eff_dt
             , lag(nr_term_dt) over(partition by case_id order by nr_eff_dt, nr_term_dt) as prev_term_dt
             , lead(nr_eff_dt) over(partition by case_id order by nr_eff_dt, nr_term_dt) as next_eff_dt
             , lead(nr_term_dt) over(partition by case_id order by nr_eff_dt, nr_term_dt) as next_term_dt
             , lag(nr_reason_cd) over(partition by case_id order by nr_eff_dt, nr_term_dt) as prev_reason_cd
             , lead(nr_reason_cd) over(partition by case_id order by nr_eff_dt, nr_term_dt) as next_reason_cd
             --, 1 as slice_type
    from(
        select case_no || sfx as case_id
             , nr_reason_cd
             , nr_eff_dt
             , nr_term_dt
         from staging.x_nrmb_chld
    ) nrmb
    order by case_id, nr_eff_dt
)
, case_one as (
    select leads.*
         , 1 as half
      from leads
     where (next_eff_dt = nr_eff_dt and next_term_dt > nr_term_dt)
     union
     select leads.*
         , 2 as half
      from leads
     where prev_eff_dt = nr_eff_dt and prev_term_dt < nr_term_dt --covers 1 count = 18
)
, case_one2 as (
    select case_id
         , nr_reason_cd
         , nr_eff_dt
         , nr_term_dt
         --, prev_term_dt
         , case when half = 1 then nr_eff_dt
                when half = 2 then prev_term_dt + 1
                end as eff_dt
         , nr_term_dt as end_dt
         , case when half = 1 then nr_reason_cd || ', ' || next_reason_cd
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
    select case_id
         , nr_reason_cd
         , nr_eff_dt as eff_dt
         , nr_term_dt
         , case when half = 1 then next_eff_dt - 1
                when half = 2 then nr_term_dt
                end as end_dt
         , case when half = 2 then nr_reason_cd || ', ' || prev_reason_cd
                else nr_reason_cd
                end as final_reason_cd
    from case_two
)
, case_three as (   -- covers case 3    --count = 24
    select leads.*
         , 0 as ending
      from leads
     where ( (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
         and (next_term_dt > nr_eff_dt and next_term_dt < nr_term_dt) )
        or ( (nr_eff_dt > prev_eff_dt and nr_eff_dt < prev_term_dt)
         and (nr_term_dt > prev_eff_dt and nr_term_dt < prev_term_dt) )
    union all
    select leads.*
         , 1 as ending
      from leads
     where (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
         and (next_term_dt > nr_eff_dt and next_term_dt < nr_term_dt)
)
, case_three2 as (
    select case_id
         , nr_reason_cd
         , nr_eff_dt
         , nr_term_dt
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
, case_four as (  -- covers case 4  --count = 14
    select leads.*
         , 0 as middle
      from leads
     where ( (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
       and (nr_term_dt > next_eff_dt and nr_term_dt < next_term_dt) )
       or (prev_eff_dt < nr_eff_dt
       and (prev_term_dt > nr_eff_dt and prev_term_dt < nr_term_dt) )
    union all
    select leads.*
         , 1 as middle
      from leads
     where (next_eff_dt > nr_eff_dt and next_eff_dt < nr_term_dt)
       and (nr_term_dt > next_eff_dt and nr_term_dt < next_term_dt)
)
, case_four2 as (
    select case_id
         , nr_reason_cd
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
select count(*) from (--case_id, nr_eff_dt, nr_term_dt, nr_reason_cd, count(*) from (
select case_id, nr_eff_dt, nr_term_dt, nr_reason_cd
from case_one
union
select case_id, nr_eff_dt, nr_term_dt, nr_reason_cd
from case_two
union 
select case_id, nr_eff_dt, nr_term_dt, nr_reason_cd
from case_three
--union all 
--select case_id, nr_eff_dt, nr_term_dt, nr_reason_cd
--from case_four
)


--
right join
(
select case_id, eff_dt, end_dt, final_reason_cd
from case_one2
union
select case_id, eff_dt, end_dt, final_reason_cd
from case_two2
union
select case_id, eff_dt, end_dt, final_reason_cd
from case_three2
) bb
on bb.case_id = aa.case_id
and bb.eff_dt = aa.eff_dt
and bb.end_dt = aa.end_dt
and bb.final_reason_cd = aa.final_reason_cd
--where aa.case_id is null

select count(*)
       --l.case_id
     --, l.nr_reason_cd
  from leads ld  
inner join case_one one
on one.case_id = ld.case_id
and one.nr_reason_cd = ld.nr_reason_cd
and one.nr_eff_dt = ld.nr_eff_dt
and one.nr_term_dt = ld.nr_term_dt
inner join case_two two
on two.case_id = ld.case_id
and two.nr_reason_cd = ld.nr_reason_cd
and two.nr_eff_dt = ld.nr_eff_dt
and two.nr_term_dt = ld.nr_term_dt
inner join case_three three
on three.case_id = ld.case_id
and three.nr_reason_cd = ld.nr_reason_cd
and three.nr_eff_dt = ld.nr_eff_dt
and three.nr_term_dt = ld.nr_term_dt
--where two.case_id is null and one.case_id is null and three.case_id is null



    select case_id
         , nr_reason_cd
         , nr_eff_dt
         , nr_term_dt
         , slice_type
         , case when slice_type = 1 then prev_term_dt + 1
                when slice_type = 2 then nr_eff_dt
           end as eff_dt 
         , case when slice_type = 1 then nr_term_dt
                when slice_type = 2 then next_eff_dt - 1
           end as end_dt
         , case when slice_type = 1 then nr_reason_cd
                when slice_type = 2 then prev_reason_cd
           end as final_reason_cd 
    from remain

)
select * from remain2
 
--case_id = '266941A'  
