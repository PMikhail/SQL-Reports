with datum as (
select distinct case_no
             , sfx
             , nr_reason_cd
             , nr_eff_dt as eff_dt
             , nr_term_dt as end_dt
         from staging.x_nrmb_chld
         where row_dlt_trnsct_id = 0
)
, eff_dts as
(select case_no
     , sfx
     , eff_dt
     , row_number() over(partition by case_no, sfx order by eff_dt) as rn
 from ( select case_no
            , sfx
            ,eff_dt
        from datum
        union
        select a.case_no
            , a.sfx
            , a.end_dt + 1 as eff_dt
        from datum a
            inner join
            datum b
                on b.case_no = a.case_no
                and b.sfx = a.sfx
                and a.end_dt >= b.eff_dt
                and a.end_dt < b.end_dt
                and not (a.eff_dt = b.eff_dt and b.end_dt = a.end_dt)
       )
)
, end_dts as
(select case_no
    , sfx
    , end_dt
    , row_number() over(partition by case_no, sfx order by end_dt) as rn
 from ( select case_no
            , sfx
            , end_dt
           from datum
        union
        select a.case_no
            , a.sfx
            , a.eff_dt - 1 as end_dt
        from datum a
            inner join
            datum b
                on  b.case_no = a.case_no
                and b.sfx = a.sfx
                and a.eff_dt > b.eff_dt
                and a.eff_dt <= b.end_dt
                and not (a.eff_dt = b.eff_dt and b.end_dt = a.end_dt)
       )
)
, slices as (
select a.case_no
    , a.sfx
    , a.eff_dt
    , b.end_dt
    , a.rn as slice_num
from eff_dts a
    inner join
    end_dts b
        on b.case_no = a.case_no
        and b.sfx = a.sfx
        and b.rn = a.rn   
)
, rows_with_slices as
(
select  s.case_no
    , s.sfx
    , s.eff_dt
    , s.end_dt
    , d.nr_reason_cd
    , row_number() over (partition by s.case_no, s.sfx, s.eff_dt order by  s.end_dt) as rn
from slices s
    inner join
    datum d
        on d.case_no = s.case_no
        and d.sfx = s.sfx
        and s.eff_dt >= d.eff_dt
        and s.end_dt <= d.end_dt
)    
select case_no
    , sfx
    , eff_dt
    , end_dt
    , max(case when rn = 1 then nr_reason_cd else null end) as nr_reason_cd_1
    , max(case when rn = 2 then nr_reason_cd else null end) as nr_reason_cd_2
  from rows_with_slices
  where case_no = 165054 and sfx = 'C'
  group by case_no
    , sfx
    , eff_dt
    , end_dt
       order by case_no, sfx, eff_dt