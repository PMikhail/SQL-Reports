select * from (
select dim_case_party_id
     , nr_eff_dt as eff_dt
     , nr_term_dt as end_dt
     , nrmb.case_id
     , dim_nonreimburse_reason_id
     , count(*) over(partition by dim_case_party_id, nr_term_dt order by 1) as cnt
from
(

select case_id, end_dt, count(*) from(

select * from(
select this.*, count(*) over(partition by case_id, end_dt order by eff_dt) as cnt from(

select case_id
     , nr_reason_cd
     , nr_eff_dt as eff_dt
     , case when nr_term_dt > next_eff_dt then next_eff_dt - 1
            else nr_term_dt end as end_dt
    from(
    select case_no || sfx as case_id
         , nr_eff_dt
         , nr_term_dt
         , nr_reason_cd
         , lag(nr_eff_dt) over(partition by case_no, sfx order by nr_eff_dt) as prev_eff_dt
         , lag(nr_term_dt) over(partition by case_no, sfx order by nr_eff_dt) as prev_term_dt
         , lead(nr_term_dt) over(partition by case_no, sfx order by nr_eff_dt) as next_term_dt
         , lead(nr_eff_dt) over(partition by case_no, sfx order by nr_eff_dt) as next_eff_dt
    from staging.x_nrmb_chld nrmb
    where row_dlt_trnsct_id = 0
    and case_no = '165054'    
    )
    
    ) this
    )
    where cnt > 1
    
    
    )
    group by case_id, end_dt
    having count(*) > 1
    
) nrmb
left join (
            select case_no || suffix as case_id
                 , dim_case_party_id
            from dim_case_party
          ) dcp
on dcp.case_id = nrmb.case_id
left join (
            select dim_nonreimburse_reason_id
                 , nonreimburse_reason_cd 
            from dim_nonreimburse_reason
          ) dnr
on nonreimburse_reason_cd = nr_reason_cd
)
--where dim_case_party_id = '192778'



------------------------------------------------
select * from dim_case_party 
where case_no = 273991

select case_no, sfx, nr_eff_dt, count(*) from staging.x_nrmb_chld nrmb
where row_dlt_trnsct_id = 0
group by case_no, sfx, nr_eff_dt
having count(*) > 1

select * from dim_nonreimburse_reason

select * 
from staging.x_nrmb_chld
where case_no = '165054'
and row_dlt_trnsct_id = 0

select distinct nr_reason_cd, nr_eff_dt, 