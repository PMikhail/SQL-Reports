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

insert into dhs_core.fact_case_managing_rg_etl
        ( dim_case_id
        , dim_managing_org_id
        , eff_dt
        , end_dt
        , last_modified_dt
        )
with case_status
as (
     select fwps.dim_work_product_id
          , fwps.eff_dt
          , eff_dt_status
          , fwps.end_dt
          , end_dt_status
          , slice_seq_num
          , fwps.dim_status_id
          , fwps.dim_status_id_prev
          , status_cd
       from fact_work_prod_status fwps
 inner join dim_status ds
         on ds.dim_status_id = fwps.dim_status_id
      where fwps.work_product_type_cd = 'CASE'
      order by dim_work_product_id, fwps.eff_dt, fwps.end_dt
)
, case_referral as (
     select dim_work_product_id
          , dim_case_id
          , case when status_cd = 'CC' then 0 else nvl(dim_managing_org_id, 9) end as dim_managing_org_id
          , status_cd
          , eff_dt
          --, eff_dt_status
          , end_dt
          --, end_dt_status
          , trunc(completed_dt) as completed_dt
          --, row_number() over(partition by dim_work_product_id order by eff_dt, status_cd) as rn
       from fact_case_cua_referral fccr
  left join case_status cs
         on fccr.dim_case_id = cs.dim_work_product_id
        and completed_dt between eff_dt - 1 and end_dt - 1 --dim_case_id 151465 eff_dt and eff_dt_status is strange
        and status_cd <> 'CC' --again for dim_case_id 151465
       where called_back_ind = 'N'
        and completed_dt is not null 
)
, base_slice as
(
select *
from (
        select three.*
             , case when dim_managing_org_id = 0 then spell_start
                    when spell_start > prev_spell_start and spell_end <= prev_spell_end then null
                    --when next_spell_start < spell_start then next_spell_start
                    when spell_start < next_spell_start - 1 then spell_start 
                    when next_spell_start is null then spell_start
                    when spell_end > next_spell_end - 1 then spell_start
                    when spell_start < next_spell_start then spell_start else null 
               end as eff_date
             , case when dim_managing_org_id = 0 then spell_end
                    when spell_end < prev_spell_end and spell_start >= prev_spell_start then null
                    when spell_end > next_spell_end then spell_end
                    when spell_start < next_spell_start - 1 then spell_end
                    --when next_spell_end > spell_end then next_spell_end 
                    when next_spell_end is null then spell_end else null 
               end as end_date
        from (
                select two.* 
                     , lead(spell_start) over(partition by dim_work_product_id, dim_managing_org_id order by spell_start, spell_end)  as next_spell_start
                     , lag(spell_start) over(partition by dim_work_product_id, dim_managing_org_id order by spell_start, spell_end) as prev_spell_start
                     , lead(spell_end) over(partition by dim_work_product_id, dim_managing_org_id order by spell_start, spell_end)  as next_spell_end
                     , lag(spell_end) over(partition by dim_work_product_iD, dim_managing_org_id order by spell_start, spell_end) as prev_spell_end
                  from (
                        select 
                               distinct dim_work_product_id
                             , dim_managing_org_id
                             , spell_start
                             , spell_end
                          from (
                                select mm.*
                                     , case when status_cd = 'CC' then trunc(eff_dt)
                                            when prev_status2 = 'CC' then trunc(eff_dt)
                                            when prev_eff_dt2 is null then trunc(eff_dt)
                                            when prev_eff_dt2 < eff_dt or prev_eff_dt2 is null then trunc(prev_eff_dt2)
                                            when prev_end_dt2 is null or prev_end_dt2 < eff_dt - 1 or prev_eff_dt2= eff_dt then trunc(prev_eff_dt2) else null
                                       end as spell_start
                                     , case when status_cd = 'CC' then trunc(end_dt)
                                            when end_dt > next_eff_dt2 then null -- overlapping 
                                            when end_dt = to_date('12/31/9999', 'mm/dd/yyyy') then end_dt
                                            when next_status2 = 'CC' then trunc(end_dt)
                                            when next_eff_dt2 is null or next_eff_dt2 > end_dt + 1 then trunc(end_dt)
                                            when next_status2 <> 'CC' and next_eff_dt2 < end_dt + 1 then trunc(next_end_dt2) else null 
                                       end as spell_end
                                     , case when status_cd = 'CC' then 0 else 9 end as dim_managing_org_id
                                  from (
                                        select dim_work_product_id
                                             , status_cd
                                             , lag(status_cd) over(partition by dim_work_product_id order by eff_dt) as prev_status2
                                             , lead(status_cd) over(partition by dim_work_product_id order by eff_dt) as next_status2
                                             , eff_dt
                                             , end_dt
                                             , lag(end_dt) over(partition by dim_work_product_id order by eff_dt) as prev_end_dt2
                                             , lag(eff_dt) over(partition by dim_work_product_id order by eff_dt) as prev_eff_dt2
                                             , lead(eff_dt) over(partition by dim_work_product_id order by eff_dt) as next_eff_dt2
                                             , lead(end_dt) over(partition by dim_work_product_id order by eff_dt) as next_end_dt2
                                             , row_number() over(partition by dim_work_product_id, status_cd, eff_dt order by eff_dt) as rn --remove duplicate of transformed rows
                                           from (
                                                select distinct dim_work_product_id
                                                     , status_cd
                                                     , case when eff_dt > eff_dt_status then eff_dt_status else eff_dt end as eff_dt
                                                     , case when end_dt < end_dt_status then end_dt_status else end_dt end as end_dt
                                                     , lag(eff_dt) over(partition by dim_work_product_id order by eff_dt) as prev_eff_dt1
                                                     , lead(end_dt) over(partition by dim_work_product_id order by eff_dt) as next_end_dt1
                                                     , lag(status_cd) over(partition by dim_work_product_id order by eff_dt) as prev_status1
                                                     , lead(status_cd) over(partition by dim_work_product_id order by eff_dt) as next_status1
                                                  from case_status
                                                 --where-- completed_dt is not null and status_cd = 'CS'
                                                 order by dim_work_product_id, eff_dt, end_dt
                                                ) 
                                            where prev_eff_dt1 is null 
                                               or next_end_dt1 is null  
                                               or status_cd = 'CC' 
                                               or next_status1 = 'CC'
                                               or prev_status1 = 'CC'
                                               or prev_status1 = 'CI'
                                       ) mm
                                       where rn = 1
                                )
                                order by dim_work_product_id, spell_start, spell_end        
                       ) two
                       order by dim_work_product_id, spell_start, spell_end          
             ) three
             order by dim_work_product_id, spell_start, spell_end
     )
     where eff_date is not null and end_date is not null
)
, merged as
(
    select dim_work_product_id
         , dim_managing_org_id
         , 'DHS' as org_name
         , eff_date
         , end_date
      from base_slice
    union
    select dim_work_product_id
         , dim_managing_org_id
         , 'CUA' as org_name
         , completed_dt as eff_date
         , null as end_dt
      from case_referral
    order by dim_work_product_id, eff_date
) 
select dim_case_id
     , dim_managing_org_id
     , to_date(eff_dt, 'mm/dd/yyyy hh24:mi:ss') as eff_dt
     --, end_dt
     , to_date(to_char(end_dt, 'mm/dd/yyyy hh24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss') as end_dt
     , sysdate
     from (
select dim_case_id
     , dim_managing_org_id
     , to_char(eff_dt, 'mm/dd/yyyy hh24:mi:ss') as eff_dt
     , end_dt
     , sysdate
from (
        select dim_case_id
             , dim_managing_org_id
        --     , eff_date
        --     , end_date
        --     , prev_end_date
        --     , next_eff_date
        --     , next_end_date
             , trunc(eff_date) as eff_dt
             , case when end_date is null and prev_end_date is not null and next_end_date is null 
                                 and next_eff_date is null then prev_end_date
                    when end_date is null and prev_end_date is null and next_end_date is null
                                 and next_eff_date is null then to_date('12/31/9999', 'mm/dd/yyyy')
                    when end_date is null and next_end_date is null then next_eff_date
                    when end_date is null and next_eff_date is not null then next_eff_date
                    when end_date > next_eff_date then next_eff_date
                    when end_date is null then prev_end_date
                    else end_date
               end - 1/86400 as end_dt
         from (      
                select dim_work_product_id as dim_case_id
                     , dim_managing_org_id
                     , org_name
                     , eff_date
                     , end_date
                     , lag(eff_date) over(partition by dim_work_product_id order by eff_date) as prev_eff_date
                     , lead(eff_date) over(partition by dim_work_product_id order by eff_date) as next_eff_date
                     , lag(end_date) over(partition by dim_work_product_id order by eff_date) as prev_end_date
                     , lead(end_date) over(partition by dim_work_product_id order by eff_date) as next_end_date 
                     --, lag(org_name) over(partition by dim_work_product_id order by eff_date) as prev_org_name
                  from merged
              ) 
      )
);