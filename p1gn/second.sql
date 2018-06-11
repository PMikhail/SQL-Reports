select dim_child_id, service_seq_num, dim_service_id, end_dt, end_dt_service,
row_number() over(partition by dim_child_id order by service_seq_num desc) as rn
from reporting.fact_primary_service

select * from reporting.fact_primary_service where end_dt_service is not null order by end_dt_service desc

select *
from (
    select case_nmbr || '-' || child_sfx as child_id, plc.*
    from staging.placement_services_as_of plc
    )
--where rn = 1

select count(*) from (
select cua_cd, fps.dim_child_id, child_id, fps.dim_service_id, fps2.* 
from reporting.fact_primary_service fps
left join (
    select * from (
        select dim_child_id, service_seq_num, ff.dim_service_id, end_dt, end_dt_service, serv_cd,
        row_number() over(partition by dim_child_id order by service_seq_num desc) as rn,
        pass_reason_code || ' - ' || pass_reason_long_name as pass_reason
        from reporting.fact_primary_service ff inner join reporting.dim_service_status dss
        on dss.dim_service_status_id = ff.dim_service_status_id
        inner join reporting.dim_service ds on ds.dim_service_id = ff.dim_service_id
        where service_seq_num is not null
    ) where rn = 1
) fps2
on fps2.dim_child_id = fps.dim_child_id
inner join reporting.dim_child dchild
on dchild.dim_child_id = fps.dim_child_id
inner join reporting.dim_service_status dss
on dss.dim_service_status_id = fps.dim_service_status_id
inner join reporting.fact_case_child_assign fcca
on fcca.dim_child_id = fps.dim_child_id
inner join reporting.dim_position dpos
on dpos.dim_position_id = fcca.dim_position_id
inner join reporting.dim_service dserv
on dserv.dim_service_id = fps.dim_service_id
where to_date('10/15/2015', 'mm/dd/yyyy') between fps.eff_dt and fps.end_dt
--and dserv.service_group_name = 'Zero Rate CUA'
and (dserv.serv_cd is null or dserv.serv_cd = 'P1GN')
and dim_status_id in (select dim_status_id from reporting.dim_status where status_cd = 'CO')
and to_date('10/15/2015', 'mm/dd/yyyy') between fcca.eff_dt and fcca.end_dt
and cua_cd <> 'NAPP'
--and child_id = '230489-B'
order by cua_cd
)


select * from reporting.dim_child cd
inner join reporting.dim_case cs
on CD.DIM_WORK_PRODUCT_ID = Cs.dim_case_id
where dim_case_id = 211598

select * from reporting.dim_case where case_no = '195723'

select * from reporting.fact_case_child_assign fcca

inner join reporting.dim_position dp
on dp.dim_position_id = fcca.dim_position_id

--and case when serv_cd is null then service_seq_num = (

    else service_seq_num = max(sevice_seq_num)
--group by cua_cd
--order by cua_cd

select * from reporting.fact_primary_service where dim_child_id = 257980 order by service_seq_num desc



select *
from reporting.dim_child
where child_id = '216301-E'

select * from reporting.fact_primary_service
where dim_child_id = 501121
order by end_dt desc

select *
from reporting.fact_primary_service
where dim_child_id = 501121
order by end_dt desc

select *
from reporting.dim_service
where dim_service_id = 140