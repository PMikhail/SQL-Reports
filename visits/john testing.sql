select count(*)
from
(
select
dw_d.LAST_NAME || ', ' || dw_d.FIRST_NAME As dir_name,
dw_a.LAST_NAME || ', ' || DW_A.FIRST_NAME As ADMIN_name,
dw_S.LAST_NAME || ', ' || dw_S.FIRST_NAME As SUPER_name,
dw_W.LAST_NAME || ', ' || dw_W.FIRST_NAME As WORK_name
,Case When dp.CUA_CD = 'NAPP' Then 'DHS'
Else dp.CUA_LONG_DESC End As org_name,
d2.*,d1.*,f1.*, nvl(service_group_name, 'No Service') as service_name
from reporting.FACT_CASE_CHILD_STATUS f1 
inner join reporting.dim_status d1 
on F1.DIM_STATUS_ID = d1.dim_status_id
inner join reporting.dim_child d2
on F1.DIM_CHILD_ID = D2.DIM_CHILD_ID
left join 
(select Dim_child_Id, worker_Dim_worker_Id, director_Dim_worker_Id, admin_Dim_worker_Id, supervisor_Dim_worker_Id, current_flag , WORKER_DIM_POSITION_ID
from REPORTING.FACT_CASE_CHILD_STATUS_HIER) FCCSH
ON d2.dim_child_id = FCCSH.DIM_Child_ID and FCCSH.current_flag = 'Y'
INNER JOIN REPORTING.DIM_POSITION DP
ON FCCSH.WORKER_DIM_POSITION_ID = DP.DIM_POSITION_ID
INNER JOIN REPORTING.DIM_WORKER DW_D
ON DW_D.DIM_WORKER_ID = FCCSH.DIRECTOR_DIM_WORKER_ID
INNER JOIN REPORTING.DIM_WORKER DW_A
ON DW_A.DIM_WORKER_ID = FCCSH.ADMIN_DIM_WORKER_ID
INNER JOIN REPORTING.DIM_WORKER DW_S
ON DW_S.DIM_WORKER_ID = FCCSH.SUPERVISOR_DIM_WORKER_ID
INNER JOIN REPORTING.DIM_WORKER DW_W
ON DW_W.DIM_WORKER_ID =FCCSH.WORKER_DIM_WORKER_ID
inner join reporting.fact_child_service fcs
on f1.dim_child_id = FCS.DIM_CHILD_ID
inner join reporting.dim_service dserv
on fcs.dim_service_id = dserv.dim_service_id
where to_date('8/26/2015', 'mm/dd/yyyy') between fcs.eff_dt and fcs.end_dt
and fcs.current_flag = 'Y'
and 20150826 between dim_date_id_end_of_day_eff and dim_date_id_end_of_day_end and d1.status_cd IN ('CO','CS')
and Case When dp.CUA_CD = 'NAPP' Then 'DHS'
Else dp.CUA_LONG_DESC End = 'DHS'
order by dw_W.LAST_NAME || ', ' || dw_W.FIRST_NAME
)
--group by child_id
--count=2921




select count(*)
from
(
    select
    dw_d.LAST_NAME || ', ' || dw_d.FIRST_NAME As dir_name,
    dw_a.LAST_NAME || ', ' || DW_A.FIRST_NAME As ADMIN_name,
    dw_S.LAST_NAME || ', ' || dw_S.FIRST_NAME As SUPER_name,
    dw_W.LAST_NAME || ', ' || dw_W.FIRST_NAME As WORK_name
    ,Case When dp.CUA_CD = 'NAPP' Then 'DHS'
    Else dp.CUA_LONG_DESC End As org_name,
    d2.*,d1.*,f1.* from reporting.FACT_CASE_CHILD_STATUS f1 inner join reporting.dim_status d1 on F1.DIM_STATUS_ID = d1.dim_status_id
    inner join reporting.dim_child d2 on F1.DIM_CHILD_ID = D2.DIM_CHILD_ID
    left join (select Dim_child_Id, worker_Dim_worker_Id, director_Dim_worker_Id, admin_Dim_worker_Id, supervisor_Dim_worker_Id, current_flag , WORKER_DIM_POSITION_ID
    from REPORTING.FACT_CASE_CHILD_STATUS_HIER) FCCSH
    ON d2.dim_child_id = FCCSH.DIM_Child_ID and FCCSH.current_flag = 'Y'
    INNER JOIN REPORTING.DIM_POSITION DP
    ON FCCSH.WORKER_DIM_POSITION_ID = DP.DIM_POSITION_ID
    INNER JOIN REPORTING.DIM_WORKER DW_D
    ON DW_D.DIM_WORKER_ID = FCCSH.DIRECTOR_DIM_WORKER_ID
    INNER JOIN REPORTING.DIM_WORKER DW_A
    ON DW_A.DIM_WORKER_ID = FCCSH.ADMIN_DIM_WORKER_ID
    INNER JOIN REPORTING.DIM_WORKER DW_S
    ON DW_S.DIM_WORKER_ID = FCCSH.SUPERVISOR_DIM_WORKER_ID
    INNER JOIN REPORTING.DIM_WORKER DW_W
    ON DW_W.DIM_WORKER_ID =FCCSH.WORKER_DIM_WORKER_ID
    where 20150826 between dim_date_id_end_of_day_eff and dim_date_id_end_of_day_end and d1.status_cd IN ('CO','CS')
    and Case When dp.CUA_CD = 'NAPP' Then 'DHS'
    Else dp.CUA_LONG_DESC End = 'DHS'
    order by dw_W.LAST_NAME || ', ' || dw_W.FIRST_NAME
)