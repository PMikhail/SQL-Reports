select count(*) from reporting.fact_visit_party; --2,277,669

select count(*) from reporting.fact_case_child_status_hier where SYSDATE between eff_dt and end_dt; --299,037




select count(*) from
(
select (wwork.last_name || ', ' || wwork.first_name) as wrk_name, --worker name
(swork.last_name || ', ' || swork.first_name) as sup_name, --supervisor name
(awork.last_name || ', ' || awork.first_name) as adm_name, --administrator name
(dwork.last_name || ', ' || dwork.first_name) as dir_name, --director name
(to_char(dcase.case_no) || '_' || dchild.suffix) as child_id, dcase.case_name,
(dchild.first_nm || ' ' || dchild.last_nm) as child_full_nm, dchild.brth_dt,
trunc(months_between(SYSDATE, dchild.brth_dt)/12) as age, --service_group,
row_number() over(partition by fvp.dim_child_id order by start_dt) as rowdate,
fvp.end_dt as last_visit, fvp.dim_child_id, service_group_name
from reporting.fact_case_child_status_hier hier
inner join reporting.fact_visit_party fvp on
hier.dim_child_id = fvp.dim_child_id 
    and SYSDATE between hier.eff_dt and hier.end_dt
    and fvp.end_dt < SYSDATE
inner join reporting.dim_status stat on
hier.dim_status_id = stat.dim_status_id and status_cd = 'CO'
inner join (
            select atype.dim_assignment_type_id                 --To filter on queues. 
            from reporting.dim_assignment_type atype
            where queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN'
            ) atype on
atype.dim_assignment_type_id = hier.dim_assignment_type_id 
inner join (
            select service_group_name, dim_child_id from        --To grab service name. Should not affect count of total rows but does right now
            (
                select x.dim_child_id, dserv.service_group_name
                , row_number() over(partition by dim_child_id order by service_group_rank) as rn
                from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
                where sysdate between eff_dt and end_dt
            )
            where rn = 1
           ) serv on
serv.dim_child_id = hier.dim_child_id 
inner join reporting.dim_case dcase on
dcase.dim_case_id = fvp.dim_case_id
inner join reporting.dim_child dchild on
dchild.dim_child_id = hier.dim_child_id
inner join reporting.dim_worker wwork on
hier.worker_dim_worker_id = wwork.dim_worker_id
inner join reporting.dim_worker swork on
hier.worker_dim_worker_id = swork.dim_worker_id
inner join reporting.dim_worker awork on
hier.worker_dim_worker_id = awork.dim_worker_id
inner join reporting.dim_worker dwork on
hier.worker_dim_worker_id = dwork.dim_worker_id
where dim_visit_status_id <> 2--2 is completed visits
)
where rowdate = 1;

select * from reporting.fact_case_child_status_hier