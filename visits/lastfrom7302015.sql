select *
from
(
    select 
    visits.dim_child_id,
    (dchild.first_nm || ' ' || dchild.last_nm) as child_full_nm,
    (to_char(dcase.case_no) || '-' || dchild.suffix) as child_id
    , visits.*
    
    from reporting.fact_case_child_status_hier hier 
    inner join reporting.dim_status stat on
    hier.dim_status_id = stat.dim_status_id and status_cd = 'CO'
    left join
    (
        select * from
            (
                select fvp.*, row_number() over(partition by fvp.dim_child_id order by fvp.end_dt) as rn
                from reporting.fact_visit_party fvp
                where fvp.end_dt < SYSDATE
            )
        where rn = 1
    ) visits on
    visits.dim_child_id = hier.dim_child_id
    inner join
    (
        select atype.dim_assignment_type_id                 --To filter on queues. 
        from reporting.dim_assignment_type atype
        --where queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN'
    ) atype on
    atype.dim_assignment_type_id = hier.dim_assignment_type_id
    inner join reporting.dim_case dcase on
    hier.dim_case_id = dcase.dim_case_id
    --inner join reporting.dim_child dchild on    dchild.dim_child_id = hier.dim_child_id
    inner join reporting.dim_case_party dchild on    dchild.dim_case_party_id = hier.dim_child_id
    where sysdate between hier.eff_dt and hier.end_dt
    --and dim_visit_status_id <> 2
        and dchild.case_no = '149758' and dchild.suffix = 'G'
);


select * from reporting.dim_

select * from reporting.fact_case_child_status_hier;

select count(*), count(distinct dim_child_id), count(distinct dim_case_id)
    from reporting.fact_case_child_status_hier hier 
    inner join reporting.dim_status stat on
    hier.dim_status_id = stat.dim_status_id and status_cd = 'CO'
        where sysdate -1 between hier.eff_dt and hier.end_dt

select stat.*
    from reporting.fact_case_child_status_hier hier 
     inner join reporting.dim_status stat on
    hier.dim_status_id = stat.dim_status_id 
    inner join reporting.dim_case_party dcp
        on dcp.dim_case_party_id = hier.dim_child_id
            where sysdate -1 between hier.eff_dt and hier.end_dt
    and dcp.case_no = '149758' and suffix = 'G'
        
        
where child_id = '149758-G';

select * from reporting.dim_child dchild
left join reporting.fact_visit_party fvp on
fvp.dim_child_id = dchild.dim_child_id
where first_nm like 'Fajr';

select * from reporting.fact_visit_party