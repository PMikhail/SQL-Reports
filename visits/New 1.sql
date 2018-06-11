select * from--dim_child_id) from -- service_group_name, dim_child_id from 
        (
            select x.dim_child_id, dserv.service_group_name
            , row_number() over(partition by dim_child_id order by service_group_rank) as rn
            from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
            where sysdate between eff_dt and end_dt
        )where rn = 1;
        
        select * from reporting.fact_visit_party fvp
        inner join reporting.fact_case_child_status_hier hier on
        fvp.dim_child_id = hier.dim_child_id
        inner join reporting.dim_status stat on
        hier.dim_status_id = stat.dim_status_id
        where fvp.dim_child_id = 957672 order by fvp.end_dt;
        
        select hier.*, serv.* from reporting.fact_case_child_status_hier hier
        inner join 
        (
            select * from
            (
                select x.dim_child_id, dserv.service_group_name
                , row_number() over(partition by dim_child_id order by service_group_rank) as rn
                from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
                where sysdate between eff_dt and end_dt
            )where rn = 1
        ) serv on
        hier.dim_child_id = serv.dim_child_id
        inner join reporting.dim_status stat on
        hier.dim_status_id = stat.dim_status_id
        inner join REPORTING.FACT_CASE_CHILD_STATUS_ASSIGN fccsa on
        FCCSA.DIM_CHILD_ID = hier.dim_child_id
        inner join REPORTING.DIM_ASSIGNMENT_TYPE datype on
        DATYPE.DIM_ASSIGNMENT_TYPE_ID = FCCSA.DIM_ASSIGNMENT_TYPE_ID
        inner join reporting.fact_visit_party fvp on
        hier.dim_child_id = fvp.dim_child_id
        where status_cd = 'CO'
        and (queue_cd like 'CUA%' or queue_cd = 'NAPP' or queue_cd = 'UNKN')
        and sysdate between hier.eff_dt and hier.end_dt;
        
        select * from reporting.fact_visit_party fvp
        where dim_child_id = 957672
        order by end_dt;
        
        
        select * from reporting.fact_visit_party;
        select * from reporting.fact_case_child_status_hier;
        
        select * from
            (
                select x.dim_child_id, dserv.service_group_name
                , row_number() over(partition by dim_child_id order by service_group_rank) as rn
                from reporting.fact_child_service x inner join reporting.dim_service dserv on dserv.dim_service_id = x.dim_service_id
                where sysdate between eff_dt and end_dt
            )where dim_child_id = 957672;
            
            select * from reporting.fact_child_service where dim_child_id = 957672;
            
            
/*
    To grab queues by joining fact case child status assign with dim queues and finally visits
*/
select * from reporting.fact_case_child_status_assign factassign
inner join reporting.dim_assignment_type assigntype on
factassign.dim_assignment_type_id = assigntype.dim_assignment_type_id;

select distinct queue_long_desc, queue_cd from reporting.dim_assignment_type order by 1;

select * from reporting.dim_assignment_type
where queue_long_desc = 'Unknown';


select * from reporting.fact_case_child_status_assign factassign
inner join reporting.dim_assignment_type assigntype on
factassign.dim_assignment_type_id = assigntype.dim_assignment_type_id
where assigntype.dim_assignment_type_id = 33