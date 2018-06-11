select *
from
(
    select case when work_product_type_cd = 'CASE' then dc.dim_child_id else dc_2.dim_child_id end as dim_child_id,
    max(fvp.dim_date_id_visit) as max_dim_date_id
    from reporting.fact_visit_party fvp
    inner join
    reporting.dim_child dc
    on dc.dim_child_id = fvp.dim_child_id
    left join
    reporting.dim_work_product dwp
    on dwp.dim_work_product_id = dc.dim_work_product_id
    and dwp.work_product_type_cd = 'RPT'
    left join
    reporting.dim_child dc_2
    on dc_2.dim_work_product_id = dwp.dim_case_id_parent
    and dc_2.party_id = dc.party_id
    where (work_product_type_cd = 'CASE' or dc_2.dim_child_id is not null)
    group by case when work_product_type_cd = 'CASE' then dc.dim_child_id else dc_2.dim_child_id end
);

select count(distinct queue_cd) from REPORTING.DIM_ASSIGNMENT_TYPE where queue_cd like '%CUA%'