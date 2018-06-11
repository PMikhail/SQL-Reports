select fvp.end_dt, fvp.dim_child_id
from reporting.fact_visit_party fvp
inner join reporting.dim_child dchild on
dchild.dim_child_id = fvp.dim_child_id
where child_id = '286825-B'
order by fvp.end_dt desc


select dim_child_id, count(end_dt) as visits_num
from reporting.fact_visit_party fvp
where extract(month from fvp.end_dt) = extract(month from sysdate)
and dim_child_id = 800432
group by dim_child_id

select end_dt
from reporting.fact_visit_party 
where dim_child_id = 800432
and extract(month from end_dt) = extract(month from sysdate)
and extract(year from end_dt) = extract(year from sysdate)
order by end_dt desc