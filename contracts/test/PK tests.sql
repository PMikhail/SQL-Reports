select *
from temp_cont_rate
where dim_contract_id = 341557
order by end_dt, dim_service_id

select * --srv.id, mdoc_no, eff_dt,term_dt, rate_sp, rate
from staging.x_cont_srv srv
left join staging.x_cont_srv_rates_p1 rp
on rp.parnt_id = srv.id
where srv.id = 19431 or srv.id = 20109

select *
from staging.x_cont_srv_rates_p1
where parnt_id = 19431 or parnt_id = 20109

select count(*) from temp_cont_rate