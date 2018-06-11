select * from reporting.dim_worker_data_entry
where full_name = 'Vera Days'
or full_name = 'Lynette Davis';
--where dim_worker_id_data_entry = 3887;
--where pstn_typ_shrt_desc = 'Director';

select * from REPORTING.FACT_VISIT_PARTY;

select distinct pstn_typ_shrt_desc from reporting.dim_worker;

select * from reporting.dim_worker
where party_id = 38876;

select wrkr_stts_id, count(distinct pstn_typ_shrt_desc)
from reporting.dim_worker_data_entry
--where pstn_typ_shrt_desc = 'Director'
group by wrkr_stts_id;

select * from reporting.dim_worker_data_entry
where pstn_typ_cd = 'DR' 
and wrkr_stts_shrt_desc like 'Active%';

select * from reporting.dim_worker_data_entry
where full_name = 'Lynette Davis'; --3887

select * from reporting.dim_worker_visit
where full_name = 'Vicente Duvivier';

select * from reporting.fact_position_hierarchy_flat where current_flag = 'Y' and initl_pstn_id_1 = 2359;

select * from reporting.dim_worker where dim_worker_id = 2359;

select * from reporting.fact_report_hierarchy where end_dt > sysdate and worker_pstn_id = 2359;

select full_name
from reporting.dim_worker_data_entry dwde
where pstn_typ_cd = 'DR' 
and wrkr_stts_shrt_desc like 'Active%';



select 
from reporting.fact_visit_party fvp
inner join reporting.dim_worker_data_entry dwde on
fvp.dim_worker_id_data_entry = dwde.dim_worker_id_data_entry
