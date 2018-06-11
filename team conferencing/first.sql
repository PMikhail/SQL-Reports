select *
from staging.cnfrnc_log clog
inner join STAGING.WRK_PRDCT_FORM wpf
on WPF.WRK_PRDCT_ID = clog.wrk_prdct_id
where cnfrnc_rqrd_dt between to_date('12/1/2014', 'mm/dd/yyyy') 
and to_date('1/1/2015', 'mm/dd/yyyy')
order by clog.wrk_prdct_id, cnfrnc_rqrd_dt desc

select *
from staging.cnfrnc_log


select * from staging.cnfrnc_lctn

select *
from staging.schdl
where row_crtn_dt between to_date('12/1/2014', 'mm/dd/yyyy') 
and to_date('1/1/2015', 'mm/dd/yyyy')

select * from staging.schdl_cnfrnc_party

select * from staging.cd_schdl_supr_typ

select distinct cd 
from staging.cd_schdl_typ schtyp
--inner join staging.cd_schdl_supr_typ supr
--on supr.cd = schtyp.cd

select * from staging.cd_cnfrnc_reasn
