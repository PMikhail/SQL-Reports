select * from fact_provider_licensure
where dim_provider_id = 30326

select * from dim_provider
where dim_provider_id = 30326


select * from staging.prvdr_lcnc
where prvdr_no = '6745IC0243'




select * from dim_provider where dim_provider_id = 1872 --'6516FH0284'

select * from staging.prvdr_lcnc
where prvdr_no = '6787FH0021'
--and dcmnt_cd = 'AT-COA'
order by apprvl_effctv_dt


select dim_provider_id, dim_document_type_id, eff_dt, end_dt
from fact_provider_licensure1 where dim_provider_id = 1872
