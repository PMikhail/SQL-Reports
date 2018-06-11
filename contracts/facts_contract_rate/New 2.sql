select serv_type, count(*) from staging.x_cont_srv
where row_dlt_trnsct_id = 0
group by serv_type 
order by 1

SELECT * FROM STAGING.X_CONT_SRV WHERE ROW_DLT_TRNSCT_ID = 0 and serv_type = 'CF'

select 
from staging.x_cont xc
left join staging.x_cont_srv srv
on srv.parnt_id = xc.id
where xc.row_dlt_trnsct_id = 0
and srv.row_dlt_trnsct_id = 0

select * from staging.x_cont where row_dlt_trnsct_id = 0

dhs_core.dim_contract
staging.x_cont_srv

select * from staging.x_cont_srv_rates_p1 where row_dlt_trnsct_id = 0


select count(*)
from staging.x_cont_srv srv
join staging.x_cont_srv_rates_p1 rates
on srv.id = rates.parnt_id
and SRV.ROW_DLT_TRNSCT_ID = 0
where rates.row_dlt_trnsct_id = 0

select count(*) from staging.x_cont xc
inner join staging.x_cont_srv srv
on SRV.MDOC_NO = xc.mdoc_no
and srv.row_dlt_trnsct_id = 0
where xc.row_dlt_trnsct_id = 0

select count(*) from dhs_core.dim_contract xc
inner join staging.x_cont_srv srv
on SRV.MDOC_NO = XC.CONTRACT_DOCUMENT_ID
and srv.row_dlt_trnsct_id = 0
inner join staging.x_cont_srv_rates_p1 rt
on RT.PARNT_ID = srv.id
and RT.ROW_DLT_TRNSCT_ID = 0
left join ( select *
              from dhs_core.dim_service ) ds
on DS.SERV_CD = SRV.SERV_CD
and nvl(SRV.Rate_CAT, -1) = nvl(DS.Rate_CATEGORY, -1)
and nvl(SRV.EMGCY_IND, 'N') = DS.EMERGENCY_IND

select count(*)
from dhs_core.dim_contract dc
inner join ( select *
               from staging.x_cont_srv srv
              where row_dlt_trnsct_id = 0) srv
on srv.mdoc_no = dc.contract_document_id
inner join staging.x_cont_srv_rates_p1 rp
on rp.parnt_id = srv.id
and rp.row_dlt_trnsct_id = 0
left join ( select *
              from dhs_core.dim_service ) ds
on DS.SERV_CD = SRV.SERV_CD
and nvl(SRV.Rate_CAT, -1) = nvl(DS.Rate_CATEGORY, -1)
and nvl(SRV.EMGCY_IND, 'N') = DS.EMERGENCY_IND




--45360
select *
from staging.x_cont_srv srv
where SRV.ROW_DLT_TRNSCT_ID = 0
and id = 2351

select * from staging.x_cont_srv
where id = 10007501
