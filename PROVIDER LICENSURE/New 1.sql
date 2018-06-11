select count(*)
from staging.prvdr_lcnc pl
left join dhs_core.dim_provider dp
on pl.prvdr_no = dp.prv_no
left join dhs_core.dim_document_set ds1
on nvl(ds1.doc_cd_list,'_') = nvl(pl.nn_cmplnc_rson,'_')
and nvl(ds1.other_text, '_') = nvl(pl.nn_cmplnc_oth, '_')
left join dhs_core.dim_document_set ds2
on nvl(ds2.doc_cd_list,'_') = nvl(pl.awtng_dcmntn,'_')
left join dim_document_type dt
on dt.document_cd = pl.dcmnt_cd
--WHERE PL.ID = 101704
--order by prv_no, apprvl_end_dt

select * from staging.prvdr_lcnc where id = 101704

select * from dim_document_set where doc_cd_list = 'NCR-OTH'

select count(distinct prvdr_no) from staging.prvdr_lcnc

select  from (

select prvdr_no, dcmnt_cd, count() from staging.prvdr_lcnc group by prvdr_no, dcmnt_cd

select * from staging.prvdr_lcnc where prvdr_no = '6606IC0196' 


select count(*) from temp_fact_prov_lcnc

select count(*) from temp_fact_prov_lcnc one
full outer join (select distinct dim_provider_id, dim_document_type_id, dim_date_id_end
                    from temp_fact_prov_lcnc) two
on two.dim_provider_id = one.dim_provider_id
and two.dim_document_type_id = one.dim_document_type_id
and two.dim_date_id_end = one.dim_date_id_end 
where two.dim_provider_id is null
or one.dim_provider_id is null

select count(*) from (
select distinct dim_provider_id, dim_document_type_id, dim_date_id_end
                    from temp_fact_prov_lcnc )


-------------------------------------------------



select * from (
    select pl.*
         , case when dim_yn_id_is_licensed = 1
                then DD1.DIM_DATE_ID 
                else null
           end as dim_date_id_licensed_from
         , case when dim_yn_id_is_licensed = 1
                then dd2.dim_date_id
                else null
           end as dim_date_id_licensed_to
    from(
        select    dim_provider_id
                , dim_document_type_id
                , implied_start_date_ind
                , dd1.dim_date_id as dim_date_id_approval_eff
                , dd1.dim_date_id as dim_date_id_approval_end
                , dim_date_id_eff as eff_dt
                , dim_document_set_id_missing
                , dim_document_set_id_noncomp
                , dim_yn_id_is_licensed
                , case when dim_date_id_end = ( lead(dim_date_id_end) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff, dim_date_id_end) )
                       then lead(dim_date_id_eff) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff)  - 1
                       else dim_date_id_end
                  end as end_dt --will also be as dim_date_id_licensed_to
          from (
                select     prvdr_no
                         , dim_provider_id
                         , dim_document_type_id
                         , case when pl.apprvl_actual_end_dt is not null 
                                then pl.apprvl_actual_end_dt--to_number(to_char(pl.apprvl_actual_end_dt, 'yyyymmdd'))
                                when pl.apprvl_actual_end_dt is null and pl.apprvl_end_dt is not null
                                then pl.apprvl_end_dt--to_number(to_char(pl.apprvl_end_dt, 'yyyymmdd'))
                                else to_date('12/31/9999', 'MM/DD/YYYY')
                           end as dim_date_id_end
                         , case when pl.apprvl_effctv_dt is null and pl.apprvl_actual_end_dt is null
                                then null
                                when pl.apprvl_effctv_dt is null and pl.apprvl_actual_end_dt is not null
                                then to_date('1/1/1900', 'MM/DD/YYYY')
                                else pl.apprvl_effctv_dt
                           end as dim_date_id_eff 
                         , case when pl.apprvl_effctv_dt is null and pl.apprvl_actual_end_dt is not null
                                then 'Y' else 'N'
                           end as implied_start_date_ind
                         , DS2.DIM_DOCUMENT_SET_ID as dim_document_set_id_missing
                         , ds1.dim_document_set_id as dim_document_set_id_noncomp
                         , case when ds1.num_docs = 0 and ds2.num_docs = 0 then 1
                                else 0
                           end as dim_yn_id_is_licensed
                from ( select distinct awtng_dcmntn
                                     , nn_cmplnc_rson
                                     , nn_cmplnc_oth
                                     , prvdr_no
                                     , dcmnt_cd
                                     , apprvl_effctv_dt
                                     , apprvl_end_dt
                                     , apprvl_actual_end_dt 
                                 from staging.prvdr_lcnc ) pl
                left join dim_document_type dt
                on pl.dcmnt_cd = dt.document_cd
                left join dim_provider dp
                on dp.prv_no = pl.prvdr_no
                left join DHS_CORE.DIM_DOCUMENT_SET ds1
                on nvl(ds1.doc_cd_list,'_') = nvl(pl.nn_cmplnc_rson,'_')
                and nvl(ds1.other_text, '_') = nvl(substr(pl.nn_cmplnc_oth, 1, 50), '_')
                inner JOIN DHS_CORE.DIM_DOCUMENT_SET DS2 on nvl(ds2.doc_cd_list,'_') = nvl(pl.AWTNG_DCMNTN,'_') 
                                        ) pl
            INNER JOIN DIM_DATE DD1
            ON DD1.DT = PL.dim_date_id_eff
            INNER JOIN DIM_DATE DD2
            ON DD2.DT = PL.dim_date_id_end ) pl
    inner join dim_date dd1
    on dd1.dt = eff_dt
    inner join dim_date dd2
    on dd2.dt = end_dt )
    

                                        
                                    
--WHERE RN > 1
--where dim_date_id_eff = to_date('1/1/1900', 'mm/dd/yyyy')
--where implied_start_date_ind = 'Y'
--ORDER BY DIM_PROVIDER_ID, DIM_DOCUMENT_TYPE_ID, end_dt 

select * from staging.prvdr_lcnc 
where prvdr_no = '6553FH0817' and dcmnt_cd = 'AT-COA'
order 

select * from dim_document_type




select count(*) from (
select distinct awtng_dcmntn, nn_cmplnc_rson, nn_cmplnc_oth, prvdr_no, dcmnt_cd, apprvl_effctv_dt, apprvl_end_dt, apprvl_actual_end_dt 
from staging.prvdr_lcnc) --where prvdr_no = '6977IC0150'

select count(*), this.* 
from (
select prvdr_no, 
dcmnt_cd, apprvl_actual_end_dt 
, lead(apprvl_end_dt, 1) over (order by apprvl_end_dt) as end_dt 
from staging.prvdr_lcnc ) this
group by prvdr_no, dcmnt_cd, 
apprvl_actual_end_dt, end_dt
having count(*) > 1
order by prvdr_no, dcmnt_cd, end_dt


select * from staging.prvdr_lcnc
where apprvl_effctv_dt > apprvl_end_dt

select * from staging.prvdr_lcnc where prvdr_no = '6037FH0147' order by dcmnt_cd, apprvl_end_dt, apprvl_effctv_dt