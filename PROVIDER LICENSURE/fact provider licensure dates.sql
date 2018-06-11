   select pl2.*
              , case when dim_yn_id_is_licensed = 1
                     then dd11.dim_date_id 
                     else 0
                end as dim_date_id_licensed_from
              , case when dim_yn_id_is_licensed = 1
                     then dd12.dim_date_id
                     else 0
                end as dim_date_id_licensed_to
                
                
select count(*) from (                 
select pl5.*
     , case when lic_start_dt is null then prev_lic_start_dt else lic_start_dt end as dim_lic_from
--     , case when lic_end_dt is null then ( select lic_end_t
--                                             from  ) else lic_end_dt end as dim_lic_to              
  from (
        select pl4.*
             , lag(lic_start_dt) over(partition by dim_provider_id, dim_document_type_id order by rn) as prev_lic_start_dt
             , lead(lic_end_dt) over(partition by dim_provider_id, dim_document_type_id order by rn) as next_lic_end_dt
            from (                
                select pl3.*
                     , case when prev_end_dt is null or prev_end_dt < eff_dt - 1 then eff_dt else null end as lic_start_dt
                     , case when end_dt > next_eff_dt then null -- overlapping license
                            when end_dt = to_date('12/31/9999', 'mm/dd/yyyy') then end_dt
                            when next_eff_dt is null or next_eff_dt > end_dt + 1 then end_dt else null end as lic_end_dt
                  from (                
                    select pl2.*
                         , lag(end_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as prev_end_dt
                         , lag(eff_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as prev_eff_dt
                         , lead(end_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as next_end_dt
                         , lead(eff_dt) over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt) as next_eff_dt
                         , row_number() over(partition by dim_provider_id, dim_document_type_id order by eff_dt, end_dt desc ) as rn
                            from(
                                select    dim_provider_id
                                        , dim_document_type_id
                                        , implied_start_date_ind
                                        , dd1.dim_date_id as dim_date_id_approval_eff
                                        , dd2.dim_date_id as dim_date_id_approval_end
                                        , dim_date_id_eff as eff_dt
                                        , dim_document_set_id_missing
                                        , dim_document_set_id_noncomp
                                        , dim_yn_id_is_licensed
                                        , case when dim_date_id_end = ( lead(dim_date_id_eff) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff, dim_date_id_end) )
                                               then lead(dim_date_id_eff) over (partition by dim_provider_id, dim_document_type_id order by dim_date_id_eff)  - 1/86400
                                               else dim_date_id_end + 1/86400
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
                                                 , ds2.dim_document_set_id as dim_document_set_id_missing
                                                 , ds1.dim_document_set_id as dim_document_set_id_noncomp
                                                 , case when ds1.num_docs = 0 and ds2.num_docs = 0 then 1
                                                        else 0
                                                   end as dim_yn_id_is_licensed
                                        from ( select distinct awtng_dcmntn         --HAD DUPLICATE ROWS WITH SAME EXACT DATA, EXCEPT ROW_CRTN_USR AND ROW_CRTN_DT
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
                                        left join dhs_core.dim_document_set ds1
                                        on nvl(ds1.doc_cd_list,'_') = nvl(pl.nn_cmplnc_rson,'_')
                                        and nvl(ds1.other_text, '_') = nvl(substr(pl.nn_cmplnc_oth, 1, 50), '_')
                                        inner join dhs_core.dim_document_set ds2 on nvl(ds2.doc_cd_list,'_') = nvl(pl.awtng_dcmntn,'_') 
                                                                ) pl1
                                    inner join dim_date dd1  --JOINS TO GET DIM_DATE_ID_APPROVAL_EFF AND END
                                    on dd1.dt = pl1.dim_date_id_eff
                                    inner join dim_date dd2
                                    on dd2.dt = pl1.dim_date_id_end ) pl2
                         )pl3
                  )pl4
        )pl5
        --            inner join dim_date dd11 --JOINS TO GET DIM_DATE_ID_LICENSED_FROM AND TO
        --            on dd11.dt = eff_dt
        --            inner join dim_date dd12
        --            on dd12.dt = end_dt 
        --        )src
)