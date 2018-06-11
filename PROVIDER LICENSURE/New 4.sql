select * from (
select THIS.*
     , row_number() over(partition by dim_provider_id, dim_document_type_id, END_DT order by END_DT ) as rn 
from(
    SELECT PL.*
            , LEAD(DIM_DATE_ID_END, 1) OVER (ORDER BY DIM_DATE_ID_EFF) - 1 AS END_DT
--         , CASE WHEN APPRVL_ACTUAL_END_DT IS NULL AND  
--                THEN (LEAD(
--                WHEN APPRVL_ACTUAL_END_DT IS NOT NULL AND 
--                THEN (LEAD(DIM_DATE_ID_EFF, 1) OVER (ORDER BY DIM_DATE_ID_EFF) - 1)
--                ELSE APPRVL_ACTUAL_END_DT
--           END AS END_DT
         --, DIM_DATE_ID_END AS END_DT
      FROM (
        select prvdr_no
             , dim_provider_id
             , dim_document_type_id
             , dcmnt_cd
             , case when pl.apprvl_actual_end_dt is not null 
                    then PL.APPRVL_ACTUAL_END_DT--to_number(to_char(pl.apprvl_actual_end_dt, 'yyyymmdd'))
                    when PL.APPRVL_ACTUAL_END_DT IS NULL AND pl.apprvl_end_dt is not null
                    then PL.APPRVL_END_DT--to_number(to_char(pl.apprvl_end_dt, 'yyyymmdd'))
                    else TO_DATE('12/31/9999', 'MM/DD/YYYY')
               end as dim_date_id_end
             , case when PL.APPRVL_EFFCTV_DT is null AND PL.APPRVL_ACTUAL_END_DT IS NULL
                    THEN NULL
                    WHEN PL.APPRVL_EFFCTV_DT IS NULL AND PL.APPRVL_ACTUAL_END_DT IS NOT NULL
                    THEN TO_DATE('1/1/1900', 'MM/DD/YYYY')
                    else PL.APPRVL_EFFCTV_DT
               end as dim_date_id_eff 
             , apprvl_actual_end_dt
             , apprvl_end_dt
             , apprvl_effctv_dt
        from (select DISTINCT PRVDR_NO, DCMNT_CD, APPRVL_EFFCTV_DT, APPRVL_END_DT, APPRVL_ACTUAL_END_DT 
                from staging.prvdr_lcnc) PL
        left join dim_document_type dt
        on pl.dcmnt_cd = dt.document_cd
        left join dim_provider dp
        on dp.prv_no = pl.prvdr_no ) PL ) THIS
)
where prvdr_no = '6608FH0246'
OR PRVDR_NO = '6610IC0816'
--where rn > 1 --272-16-2 = 254
ORDER BY PRVDR_NO, DIM_DOCUMENT_TYPE_ID, end_dt