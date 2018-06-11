SELECT * FROM all_col_comments where table_name = 'DIM_DOCUMENT_SET'

select prvdr_no, count(*) from staging.prvdr_lcnc group by prvdr_no

select * from staging.prvdr_lcnc

select * from staging.prvdr_lcnc where prvdr_no = '6606IC0196'

select * from staging.prvdr_lcnc pl
where PL.NN_CMPLNC_RSON like '%OTH%'

select * from staging.lookups order by cd



select distinct nn_cmplnc_rson from staging.prvdr_lcnc


select    
   case when nn_cmplnc_rson || '|' like '%-MMD|%' then 'Y'
         when nn_cmplnc_rson || '|' like '%-MEDDOC|%' then 'Y'
         when awtng_dcmntn || '|' like '%-MMD|%' then 'Y'
         when awtng_dcmntn || '|' like '%-MEDDOC|%' then 'Y'
    else 'N' end as MEDICAL_FLAG
  , case when nn_cmplnc_rson || '|' like '%-CAC|%' then 'Y'
         when nn_cmplnc_rson || '|' like '%-MCAC|%' then 'Y'
         when nn_cmplnc_rson || '|' like '%-CHLDABUS|%' then 'Y'
         when awtng_dcmntn || '|' like '%-CAC|%' then 'Y'
         when awtng_dcmntn || '|' like '%-MCAC|%' then 'Y'
         when awtng_dcmntn || '|' like '%-CHLDABUS|%' then 'Y'
    else 'N' end as CHILD_ABUSE_FLAG
  , case when nn_cmplnc_rson || '|' like '%-CC|%' then 'Y'
         when nn_cmplnc_rson || '|' like '%-MCC|%' then 'Y'
         when nn_cmplnc_rson || '|' like '%-CRIMCLR|%' then 'Y'
         when awtng_dcmntn || '|' like '%-CC|%' then 'Y'
         when awtng_dcmntn || '|' like '%-MCC|%' then 'Y'
         when awtng_dcmntn || '|' like '%-CRIMCLR|%' then 'Y'
    else 'N' end as CRIMINAL_CLEARANCE_FLAG
  , case when nn_cmplnc_rson || '|' like '%-LTH|%' then 'Y'
         when awtng_dcmntn || '|' like '%-LTH|%' then 'Y'
    else 'N' end as TRAINING_HOURS_FLAG
  , case when nn_cmplnc_rson || '|' like '%-FCM|%' then 'Y'
         when nn_cmplnc_rson || '|' like '%-MFCM|%' then 'Y'
         when awtng_dcmntn || '|' like '%-FCM|%' then 'Y'
         when awtng_dcmntn || '|' like '%-MFCM|%' then 'Y'
    else 'N' end as FBI_CLEARANCE_FLAG
  , case when nn_cmplnc_rson || '|' like '%-OTH|%' then 'Y'
    else 'N' end as OTHER_FLAG
  , NN_CMPLNC_OTH AS OTHER_TEXT    
  , case when AWTNG_DCMNTN is null then nn_cmplnc_rson else awtng_dcmntn
    end as doc_cd_list
from ( select distinct PL.NN_CMPLNC_RSON, PL.NN_CMPLNC_OTH  
          from staging.prvdr_lcnc pl ) ONE
FULL OUTER JOIN ( SELECT DISTINCT AWTNG_DCMNTN FROM STAGING.PRVDR_LCNC ) TWO
ON ONE.NN_CMPLNC_RSON = TWO.AWTNG_DCMNTN

          

select distinct awtng_dcmntn, nn_cmplnc_rson, nn_cmplnc_oth
from staging.prvdr_lcnc

select * from staging.prvdr_lcnc


select * from staging.lookups order by cd

select * from STAGING.LOOKP_TYPS

select * from dim_document_type


select * from staging.prvdr_lcnc









select pl.*,
       case when nn_cmplnc_rson is not null then 
            case when awtng_dcmntn is not null
                 then nn_cmplnc_rson || '|' || awtng_dcmntn
                 else nn_cmplnc_rson
            end
            else awtng_dcmntn
       end as doc_cd_list 
from staging.prvdr_lcnc pl )

select count(distinct nn_cmplnc_rson) from staging.prvdr_lcnc

select count(distinct awtng_dcmntn) from staging.prvdr_lcnc  