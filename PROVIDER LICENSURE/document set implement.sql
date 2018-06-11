CREATE TABLE TEMP_DOC_SET AS
( SELECT NVL((LENGTH(DOC_CD_LIST) - LENGTH(REPLACE(DOC_CD_LIST, '-'))), 0) AS NUM_DOCS, THIS.* FROM
     ( select    
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
        ON nvl(ONE.NN_CMPLNC_RSON,'_') = nvl(TWO.AWTNG_DCMNTN, '_')
        ) THIS
)

DROP TABLE TEMP_DOC_SET

SELECT * FROM TEMP_DOC_SET ORDER BY LENGTH(OTHER_TEXT) DESC

ALTER TABLE DIM_DOCUMENT_SET
MODIFY OTHER_TEXT VARCHAR(50)


INSERT INTO DHS_CORE.DIM_DOCUMENT_SET
(   DIM_DOCUMENT_SET_ID
  , NUM_DOCS
  , MEDICAL_FLAG
  , CHILD_ABUSE_FLAG
  , CRIMINAL_CLEARANCE_FLAG
  , TRAINING_HOURS_FLAG
  , FBI_CLEARANCE_FLAG
  , OTHER_FLAG
  , OTHER_TEXT
  , LAST_MODIFIED_DT
  , DOC_CD_LIST )
SELECT  DHS_CORE.DIM_DOCUMENT_SET_ID_SEQ.NEXTVAL
      , NUM_DOCS
      , MEDICAL_FLAG
      , CHILD_ABUSE_FLAG
      , CRIMINAL_CLEARANCE_FLAG
      , TRAINING_HOURS_FLAG
      , FBI_CLEARANCE_FLAG
      , OTHER_FLAG
      , SUBSTR(OTHER_TEXT,1, 50)
      , SYSDATE
      , DOC_CD_LIST
      FROM TEMP_DOC_SET

COMMIT

SELECT * FROM ALL_COL_COMMENTS WHERE TABLE_NAME = 'DIM_DOCUMENT_SET'

SELECT * FROM DIM_DOCUMENT_SET

DELETE FROM DIM_DOCUMENT_SET WHERE DIM_DOCUMENT_SET_ID IS NOT NULL

SELECT DHS_CORE.DIM_DOCUMENT_SET_ID_SEQ.CURRVAL FROM DUAL

ALTER SEQUENCE DHS_CORE.DIM_DOCUMENT_SET_ID_SEQ INCREMENT BY 1
