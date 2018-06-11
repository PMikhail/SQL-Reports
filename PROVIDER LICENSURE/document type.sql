create table temp_doc_type
as    (  
select
         distinct dcmnt_cd as document_cd--DISTINCT SUBSTR(REGEXP_SUBSTR(PL.DCMNT_CD, '-[^$]+'),2) DOCUMENT_CD       
       , lkup.descr as document_name
    from staging.prvdr_lcnc pl
inner join staging.lookups lkup
      on pl.dcmnt_cd = lkup.id
inner join STAGING.LOOKP_TYPS lt
      on lt.typ = LKUP.TYP 
      )
      



SELECT * FROM TEMP_DOC_TYPE

FROM STAGING.PRVDR_LCNC
INNER JOIN 
      
drop table temp_doc_type
      
delete from dim_document_type
where dim_document_type_id is not null
      
insert into dim_document_type 
(   dim_document_type_id
  , document_cd
  , document_name
  , last_modified_dt )
select DHS_CORE.dim_document_type_id_seq.nextval
     , document_cd
     , document_name
     , current_timestamp
     from temp_doc_type
          
          
select dhs_core.dim_document_type_id
     
select * from staging.lookp_typs
          
select * from dim_document_type

commit