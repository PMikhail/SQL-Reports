select count(*) from
(
select    cc.CUA                    "CUA Code"
        , C.CASE_NMBR               "Case"
        , CP.PRSN_INVLVD_SFX_TXT    "SFX"
        , PAR.LAST_NM               "Last Name"
        , PAR.FIRST_NM              "First Name"
        , case when lp.serv_cd is null then trunc(cc.cua_case_xfer_dt)
               when substr(lp.prv_no,5,2) in ('HS', 'RU') then null
               else lp.pass_dt
          end                       "Last Discharge Dt"
        , case when lp.serv_cd is null then null
               when substr(lp.prv_no,5,2) in ('HS', 'RU') then null
               else lp.serv_cd
          end                       "Last Serv CD"
        , case when lp.serv_cd = 'P1GN' and lp.pass_dt is null then 'In CUA care'
               when lp.serv_cd is null              then 'No Serv'
               when substr(lp.prv_no,5,2) = 'HS'    then 'Hosp'
               when substr(lp.prv_no,5,2) = 'RU'    then 'Runaway'
               else lp.prv_no
          end                       "Last Prv No"
        , case when lp.serv_cd is null then null
               when lp.serv_cd = 'P1GN' then null
               when substr(lp.prv_no,5,2) in ('HS', 'RU') then null
               else lp.pass_reas || ' - ' || (select initcap(TBL_LONG_NAME)
                                                from STAGING.X_TABLES
                                               where TBL_NO = '058'
                                                 and TBL_CD = lp.PASS_REAS)
          end                       "Pass From Care Code/Reason"
        , case when lp.serv_cd = 'P1GN' and lp.pass_dt is null then 'Need Service'
               when lp.serv_cd = 'P1GN' then 'Potential To Close'
               when lp.pass_reas is null then 'Potential to Close'
               when substr(lp.prv_no,5,2) in ('HS', 'RU') then 'Potential To Close'
               when lp.pass_reas in ('28','33','35','13','25','03','01','06',
                    '02','12','09','08','22','14','32','23','26','04')
               then 'Potential To Close'
               else 'Need Service'
          end                       "Recommendation"
        , case when lp.serv_cd is null then trunc(SYSDATE) - trunc(cc.cua_case_xfer_dt)
               when lp.serv_cd = 'P1GN' and lp.pass_dt is null then trunc(SYSDATE) - lp.plcm_dt
               when substr(lp.prv_no,5,2) in ('HS','RU') then trunc(SYSDATE) - lp.plcm_dt
               else trunc(SYSDATE) - lp.pass_dt
          end                       "Days since Discharge"
        , PAR.BRTH_DT DOB
from 
( 
    select   WPA.WRK_PRDCT_ID  case_id
            , CUA.CUA_BSNS_CD   cua
            , WPA.EFF_DT        cua_case_xfer_dt
            , WPA.CD_WRK_PRDCT_TYP_ID wptype_id
        from STAGING.WRK_PRDCT_ASGNMT wpa
          join STAGING.WRK_PRDCT_STTS wps
            on WPS.WRK_PRDCT_ID = WPA.WRK_PRDCT_ID
           and WPS.WRK_PRDCT_PARTY_ID is null
           and SYSDATE between WPS.EFF_DT and WPS.END_DT
          join STAGING.CD_WRK_PRDCT_TYP_STTS stat
            on WPA.CD_WRK_PRDCT_TYP_ID = STAT.CD_WRK_PRDCT_TYP_ID
           and STAT.CD = 'CO'
          join STAGING.PSTN pos
            on POS.INITL_PSTN_ID = WPA.PSTN_ID
           and SYSDATE between POS.EFF_DT and POS.END_DT
           and POS.CD_CUA_ID > 0
          join STAGING.CD_CUA cua
            on CUA.ID = POS.CD_CUA_ID
          join STAGING.CD_WRK_PRDCT_TYP wpt
            on WPT.ID = WPA.CD_WRK_PRDCT_TYP_ID
           and WPT.CD = 'CASE'
    where WPA.WRK_PRDCT_PARTY_ID is null
      and SYSDATE between wpa.EFF_DT and wpa.END_DT
    order by 1   
   ) cc
    join STAGING.CASE c
      on cc.CASE_ID = c.ID
    join STAGING.CASE_PARTY cp
      on CP.CASE_ID = C.ID
    join STAGING.PARTY par
      on par.ID = CP.PRSN_INVLVD_PARTY_ID
    left join 
    (
    select *
    from (
            select P.CASE_NO
                 , P.SFX
                 , (case when P.SERV_CD = 'P1GN' then null else P.PRV_NO end) prv_no
                 , P.SERV_CD
                 , P.PLCM_DT
                 , (case when P.SERV_CD = 'P1GN' and P.PASS_DT > SYSDATE then null
                    else P.PASS_DT end) pass_dt
                 , (case when P.SERV_CD = 'P1GN' then null else P.PASS_REAS end) pass_reas
                 , row_number() over (partition by P.CASE_NO, P.SFX order by P.PASS_DT desc)
                    as row_nmbr 
            from STAGING.X_PLCM p
           where (      P.SERV_CD = 'P1GN'
                  and   P.ROW_DLT_TRNSCT_ID <> 0
                  and   0 = ( select count(*)
                                from STAGING.X_PLCM lp
                               where nvl(LP.PAYM_STATUS, ' ') <> 'V'
                                 and LP.TYPE in ('R', 'H')
                            )
                 )
              or ( nvl(P.PAYM_STATUS, ' ') <> 'V'
                    and P.TYPE IN ('R', 'H')
                    and P.ROW_DLT_TRNSCT_ID = 0
                 )
         )
    where row_nmbr = 1
    ) lp
      on lp.CASE_NO = C.CASE_NMBR
     and lp.SFX     = CP.PRSN_INVLVD_SFX_TXT
     join staging.cd_wrk_prdct_typ_stts stat
     on cc.wptype_id = stat.cd_wrk_prdct_typ_id
where 1 = 1
  and CP.CD_CASE_INVLVMT_ID = (select ID from STAGING.CD_CASE_INVLVMT where CD = 'C')
  and stat.cd = 'CO'
  and (select stat.cd 
       from STAGING.WRK_PRDCT_STTS wps
     inner join STAGING.CD_WRK_PRDCT_TYP_STTS stat
         on WPS.CD_WRK_PRDCT_TYP_ID = STAT.CD_WRK_PRDCT_TYP_ID
      where WPS.WRK_PRDCT_ID = C.ID
        and WPS.WRK_PRDCT_PARTY_ID = CP.ID
        and SYSDATE between wps.EFF_DT and wps.END_DT
      ) = 'CO'
  and nvl(lp.pass_dt,SYSDATE) <= SYSDATE
order by 1,2,3
);



-------------------------------------------------------------------------------

    select stat.cd 
      from STAGING.WRK_PRDCT_STTS wps
right join STAGING.CD_WRK_PRDCT_TYP_STTS stat
        on WPS.CD_WRK_PRDCT_TYP_ID = STAT.CD_WRK_PRDCT_TYP_ID
      join staging.case_party cp
      on cp.        
     where WPS.WRK_PRDCT_ID = staging.case.ID
       and WPS.WRK_PRDCT_PARTY_ID = STAGING.CASE_PARTY.ID
       and SYSDATE between wps.EFF_DT and wps.END_DT
       
       select count(*)
       from (
       select stat.cd from staging.case c
       right join staging.case_party cp
       on CP.CASE_ID = c.id
       inner join staging.wrk_prdct_stts wps
       on wps.wrk_prdct_id = c.id
       right join staging.cd_wrk_prdct_typ_stts stat
       on STAT.CD_WRK_PRDCT_TYP_ID = WPS.CD_WRK_PRDCT_TYP_ID
       and stat.cd = 'CO'
       and wps.wrk_prdct_party_id = cp.id
       and sysdate between wps.eff_dt and wps.end_dt
       )
       
       select * from staging.cd_wrk_prdct_typ_stts stat
       
       where sysdate between 
       
       select * from staging.case_party cp
       
       
       (select
       
       select * from staging.wrk_prdct_stts
       right joing 


select * from staging.cd_wrk_prdct_typ_stts

select * from staging.wrk_prdct_stts


select * from staging.cd_wrk_prdct_typ_stts

select * from staging.wrk_prdct_stts

select * from staging.wrk_prdct_stts

select * from staging.wrk_prdct_asgnmt

select * from STAGING.CD_WRK_PRDCT_TYP_STTS --(cd_wrk_prdct_type_id)

select * from STAGING.CD_WRK_PRDCT_TYP
