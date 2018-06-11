with
current_cua_cases as
  (select wpa.WRK_PRDCT_ID case_id
        , cua.CUA_BSNS_CD  cua
        , wpa.EFF_DT       cua_case_xfer_dt
     from VC0_WRK_PRDCT_ASGNMT wpa
          join VC0_WRK_PRDCT_STTS wps
            on wps.WRK_PRDCT_ID = wpa.WRK_PRDCT_ID
           and wps.WRK_PRDCT_PARTY_ID is null
           and sysdate between wps.EFF_DT and wps.END_DT
           and wps.WRK_PRDCT_TYP_STTS_CD = 'CO'
          join VL_PSTN pos
            on pos.INITL_PSTN_ID = wpa.PSTN_ID
           and sysdate between pos.EFF_DT and pos.END_DT
           and pos.CD_CUA_ID > 0
          join VL_CD_CUA cua
            on cua.ID = pos.CD_CUA_ID
    where wpa.WRK_PRDCT_TYP_CD = 'CASE'
      and wpa.WRK_PRDCT_PARTY_ID is null
      and sysdate between wpa.EFF_DT and wpa.END_DT
    order by 1
   ),
last_placement as
   (select  *
      from (select p.CASE_NO
                 , p.SFX
                 , (case when p.SERV_CD = 'P1GN' then null else p.PRV_NO end) prv_no
                 , p.SERV_CD
                 , p.PLCM_DT
                 , (case when p.SERV_CD = 'P1GN' and p.pass_dt > sysdate then null else p.PASS_DT end) pass_dt
                 , (case when p.SERV_CD = 'P1GN' then null else p.PASS_REAS end) pass_reas
                 , row_number() over (partition by p.CASE_NO, p.SFX order by p.PASS_DT desc) as row_nmbr
              from X_PLCM p
             where (    p.SERV_CD = 'P1GN'
                    and p.ROW_DLT_TRNSCT_ID <> 0
                    and 0 = (select count(*)
                               from VL_X_PLCM lp
                              where nvl(lp.PAYM_STATUS,' ') <> 'V'
                                and lp.TYPE in ('R','H')
                            )
                   )
                or (    nvl(p.PAYM_STATUS,' ') <> 'V'
                    and p.TYPE in ('R','H')
                    and p.ROW_DLT_TRNSCT_ID = 0
                   )
           )
     where row_nmbr = 1
   )
select (cc.cua || ' - ' ||
          (select initcap(pr.NAME)
             from VL_X_PROV_CUA_INFO_P6 pc
                  join VL_X_PROV pr
                    on pr.ID = pc.PARNT_ID
            where pc.CUA_CD = cc.cua
          )
       )                      "CUA Code and Name"
     , c.CASE_NMBR            "Case"
     , cp.PRSN_INVLVD_SFX_TXT "Sfx"
     , par.LAST_NM            "Last Name"
     , par.FIRST_NM           "First Name"
     , case when lp.serv_cd is null then trunc(cc.cua_case_xfer_dt)
            when substr(lp.prv_no,5,2) in ('HS','RU') then trunc(lp.plcm_dt)
         else lp.pass_dt
       end                    "Last Discharge Dt"
     , case when lp.serv_cd is null then null
            when substr(lp.prv_no,5,2) in ('HS','RU') then null
            else lp.serv_cd
       end                    "Last Serv Cd"
     , case when lp.serv_cd = 'P1GN' and lp.pass_dt is null then 'InCUA care'
            when lp.serv_cd is null           then 'No Serv'
            when substr(lp.prv_no,5,2) = 'HS' then 'Hosp'
            when substr(lp.prv_no,5,2) = 'RU' then 'Runaway'
            else lp.prv_no
       end                    "Last Prv No"
     , case when lp.serv_cd is null then null
            when lp.serv_cd = 'P1GN' then null
            when substr(lp.prv_no,5,2) in ('HS','RU') then null
            else lp.pass_reas || ' - ' || (select initcap(TBL_LONG_NAME)
                                             from VL_X_TABLES
                                            where TBL_NO = '058'
                                              and TBL_CD = lp.PASS_REAS)
       end                    "Pass From Care Code/Reason"
     , case when lp.serv_cd = 'P1GN' and lp.pass_dt is null then 'Need Service'
            when lp.serv_cd = 'P1GN' then 'Potential To Close'
            when lp.pass_reas is null then 'Potential To Close'
            when substr(lp.prv_no,5,2) in ('HS','RU') then 'Potential To Close'
            when lp.pass_reas in ('28','33','35','13','25','03','01','06','02','12','09','08','22','14','32','23','26','04')
            then 'Potential To Close'
            else 'Need Service'
       end                    "Recommendation"
     , case when lp.serv_cd is null then trunc(sysdate) - trunc(cc.cua_case_xfer_dt)
            when lp.serv_cd = 'P1GN' and lp.pass_dt is null then trunc(sysdate) - lp.plcm_dt
            when substr(lp.prv_no,5,2) in ('HS','RU') then trunc(sysdate) - lp.plcm_dt
            else trunc(sysdate) - lp.pass_dt
       end                    "Days since Discharge"
  , par.brth_dt DOB
  from current_cua_cases cc
       join VL_CASE c
         on cc.case_id = c.ID
       join VL_CASE_PARTY cp
         on cp.CASE_ID = c.ID
       join VL_PARTY par
         on par.ID = cp.PRSN_INVLVD_PARTY_ID
       left join last_placement lp
         on lp.case_no = c.CASE_NMBR
        and lp.sfx     = cp.PRSN_INVLVD_SFX_TXT
 where 1 = 1
   and cp.CD_CASE_INVLVMT_ID = (select ID from VL_CD_CASE_INVLVMT where CD = 'C')
   and (select wps.WRK_PRDCT_TYP_STTS_CD
          from VC0_WRK_PRDCT_STTS wps
         where wps.WRK_PRDCT_ID = c.ID
           and wps.WRK_PRDCT_PARTY_ID = cp.ID
           and sysdate between wps.EFF_DT and wps.END_DT
        ) = 'CO'
   and nvl(lp.pass_dt,sysdate) <= sysdate
 order by 1, 2, 3
;