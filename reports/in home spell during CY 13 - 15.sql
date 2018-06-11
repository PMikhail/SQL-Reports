select ch.case_no||'-'||ch.suffix CaseChild
    ,to_char(fcs.END_DT,'YYYY') Spell_Year
    ,s.serv_cd as serv_cd_spell_end
    ,case when pass_reason_code in ('02','26','32') then 'PLC'
         when pass_reason_code in ('01') then 'REUNIFICATION'
         when pass_reason_code in ('09') then 'ADOPTION' else 'NON-PERM' end Perm_Group
    ,ds.pass_reason_code
    ,ds.PASS_REASON_LONG_NAME
    ,fcs.eff_dt as spell_start_dt
    ,fcs.end_dt as spell_end_dt
    ,fcs.spell_seq_num
    ,s_n.serv_cd as next_serv_cd
    ,fcs.service_eff_dt_next
    ,case when dp.dim_position_id = 0 then -1 else dp.cua_cd_id end as cua_cd_id
    ,w.full_name assigned_worker
    ,W.PSTN_TYP_LONG_DESC assigned_worker_type
    ,sv.full_name Sup_assigned_worker
    ,sv.PSTN_TYP_LONG_DESC Sup_assigned_worker_type
    ,ad.full_name ad_assigned_worker
    ,ad.PSTN_TYP_LONG_DESC ad_assigned_worker_type
    ,dr.full_name dr_assigned_worker
    ,dr.PSTN_TYP_LONG_DESC dr_assigned_worker_type
    ,SVB.BSNS_UNIT_CD||'-'||SVB.BSNS_UNIT_LVL_SHRT_DESC UNIT
    ,ADB.BSNS_UNIT_CD||'-'||ADB.BSNS_UNIT_LVL_SHRT_DESC SECTION
    ,DRB.BSNS_UNIT_CD||'-'||DRB.BSNS_UNIT_LVL_SHRT_DESC CENTER
    ,ch.brth_dt as dob
    ,ch.gender_cd 
    ,case when ch.primary_race_shrt_desc = 'Multiple' then ch.secondary_race_glob else ch.primary_race_shrt_desc end  as race
    ,ch.hspnc_ind as hispanic_ind
from (select sp.dim_child_id
             ,sp.eff_dt
             ,sp.end_dt
             ,sp.dim_service_id
             ,sp.dim_service_status_id
             ,sp.spell_seq_num
             ,srv.dim_service_id as dim_service_id_next
             ,srv.eff_dt as service_eff_dt_next
             ,row_number() over(partition by sp.dim_child_id, sp.spell_seq_num order by nvl(srv.eff_dt,sp.eff_dt)) as rn
        from reporting.FACT_CHILD_SPELL sp
            left join
            reporting.FACT_CHILD_SERVICE srv
                on srv.dim_child_id = sp.dim_child_id
               and srv.eff_dt > sp.end_dt
        where sp.end_dt >=  to_date('01/01/2013 00:00:00','mm/dd/yyyy HH24:MI:SS')
          and (sp.end_dt  <=  to_date('12/31/2015 23:59:59','mm/dd/yyyy HH24:MI:SS')
            OR sp.eff_dt between to_date('01/01/2013 00:00:00','mm/dd/yyyy HH24:MI:SS') and to_date('12/31/2015 23:59:59','mm/dd/yyyy HH24:MI:SS'))
        and sp.DIM_SPELL_TYPE_ID = (select dim_spell_type_id
                                     from reporting.DIM_SPELL_TYPE
                                    where spell_type = 'In-home Service - 30 day gap')
        --and sp.dim_child_id = 718680
    ) fcs
    inner join 
    reporting.DIM_CASE_CHILD ch 
        on fcs.dim_child_id = ch.dim_child_id
    inner join 
    reporting.DIM_SERVICE s 
        on fcs.dim_service_id = s.dim_service_id
    left join 
    reporting.DIM_SERVICE s_n
        on fcs.dim_service_id_next = s_n.dim_service_id
    inner join 
    REPORTING.DIM_SERVICE_STATUS ds 
        on fcs.dim_service_status_id = ds.dim_service_status_id
    inner join 
    (select x.*
             , lag(worker_dim_position_id) over (partition by dim_child_id order by eff_dt) as dim_position_id_prev
           from reporting.fact_case_child_status_hier x
     ) h 
        on h.dim_child_id = fcs.dim_child_id
        and fcs.end_dt  between h.eff_dt and h.end_dt
    inner join
    reporting.dim_position dp
        on dp.dim_position_id = case when h.worker_dim_position_id = 0 then  h.dim_position_id_prev else  h.worker_dim_position_id end       
    left join 
    reporting.dim_worker w 
        on w.dim_worker_id = H.WORKER_DIM_WORKER_ID
    left join
    reporting.dim_worker sv 
        on sv.dim_worker_id = H.SUPERVISOR_DIM_WORKER_ID
    left join 
    reporting.dim_worker ad 
        on ad.dim_worker_id = H.ADMIN_DIM_WORKER_ID
    left join 
    reporting.dim_worker dr 
        on dr.dim_worker_id = H.DIRECTOR_DIM_WORKER_ID
    left join 
    reporting.DIM_BUSINESS_UNIT svb 
        on H.SUPERVISOR_DIM_BUSINESS_UNT_ID = SVB.DIM_BUSINESS_UNIT_ID
    left join 
    reporting.DIM_BUSINESS_UNIT adb 
        on H.ADMIN_DIM_BUSINESS_UNIT_ID = adb.DIM_BUSINESS_UNIT_ID
    left join 
    reporting.DIM_BUSINESS_UNIT drb 
        on H.DIRECTOR_DIM_BUSINESS_UNIT_ID = drb.DIM_BUSINESS_UNIT_ID         
where fcs.rn = 1
 and S.DEP_DLQ = 'DEP'
order by to_char(spell_END_DT,'YYYY'),
    case when pass_reason_code in ('02','26','32') then 'PLC'
         when pass_reason_code in ('01') then 'REUNIFICATION'
         when pass_reason_code in ('09') then 'ADOPTION' else 'NON-PERM' end,
    ds.pass_reason_code,ch.case_no||'-'||ch.suffix
