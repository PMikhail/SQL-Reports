select * from STAGING.X_NRMB_CHLD
where case_no = 165054
and sfx = 'C'
and row_dlt_trnsct_id = 0
order by nr_eff_dt