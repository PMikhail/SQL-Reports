-- Following is status_id of just CO
-- 28, 60, 100, 101, 144, 145
select *
from reporting.dim_status
where status_cd = 'CO';

-- Following is status_id of just CS
-- 27,59,104,105,108,109,112,113,116,117,120,121,126,127,132,133,136,137,140
--141,148,149,152,153,156,157,162,163,166,167,170,171,176,177,182,183,188,189
select *
from reporting.dim_status
where status_cd = 'CS';

--Following is children with 'CO' status
--count = 837,709
select count(*)
from reporting.fact_case_child_status
where dim_status_id = 28
or dim_status_id = 60
or dim_status_id = 100
or dim_status_id = 101
or dim_status_id = 144
or dim_status_id = 145;



--Following is children with 'CS' status
--count = 
select count(dim_child_id)
from reporting.fact_case_child_status
where dim_status_id = 27
or dim_status_id = 59
or dim_status_id = 104
or dim_status_id = 105
or dim_status_id = 108
or dim_status_id = 109
or dim_status_id = 112
or dim_status_id = 113
or dim_status_id = 116
or dim_status_id = 117
or dim_status_id = 120
or dim_status_id = 121
or dim_status_id = 126
or dim_status_id = 127
or dim_status_id = 132
or dim_status_id = 133
or dim_status_id = 136
or dim_status_id = 137
or dim_status_id = 140
or dim_status_id = 141
or dim_status_id = 148
or dim_status_id = 149
or dim_status_id = 152
or dim_status_id = 153
or dim_status_id = 156
or dim_status_id = 157
or dim_status_id = 162
or dim_status_id = 163
or dim_status_id = 166
or dim_status_id = 167
or dim_status_id = 170
or dim_status_id = 171
or dim_status_id = 176
or dim_status_id = 177
or dim_status_id = 182
or dim_status_id = 183
or dim_status_id = 188
or dim_status_id = 189;

--Test date condition for fccs
--doesn't work when joining fcs
--count = 466,660
select *
from reporting.fact_case_child_status fccs
where fccs.dim_status_id = 60
or dim_status_id = 100
or dim_status_id = 101
or dim_status_id = 144
or dim_status_id = 145
and trunc(FCCS.EFF_DT) >= to_date('7/1/2014', 'mm/dd/yyyy');

select count(fcs.dim_child_id)
from reporting.fact_child_service fcs on
fccs.dim_child_id = fcs.dim_child_id
where fccs.dim_status_id = 60
or dim_status_id = 100
or dim_status_id = 101
or dim_status_id = 144
or dim_status_id = 145
and trunc(FCCS.EFF_DT) >= to_date('7/1/2014', 'mm/dd/yyyy')
and trunc(fcs.eff_dt) >= to_date('7/1/2014', 'mm/dd/yyyy');


select *
from reporting.fact_child_service fcs
inner join reporting.dim_service dserv on
fcs.dim_service_id = dserv.dim_service_id;


select * from reporting.fact_child_service;

select * from reporting.dim_service; 