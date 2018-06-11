Select fccs.*
from reporting.fact_case_child_status fccs
where /*(((Trunc(FCCS.EFF_DT) <= To_Date('7/1/2000', 'mm/dd/yyyy'))
    And (Trunc(FCCS.EFF_DT) >= To_Date('7/1/2000', 'mm/dd/yyyy'))) Or
    ((Trunc(FCCS.EFF_DT) >= To_Date('7/1/2000', 'mm/dd/yyyy')) And
    (Trunc(FCCS.EFF_DT) <= To_Date('7/1/2015', 'mm/dd/yyyy'))))
    and*/ (dim_status_id = 28
or dim_status_id = 60
or dim_status_id = 100
or dim_status_id = 101
or dim_status_id = 144
or dim_status_id = 145
or dim_status_id = 27
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
or dim_status_id = 189);

select count(fcs.dim_child_id)
from reporting.fact_case_child_status fcs
inner join reporting.dim_status dstat on
fcs.dim_status_id = dstat.dim_status_id
where status_cd = 'CO';


/* 
    Datefilter FCCS and inner join with Datefilter-ed FCS
    Then inner join dim_service for service information
*/
select count(fcs.DIM_CHILD_ID)--, FCS.DIM_SERVICE_ID, dserv.dep_dlq, dserv.placement_category
from reporting.fact_child_service fcs
inner join
(Select fccs.dim_child_id
from reporting.fact_case_child_status fccs
inner join reporting.dim_status dstat on
dstat.dim_status_id = FCCS.DIM_STATUS_ID
where (((Trunc(FCCS.EFF_DT) <= To_Date('7/17/2014', 'mm/dd/yyyy'))
    And (Trunc(FCCS.END_DT) >= To_Date('7/17/2014', 'mm/dd/yyyy'))) Or
    ((Trunc(FCCS.EFF_DT) >= To_Date('7/17/2014', 'mm/dd/yyyy')) And
    (Trunc(FCCS.EFF_DT) <= To_Date('7/17/2015', 'mm/dd/yyyy'))))
    and (dstat.status_cd = 'CO')) CO on
fcs.dim_child_id = CO.dim_child_id
inner join reporting.dim_service dserv on
dserv.dim_service_id = FCS.DIM_SERVICE_ID
where (((Trunc(FCS.EFF_DT) <= To_Date('7/17/2014', 'mm/dd/yyyy'))
    And (Trunc(FCS.END_DT) >= To_Date('7/17/2014', 'mm/dd/yyyy'))) Or
    ((Trunc(FCS.EFF_DT) >= To_Date('7/17/2014', 'mm/dd/yyyy')) And
    (Trunc(FCS.EFF_DT) <= To_Date('7/17/2015', 'mm/dd/yyyy'))))
GROUP BY DSERV.DEP_DLQ
    
    
select * from reporting.dim_status where status_cd = 'CS';

select count(fccs.dim_child_id)
from reporting.fact_case_child_status fccs
inner join reporting.dim_status dstat on
fccs.dim_status_id = dstat.dim_status_id
where status_cd = 'CO' or status_cd = 'CS'
and (((Trunc(FCCS.EFF_DT) <= To_Date('7/17/2015', 'mm/dd/yyyy'))
    And (Trunc(FCCS.END_DT) >= To_Date('7/17/2015', 'mm/dd/yyyy'))) Or
    ((Trunc(FCCS.EFF_DT) >= To_Date('7/17/2015', 'mm/dd/yyyy')) And
    (Trunc(FCCS.EFF_DT) <= To_Date('7/17/2015', 'mm/dd/yyyy'))))