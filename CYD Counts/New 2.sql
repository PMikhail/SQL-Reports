/*SELECT FCCS.DIM_CHILD_ID,
FCCS.EFF_DT,
FCCS.END_DT,
FCCS.DIM_DATE_ID_BGN_OF_DAY_EFF,
FCCS.DIM_DATE_ID_BGN_OF_DAY_END,
FCCS.DIM_DATE_ID_END_OF_DAY_EFF,
FCCS.DIM_DATE_ID_END_OF_DAY_END,
DSTAT.STATUS_LONG_DESC
FROM REPORTING.FACT_CASE_CHILD_STATUS FCCS
INNER JOIN REPORTING.DIM_STATUS DSTAT ON
FCCS.DIM_STATUS_ID =
DSTAT.DIM_STATUS_ID
WHERE DSTAT.STATUS_LONG_DESC = 'Open for non-CYD Service'
OR DSTAT.STATUS_LONG_DESC = 'Open for CYD Service';*/

Select FCCS.DIM_CHILD_ID,
  FCCS.EFF_DT,
  FCCS.END_DT,
  FCCS.DIM_DATE_ID_BGN_OF_DAY_EFF,
  FCCS.DIM_DATE_ID_BGN_OF_DAY_END,
  FCCS.DIM_DATE_ID_END_OF_DAY_EFF,
  FCCS.DIM_DATE_ID_END_OF_DAY_END,
  DSTAT.STATUS_LONG_DESC
From REPORTING.FACT_CASE_CHILD_STATUS FCCS
  Inner Join REPORTING.DIM_STATUS DSTAT On FCCS.DIM_STATUS_ID =
    DSTAT.DIM_STATUS_ID
Where (DSTAT.STATUS_LONG_DESC = 'Open for non-CYD Service') Or
  (DSTAT.STATUS_LONG_DESC = 'Open for CYD Service');
  
  SELECT FCCS.DIM_CHILD_ID,
FCS.DIM_SERVICE_STATUS_ID,
DSERV.DEP_DLQ,
DSERV.PLACEMENT_CATEGORY
FROM REPORTING.FACT_CASE_CHILD_STATUS FCCS
INNER JOIN REPORTING.FACT_CHILD_SERVICE FCS ON
FCCS.DIM_CHILD_ID = FCS.DIM_CHILD_ID
INNER JOIN REPORTING.DIM_SERVICE DSERV ON
FCS.DIM_SERVICE_ID = DSERV.DIM_SERVICE_ID
INNER JOIN REPORTING.DIM_STATUS DSTAT ON
FCCS.DIM_STATUS_ID = DSTAT.DIM_STATUS_ID
WHERE DSTAT.STATUS_CD = 'CO'
OR DSTAT.STATUS_CD = 'CS';

SELECT COUNT(FCCS.DIM_CHILD_ID)
FROM REPORTING.FACT_CASE_CHILD_STATUS FCCS
/*INNER JOIN REPORTING.FACT_CHILD_SERVICE FCS ON
FCCS.DIM_CHILD_ID = FCS.DIM_CHILD_ID
INNER JOIN REPORTING.DIM_SERVICE DSERV ON
FCS.DIM_SERVICE_ID = DSERV.DIM_SERVICE_ID*/
INNER JOIN REPORTING.DIM_STATUS DSTAT ON
FCCS.DIM_STATUS_ID = DSTAT.DIM_STATUS_ID
WHERE DSTAT.STATUS_CD = 'CS';

select FCCS.DIM_CHILD_ID,
 
from reporting.fact_case_child_status fccs

SELECT FCCS.DIM_CHILD_ID,
FCS.DIM_SERVICE_STATUS_ID,
DSERV.DEP_DLQ,
DSERV.PLACEMENT_CATEGORY
FROM REPORTING.FACT_CASE_CHILD_STATUS FCCS
INNER JOIN REPORTING.FACT_CHILD_SERVICE FCS ON
FCCS.DIM_CHILD_ID = FCS.DIM_CHILD_ID
INNER JOIN REPORTING.DIM_SERVICE DSERV ON
FCS.DIM_SERVICE_ID = DSERV.DIM_SERVICE_ID
INNER JOIN REPORTING.DIM_STATUS DSTAT ON
FCCS.DIM_STATUS_ID = DSTAT.DIM_STATUS_ID
WHERE DSTAT.STATUS_CD = 'CO'
OR DSTAT.STATUS_CD = 'CS'
