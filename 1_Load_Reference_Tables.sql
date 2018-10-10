-- *******************************************************************************************************
-- HOUSEKEEPING START
--   1. Create table to capture matadata during the process
--   2. Create driver table for primary care departments
--   3. Create driver table for HIC care providers
--   4. Create driver tables for diagnoses codes
--   5. Create driver tables for HIV Labs

-- *******************************************************************************************************

/********************************************************************************************************

    Step 1
        Create reference tables and metadata 

********************************************************************************************************/

-- *******************************************************************************************************
-- STEP 1.1
--		Create table to capture all data counts
---------------------------------------------------------------------------------------------------------
--		This table will permit have a point of reference of the different datasets and could
--		help troubleshoot potential issues at a basic level	
-- *******************************************************************************************************
DROP TABLE XDR_FORD_COUNTS PURGE;
CREATE TABLE XDR_FORD_COUNTS
   (	TABLE_NAME VARCHAR2(30 BYTE), 
	PAT_COUNT NUMBER,
	TOTAL_COUNT NUMBER,
	LOAD_TIME timestamp default systimestamp,
    DESCRIPTION varchar2(250 BYTE));


-- *******************************************************************************************************
-- STEP 1.2
--          Create a driver for departments with Internal Medicine, Primary Care, and Family Practice
--          This should be done ad-hoc at each site.
--          UCLA has a list of departments with a Primary Care flag already defined
-- *******************************************************************************************************
drop table XDR_FORD_PC_DEPT_DRV purge;
create table XDR_FORD_PC_DEPT_DRV as 
select DISTINCT DEPARTMENT_ID
,DEPARTMENT_NAME
,SPECIALTY
from i2b2.lz_clarity_dept
where primary_care = 1;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_ford_PC_dept_drv' AS TABLE_NAME
	--,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Create a driver for departments with Internal Medicine, Primary Care, and Family Practice' AS DESCRIPTION
FROM xdr_ford_PC_dept_drv;
COMMIT;


-- *******************************************************************************************************
-- STEP 1.3
--          Create a driver for diagnoses codes. At other sites, they will receive this driver and load it into their system
--
-- *******************************************************************************************************
  drop table XDR_FORD_DXDRV_ICD purge;
  CREATE TABLE "XDR_FORD_DXDRV_ICD" 
   (	"ICD_TYPE" NUMBER, 
	"ICD_CODE" VARCHAR2(254 BYTE), 
	"ICD_DESC" VARCHAR2(254 BYTE), 
	"DX_FLAG" VARCHAR2(254 BYTE)
    -- "HIV" NUMBER, 
	-- "MENTAL_HEALTH" NUMBER, 
	-- "ALCOHOL_ABUSE" NUMBER, 
	-- "OTHERS" NUMBER
   ) ;

    --------------------------------------------------------------
    -- Step 1.3.1: Load DX driver support file from [XDR_FORD_DX_LOOKUP_TEMP.csv]   
    --------------------------------------------------------------

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DXDRV_ICD' AS TABLE_NAME
	,COUNT(*) AS TOTAL_COUNT
    ,'Total Load ICD dx code driver support file' AS DESCRIPTION
FROM XDR_FORD_DXDRV_ICD;
COMMIT;

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DXDRV_ICD' AS TABLE_NAME
        ,TOTAL_COUNT
        ,'Load ICD dx code driver support file for ' + DX_FLAG AS DESCRIPTION
FROM (
        SELECT 
            DX_FLAG
            ,COUNT(DISTINCT ICD_CODE) AS TOTAL_COUNT
        FROM XDR_FORD_DXDRV_ICD
        GROUP BY DX_FLAG
);
COMMIT;

    --------------------------------------------------------------
    -- Step 1.3.2: Create final table including the dx_id from refference DX ICD CODE tables
    --------------------------------------------------------------
drop table XDR_FORD_DXDRV purge;
create table XDR_FORD_DXDRV as 
select edg.dx_id
,drv.*
from XDR_FORD_DXDRV_ICD      drv
join edg_current_icd9           edg on drv.icd_CODE = edg.CODE and drv.icd_type = 9
UNION
select edg.dx_id
,drv.*
from XDR_FORD_DXDRV_ICD      drv
join edg_current_icd10           edg on drv.icd_CODE = edg.CODE and drv.icd_type = 10
;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DXDRV' AS TABLE_NAME
	,COUNT(*) AS TOTAL_COUNT
    ,'Total Load ICD dx code driver support file' AS DESCRIPTION
FROM XDR_FORD_DXDRV;
COMMIT;

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DXDRV' AS TABLE_NAME
        ,TOTAL_COUNT
        ,'Load ICD dx code driver support file for ' + FLAG AS DESCRIPTION
FROM (
        SELECT 
            FLAG
            ,COUNT(*) AS TOTAL_COUNT
        FROM XDR_FORD_DXDRV
        GROUP BY FLAG
);
COMMIT;




-- *******************************************************************************************************
-- STEP 1.4
--          Create a driver list for HIV care providers
--          This should be done ad-hoc at each site.
-- *******************************************************************************************************
DROP TABLE XDR_FORD_PROVDRV PURGE;
CREATE TABLE "XDR_FORD_PROVDRV" 
(	"PROVIDER_ID" VARCHAR2(18 BYTE), 
"PROVIDER_NAME" VARCHAR2(200 BYTE), 
"PROVIDER_DISPLAY_NAME" VARCHAR2(221 BYTE), 
"PROVIDER_TYPE" VARCHAR2(254 BYTE), 
"STAFF_RESOURCE" VARCHAR2(27 BYTE), 
"PRIMARY_SPECIALTY" VARCHAR2(254 BYTE), 
"IS_RESIDENT_YN" VARCHAR2(28 BYTE), 
"CLINICIAN_TITLE" VARCHAR2(66 BYTE)
);

    --------------------------------------------------------------
    -- Step 1.4.1: Load HIV Care provider table
    --------------------------------------------------------------
INSERT INTO XDR_FORD_PROVDRV 
select distinct  prv.PROVIDER_ID
,prv.PROVIDER_NAME
,prv.PROVIDER_DISPLAY_NAME
,prv.PROVIDER_TYPE
,prv.STAFF_RESOURCE
,prv.PRIMARY_SPECIALTY
,prv.IS_RESIDENT_YN
,prv.CLINICIAN_TITLE
from V_CUBE_D_PROVIDER  prv
WHERE
    UPPER(prv.prov_type) = 'PHYSICIAN'
/*
apply your selection criteria here
*/
;


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PROVDRV' AS TABLE_NAME
	,COUNT(*) AS TOTAL_COUNT
    ,'Load HIV Care providers table' AS DESCRIPTION
FROM XDR_FORD_PROVDRV;
COMMIT;



----------------------------------------------------------------------------
--Step 1.5:     Create driver table for HIV labs relevant to the study

----------------------------------------------------------------------------/
    --------------------------------------------------------------
    -- Step 1.5.1: Create table to load the labs support file
    --------------------------------------------------------------
DROP TABLE XDR_FORD_LABDRV PURGE;
CREATE TABLE XDR_FORD_LABDRV
   (
    "LOINC_MAPPING" VARCHAR2(20 BYTE),
	"LOINC_LONG_NAME" VARCHAR2(254 BYTE),
    "LAB_FLAG" VARCHAR2(100 BYTE)
    );

    --------------------------------------------------------------
    -- Step 1.5.2: Load lab driver support file from [XDR_FORD_LABDRV.csv]
    --------------------------------------------------------------
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_LABDRV' AS TABLE_NAME
	,COUNT(*) AS TOTAL_COUNT
    ,'Total Load HIV labs support file' AS DESCRIPTION
FROM XDR_FORD_LABDRV;
COMMIT;

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_LABDRV' AS TABLE_NAME
        ,TOTAL_COUNT
        ,'Load HIV labs support file for ' + LAB_FLAG AS DESCRIPTION
FROM (
        SELECT 
            LAB_FLAG
            ,COUNT(*) AS TOTAL_COUNT
        FROM XDR_FORD_LABDRV
        GROUP BY LAB_FLAG
);
COMMIT;

