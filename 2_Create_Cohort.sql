/********************************************************************************************************

    Step 2
        Create Cohort table

********************************************************************************************************/


-- *******************************************************************************************************
-- STEP 2.1
--          Create HIV DX patients table
-- to be used to pull labs and meds counts
--
-- *******************************************************************************************************
DROP TABLE XDR_FORD_DX_HIV_coh PURGE;
CREATE TABLE XDR_FORD_DX_HIV_coh AS
SELECT pat_id
,MIN(contact_date) AS FIRST_HIV_DATE
FROM (
        SELECT DISTINCT dx.pat_id
               ,dx.contact_date
        FROM pat_enc_dx             dx
        JOIN XDR_FORD_DXDRV         drv ON dx.dx_id = drv.dx_id AND drv.dx_flag = 'HIV' 
        WHERE dx.contact_date BETWEEN '03/02/2013' AND '02/28/2018'
    )
GROUP BY pat_id;      --3160


INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT, TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX_HIV_coh' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Counts for HIV diagnoses' AS DESCRIPTION
FROM XDR_FORD_DX_HIV_coh;
COMMIT;


----------------------------------------------------------------------------
--Step 2.2:     Pull Labs based on lab driver LOINC codes
--               order_type_c = 7 is Lab Test, Check the codes at your site 
----------------------------------------------------------------------------
DROP TABLE xdr_Ford_HIVlab PURGE;
CREATE TABLE xdr_Ford_HIVlab AS 
SELECT 	DISTINCT coh.pat_id,
                o.pat_enc_csn_id, 
                o.order_proc_id, 
                p.proc_id, 
                p.proc_code, 
                p.description, 
                o.component_id, 
                cc.name component_name, 
                p.order_time, 
                p.result_time, 
                o.result_date, 
                trim(o.ord_value) as ord_value, 
                o.ord_num_value, 
                o.reference_unit, 
                o.ref_normal_vals, 
                o.reference_low, 
                o.reference_high,
                p.order_status_c, 
                p.order_type_c,
                o.RESULT_FLAG_C,
                op2.specimn_taken_time,
                drv.STEP,
		--If there is a relevant operator in this field ('%','<','>','='), it gets captured in its own field
                case when regexp_like(ord_value,'[%<>]=*','i') then regexp_substr(o.ord_value,'[><%]=*') else null end as harm_sign,
                trim(o.ord_value) as harm_text_val,
		/*
		In the following case statement, the code identifies three different value patterns and applies different strategies to clean the data:
		-If the result includes ':', or text, ':' it replaces with a default value. Ex 'NEGATIVE' or '12-19-08 6:45AM' --> '9999999'
		-If the result includes '<','>',or'=', the code strips that character and formats the number accordingly. Ex '<0.04' --> '0.04')
		-If the result includes '%', the code strips that character and formats the number accordingly. Ex. '28%' --> '28'
		
		All formatting shall respect decimal values
		*/
                case when regexp_like(ord_value,':','i')
                  or regexp_substr(ord_value,'[1-9]\d*(\.\,\d+)?') is null
                       then ord_num_value
                  when regexp_like(ord_value,'[<>]=*','i')
                       then to_number(regexp_substr(ord_value,'-?[[:digit:],.]*$'),'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,''' )
                  when regexp_like(ord_value,'%','i') 
                       then to_number(regexp_substr(ord_value,'[1-9]\d*(\.\,\d+)?'),'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,''' )
                  else ord_num_value end as harm_num_val
                  ,cc.common_name
              FROM order_results           o
              JOIN order_proc              p   ON p.order_proc_id = o.order_proc_id
              JOIN order_proc_2            op2 on p.ORDER_PROC_ID = op2.ORDER_PROC_ID 
              JOIN patient                 coh ON p.pat_id = coh.pat_id
              JOIN clarity_component       cc  ON o.component_id = cc.component_id
              LEFT JOIN lnc_db_main        ldm ON CC.DEFAULT_LNC_ID = ldm.record_id 
              join XDR_FORD_labDRV         drv ON coalesce(ldm.lnc_code, cc.loinc_code)  = drv.BIP_LOINC_MAPPING
            --   join XDR_FORD_labDRV         drv ON p.proc_id = drv.proc_id and o.component_id = drv.component_id
              where 
                      p.order_type_c in (7)--, 26, 62, 63)			--double check this codes
                      --and p.ordering_date between to_date('03/01/2013','mm/dd/yyyy') and to_date('05/08/2018','mm/dd/yyyy')
                      --and p.ordering_date between to_date('03/01/2013','mm/dd/yyyy') and to_date('05/08/2018','mm/dd/yyyy')
                      and o.ord_value is not null
                      and o.order_proc_id is not null
                      AND p.order_time BETWEEN '03/02/2013' AND '02/28/2018';

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_Ford_HIVlab' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Counts for HIV labs' AS DESCRIPTION
FROM xdr_Ford_HIVlab;
COMMIT;

----------------------------------------------------------------------------
--Step 2.3:     Harmonize lab results to apply HIV algorithm
----------------------------------------------------------------------------
drop table XDR_FORD_HIVlab_res purge;
create table XDR_FORD_HIVlab_res as
SELECT DISTINCT --lab.pat_id, 
lab.*,
       --lab.PAT_MRN_ID,
        CASE WHEN 
                  (REGEXP_LIKE (lab.ord_value, 'NO','i')  ---  NON and NONE are redundant since they both contain NO already.
                  AND  REGEXP_LIKE (lab.ord_value,'REA(C|V)(I|T)','i') ) --- You can roll these up into just one statement.  It will look for REA followed by a C or a V followed by a I or a T.
                          OR REGEXP_LIKE (lab.ord_value, 'NR','i')
                          OR REGEXP_LIKE (lab.ord_value, 'N/R','i')
                          OR REGEXP_LIKE (lab.ord_value, 'NEG','i') 
                  THEN 0
            WHEN UPPER(lab.ord_value) LIKE 'REACTIVE' 
                  OR REGEXP_LIKE (lab.ord_value, 'LY REACTIVE','i')                      --- This will cover the next three lines.
                  OR REGEXP_LIKE (lab.ord_value, 'POSITIVE','i') 
                  OR UPPER(TRIM(lab.ord_value)) = 'DETECTED'
                      THEN 1            
            ELSE null 
            END LAB_FLAG
  FROM XDR_FORD_HIVlab lab;
  SELECT count(*) from XDR_FORD_HIVlab_res WHERE LAB_FLAG = 1                ;--5293        730          689


----------------------------------------------------------------------------
--Step 2.4:     Flag positive HIV patients based on lab results
----------------------------------------------------------------------------
drop table xdr_FORD_lab_HIV_COH purge;
create table xdr_FORD_lab_HIV_COH as 
SELECT --1449
      distinct RES.pat_id
      ,RESULT_DATE
FROM XDR_FORD_HIVlab_res RES      
WHERE 
      step = '2'
      AND RES.LAB_FLAG = 1
UNION
--i.    Amplicor, RealTime TaqMan V1, Taqman V2 HIV RNA >200 HIV mRNA copies/mL (in your list proc_id=107691); 
          SELECT --171
                distinct RES.pat_id, RESULT_DATE
          FROM XDR_FORD_HIVlab_res RES
          WHERE 
                step = '1A'
                AND (harm_num_val <> 9999999  AND harm_num_val >  200)--692
UNION
--ii, COBAS® AmpliPrep/COBAS TaqMan® HIV-1 (qPCR for HIV RNA) range is 20–10,000,000 HIV-1 RNA copies/mL (1.30-7.00 log copies/mL), so value >20 would be a positive (is this: 107665), 
          SELECT --434
                distinct RES.pat_id, RESULT_DATE
          FROM XDR_FORD_HIVlab_res     RES
          WHERE 
                step = '1B'
                AND ((harm_num_val <> 9999999  AND harm_num_val >  20))  --OR (REGEXP_LIKE (ORD_VALUE,'>(100|75)','i')))  --new addition  --for future dev it needs to strip numeric value and work for all
UNION
--iii, quantitative HIV PCR (107683) 
          SELECT --45
                distinct RES.pat_id, RESULT_DATE
          FROM XDR_FORD_HIVlab_res    RES
          WHERE 
                step = '1C'
                AND ORD_VALUE = 'DETECTED'
UNION
--iv, positive p24 (107673)
          SELECT --3
                distinct RES.pat_id, RESULT_DATE
          FROM XDR_FORD_HIVlab_res   RES   
          WHERE 
                step = '1D'
                AND LAB_FLAG = 1
UNION
--western blot (107639); 
          SELECT --702
                distinct pat_id, RESULT_DATE
          FROM XDR_FORD_HIVlab_res     RES
          WHERE 
                step = '1'
                AND LAB_FLAG = 1 --600 (702)
;

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_lab_HIV_COH' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Counts for HIV labs where results were harmonized' AS DESCRIPTION
FROM xdr_FORD_lab_HIV_COH;
COMMIT;


----------------------------------------------------------------------------
--Step 2.5:     Create final cohor table
----------------------------------------------------------------------------
DROP TABLE XDR_FORD_COH PURGE;
CREATE TABLE XDR_FORD_COH (
PAT_ID VARCHAR2(8 BYTE)
,first_hiv_dx_date DATE
,first_hiv_lab_date DATE
,COHORT_TYPE VARCHAR2(50 BYTE)
);


----------------------------------------------------------------------------
--Step 2.6:     Insert patients with HIV diagnoses
----------------------------------------------------------------------------
INSERT INTO XDR_FORD_COH(PAT_ID, first_hiv_dx_date,COHORT_TYPE,first_hiv_lab_date)
select DISTINCT dx.pat_id
        ,dx.FIRST_HIV_DATE as first_hiv_dx_date
        ,CASE WHEN lab.pat_id is null then 'ONLY DX' 
                else 'DX + LAB'
                END COHORT_TYPE
        ,lab.first_hiv_lab_date
from xdr_FORD_dx_HIV_COH    dx
LEFT JOIN (select pat_id
                ,MIN(RESULT_DATE) AS first_hiv_lab_date
            from xdr_FORD_lab_HIV_COH
            group by pat_id) lab on dx.pat_id = lab.pat_id
;
COMMIT;

----------------------------------------------------------------------------
--Step 2.7:     Insert patients with HIV labs
----------------------------------------------------------------------------
INSERT INTO XDR_FORD_COH(PAT_ID, COHORT_TYPE, first_hiv_lab_date)
select lab.pat_id
                ,'LAB ONLY' AS COHORT_TYPE
                ,MIN(lab.RESULT_DATE) AS first_hiv_lab_date
            from xdr_FORD_lab_HIV_COH       lab
            LEFT JOIN XDR_FORD_COH          coh on lab.pat_id = coh.pat_id
            WHERE COH.PAT_ID IS NULL
            group by lab.pat_id;
COMMIT;           


INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DXDRV' AS TABLE_NAME
        ,TOTAL_COUNT
        ,'Patients where cohort type = ' || COHORT_TYPE AS DESCRIPTION
FROM (
        SELECT 
            COHORT_TYPE
            ,COUNT(*) AS TOTAL_COUNT
        FROM XDR_FORD_COH
        GROUP BY COHORT_TYPE
);
COMMIT;

----------------------------------------------------------------------------
-- STEP 2.8
--   Create the Patient table
----------------------------------------------------------------------------
drop  table xdr_ford_pat purge;
create table xdr_ford_pat as 
SELECT rownum as study_id,
	pat.* 
from
	(select DISTINCT coh.pat_id
                        ,coh.FIRST_HIV_DX_DATE
                        ,coh.FIRST_HIV_LAB_DATE
                        ,coh.COHORT_TYPE                
                        ,pat.BIRTH_DATE
                        ,pat.EMAIL_ADDRESS
                        ,pat.WORK_PHONE
                        ,pat.home_phone
                        ,pat.CUR_PCP_PROV_ID
                        ,pat.PAT_MRN_ID
                        ,pat.sex
                        ,pat.MARITAL_STATUS_C
                        ,zma.name as MARITAL_STATUS
                        ,pat.LANGUAGE_C
                        ,zla.name as language
                        ,pat.ADD_LINE_1
                        ,pat.ADD_LINE_2
                        ,pat.CITY
                        ,pat.ZIP
                        ,CASE WHEN 
						pat.ADD_LINE_1 is null
						or
						pat.ADD_LINE_1 IN (' ','.',',','0','00','000','0000')
						-- no PO boxes
						or
						  (
						  UPPER(pat.ADD_LINE_1) like '%BOX%'
						  and
						  UPPER(pat.ADD_LINE_1) like '%PO%'
						  )
						or --no homeless
						UPPER(pat.ADD_LINE_1) = 'HOMELESS'
						or --INVALID ADDRESSES
						UPPER(pat.ADD_LINE_1) IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
						or--INVALID ADDRESSES
						UPPER(pat.ADD_LINE_1) LIKE '%NO ADDRESS%'
						or
						UPPER(pat.ADD_LINE_1) LIKE '%NOT KNOWN%'
						or
						UPPER(pat.ADD_LINE_1) LIKE 'NO STREET'
						or
						UPPER(pat.ADD_LINE_1) LIKE '%UNKNOWN%'
						or
						UPPER(pat.ADD_LINE_1) LIKE '%0000%'
						OR
						pat.CITY IS NULL
						or
						pat.CITY  IN (' ','.',',','0','00','000','0000')
						or
                                    REGEXP_LIKE(pat.CITY,'(RETURN.MAIL|MAIL.RETURNED|BAD.ADDRESS|NO.CITY|UNKNOWN|#)','i')
                                    or
                                                pat.ZIP IS NULL  
                                    OR 
                                    LENGTH(pat.ZIP) < 5
                                    OR
                                    REGEXP_LIKE(pat.ZIP,'###','i')
						OR--HOMELESS
						UPPER(pat.CITY) = 'HOMELESS'
						or--INVALID ADDRESSES
						UPPER(pat.CITY) IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
						or
						UPPER(pat.CITY) LIKE '%NO CITY%'
						or
						UPPER(pat.CITY) LIKE '%UNKNOWN%'
						or
						UPPER(pat.CITY) LIKE '%#%' THEN 1
					ELSE 0
			END INCOMPLETE_ADDRESS        
            from XDR_FORD_COH                     coh
            --left join i2b2.lz_clarity_patient     pat ON enc.pat_id =  pat.pat_id
            left join clarity.patient             pat ON coh.pat_id =  pat.pat_id
            left join clarity.ZC_EMPY_STAT        zem ON pat.EMPY_STATUS_C = zem.EMPY_STAT_C
            left join clarity.ZC_language        zla ON pat.language_c = zla.language_c
            left join clarity.ZC_marital_status        zma ON pat.MARITAL_STATUS_C = zma.MARITAL_STATUS_C
            LEFT JOIN clarity.zc_state xst ON pat.state_c = xst.state_c
      ) pat 
ORDER BY  dbms_random.value

;

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PAT' AS TABLE_NAME
        ,TOTAL_COUNT
        ,'Patients where cohort type = ' || COHORT_TYPE AS DESCRIPTION
FROM (
        SELECT 
            COHORT_TYPE
            ,COUNT(*) AS TOTAL_COUNT
        FROM XDR_FORD_PAT
        GROUP BY COHORT_TYPE
);
COMMIT;
------------------------------------------------------
--    Step: 2.9 Race and ethnicity
--
------------------------------------------------------
drop  table xdr_ford_pat_race purge;
create table xdr_ford_pat_race as 
select distinct coh.pat_id
,rac.PATIENT_RACE_c
,zra.name as race
from XDR_FORD_COH                   coh
LEFT JOIN clarity.PATIENT_RACE      rac ON coh.pat_id = rac.pat_id
LEFT JOIN clarity.ZC_PATIENT_RACE   zra ON rac.PATIENT_RACE_c = zra.PATIENT_RACE_c;

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT,PAT_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PAT_RACE' AS TABLE_NAME
        ,COUNT(*) AS TOTAL_COUNT
        ,COUNT(distinct pat_id) AS TOTAL_COUNT
        ,'Patients race records'
FROM  XDR_FORD_PAT_RACE;
COMMIT;



drop  table xdr_ford_pat_ethnicity purge;
create table xdr_ford_pat_ethnicity as 
select distinct coh.pat_id
,pat.ETHNIC_GROUP_C
,zet.name as ehnicity_group
from XDR_FORD_COH                       coh
left join clarity.patient               pat ON coh.pat_id =  pat.pat_id
LEFT JOIN clarity.ZC_ETHNIC_GROUP       zet ON pat.ETHNIC_GROUP_C = zet.ETHNIC_GROUP_C;


INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT,PAT_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PAT_ETHNICITY' AS TABLE_NAME
        ,COUNT(*) AS TOTAL_COUNT
        ,COUNT(distinct pat_id) AS TOTAL_COUNT
        ,'Patients ethnicity records'
FROM  xdr_ford_pat_ethnicity;
COMMIT;
------------------------------------------------------
--Geolocators (ONLY UCLA)
------------------------------------------------------
drop  table XDR_FORD_PAT_GEO purge;
create table xdr_ford_pat_geo as 
SELECT DISTINCT COH.PAT_ID
            ,geo.ADD_LINE_1
            ,geo.CITY
            ,geo.EDUCATION_CD
            ,geo.INCOME_CD
            ,geo.STATE
            ,geo.ZIP
            ,geo.X
            ,geo.Y
            ,geo.STATE_FIPS
            ,geo.CNTY_FIPS
            ,geo.STCOFIPS
            ,geo.TRACT
            ,geo.TRACT_FIPS
            ,geo.BLKGRP
            ,geo.FIPS
            ,geo.UPDATE_DATE
from XDR_FORD_COH                       coh
JOIN BIP_PAT_GEOCODE               geo on coh.pat_id = geo.pat_id;


INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT,PAT_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PAT_GEO' AS TABLE_NAME
        ,COUNT(*) AS TOTAL_COUNT
        ,COUNT(distinct pat_id) AS TOTAL_COUNT
        ,'Patients geocoding records'
FROM  XDR_FORD_PAT_GEO;
COMMIT;