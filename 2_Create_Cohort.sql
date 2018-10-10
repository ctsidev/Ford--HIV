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
DROP TABLE XDR_FORD_DX_HIV PURGE;
CREATE TABLE XDR_FORDXDR_FORD_DX_HIV_PAT_HIV AS
SELECT pat_id
,MIN(EFFECTIVE_DATE) AS FIRST_HIV_DATE
FROM (
        -- UCLA has legacy data that we be brought it
        SELECT DISTINCT dx.pat_id
               ,dx.EFFECTIVE_DATE
        FROM i2b2.int_dx             dx
        JOIN XDR_FORD_DXDRV         drv ON dx.dx_id = drv.dx_id AND drv.dx_flag = 'HIV' 
        UNION
        SELECT DISTINCT dx.pat_id
               ,dx.contact_date
        FROM pat_enc_dx             dx
        JOIN XDR_FORD_DXDRV         drv ON dx.dx_id = drv.dx_id AND drv.dx_flag = 'HIV' 
    )
GROUP BY pat_id;      --3160

-- SELECT COUNT(*),COUNT(PAT_ID) FROM XDR_FORD_PAT_HIV;--5866(10/04/18)        5782	5782
--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX_HIV' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Counts for HIV diagnoses' AS DESCRIPTION
FROM XDR_FORD_DX_HIV;
COMMIT;


-- PULL HIV LAS AND APPLY LOGIC
-- MERGE PATIENTS FROM HIV DX ONLY, HIV LAB ONLY, AND HIV DX + HIV LAB,

----------------------------------------------------------------------------
--Step 2.2:     Pull Labs based on lab driver LOINC codes
--               order_type_c = 7 is Lab Test, Check the codes at your site 
----------------------------------------------------------------------------


--pull all patients or only those with DX? 10/10/18 
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
                drv.LAB_FLAG,
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
                  else ord_num_value end as harm_num_val,
                cc.common_name
              FROM order_results           o
              JOIN order_proc              p   ON p.order_proc_id = o.order_proc_id
              JOIN order_proc_2            op2 on p.ORDER_PROC_ID = op2.ORDER_PROC_ID 
              JOIN js_xdr_walling_final_pat_coh    coh ON p.pat_id = coh.pat_id AND (coh.PL_CIRRHOSIS = 1 OR COH.DX_CIRRHOSIS = 1)
              JOIN clarity_component       cc  ON o.component_id = cc.component_id
              LEFT JOIN lnc_db_main                ldm ON CC.DEFAULT_LNC_ID = ldm.record_id 
              join XDR_FORD_labDRV     drv ON coalesce(ldm.lnc_code, cc.loinc_code)  = drv.LOINC_MAPPING
              where 
                      p.order_type_c in (7)--, 26, 62, 63)			--double check this codes
                      --and p.ordering_date between to_date('03/01/2013','mm/dd/yyyy') and to_date('05/08/2018','mm/dd/yyyy')
                      --and p.ordering_date between to_date('03/01/2013','mm/dd/yyyy') and to_date('05/08/2018','mm/dd/yyyy')
                      and o.ord_value is not null
                      and o.order_proc_id is not null
                      -- AND p.order_time BETWEEN SYSDATE - (365.25 * 3) AND SYSDATE;
                      ;
--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
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
--                  OR REGEXP_LIKE (lab.ord_value, 'NEGATIVE','i') ---  NEG will also cover this
                  OR REGEXP_LIKE (lab.ord_value, 'NEG','i') 
                  THEN 0
            WHEN UPPER(lab.ord_value) LIKE 'REACTIVE' 
                  OR REGEXP_LIKE (lab.ord_value, 'LY REACTIVE','i')                      --- This will cover the next three lines.
--                  OR REGEXP_LIKE (lab.ord_value, 'WEAKLY REACTIVE','i'
--                  OR REGEXP_LIKE (lab.ord_value, 'STRONGLY REACTIVE','i')
--                  OR REGEXP_LIKE (lab.ord_value, 'REPEATEDLY REACTIVE','i')
                  OR REGEXP_LIKE (lab.ord_value, 'POSITIVE','i') 
                      THEN 1            
            ELSE null 
            END LAB_FLAG
  FROM XDR_FORD_HIVlab lab;

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_HIVlab_res' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Counts for HIV labs where results were harmonized' AS DESCRIPTION
FROM XDR_FORD_HIVlab_res;
COMMIT;



-- --FROM i2b2.lz_clarity_lab          lab   
-- -- where 
-- --      lab.proc_id IN (
-- --      SELECT DISTINCT PROC_ID FROM xdr_ford_lab_sel WHERE HIV = 1  --HIV driver table
-- --      )
--      ;
--      --in (7608, 107633)   --ucrex tests being used at that time HIV
-- CREATE INDEX XDR_FORD_HIVlab_PATIDIX ON XDR_FORD_HIVlab(pat_id);
-- CREATE INDEX XDR_FORD_HIVLAB_PRREix ON XDR_FORD_HIVlab(proc_id,ord_value,lab_flag);
-- CREATE INDEX XDR_FORD_HIVLAB_PROCIDIX ON XDR_FORD_HIVlab(PROC_ID);
-- CREATE INDEX XDR_FORD_HIVLAB_COMPIDIX ON XDR_FORD_HIVlab(COMPONENT_ID);
-- SELECT COUNT(*) FROM XDR_FORD_HIVlab;                                             --48481(10/04/2018)       178019(4/7/17)
-- SELECT COUNT(DISTINCT pat_id) FROM XDR_FORD_HIVlab;                               --4235(10/04/2018)       118677(4/7/17)



--CREATE TABLE WITH HIV LAB PATIENTS to determine 
--(DO WE NEED TO USE HARMOZIED VALUE?)



--Add other HIV patients identified from LABS that have not previously identified by HIV DX (Are there any?)
/*
POSITIVE RESULTS:
A patient shall be labelled as positive if:
1.	IF a test with proc_id = 107639(HIV-1 AB WESTERN BLOT) WHERE results = positive
a.	OR if test with proc_id = 107691(HIV-1 RNA QUANT PCR) WHERE results > 200 
b.	OR test with proc_id = 107665(HIV-1 QUANTITATION PCR) WHERE results > 20 
c.	OR test with proc_id = 107683(HIV-1 DNA,QUALITATIVE PCR) WHERE results = ‘DETECTED’ 
d.	OR test with proc_id = 107673(HIV-1 DIRECT AG (NON-ID) ELISA) WHERE results = ‘POSITIVE’ 
e.	OR IF a test with proc_id = 107709(HIV-1 GENOTYPE RT AND PR) WHERE results = positive is found THEN ‘positive’ ;
2.	ELSE IF an Antibody test (proc_ids = 107633, 7608, 327238, 244223, 327256, 728018, 327252, 725902, 327242, 327260, 60993, 4954, 56608) WHERE results = positive THEN ‘positive’ ;
*/
-- create HIB labs repository to implement new logic to analyze resutls
----------------------------------------------------------------------------

----------------------------------------------------------------------------
--Step 2.3:     Harmonize lab results to apply HIV algorithm
----------------------------------------------------------------------------
drop table xdr_FORD_HIV_confirmed purge;
create table xdr_FORD_HIV_confirmed as 
SELECT --1449
      distinct RES.pat_id, 1 as HIV_TEST
FROM XDR_FORD_HIVlab_res RES      
WHERE 
step = '2'
-- component_id --with a positive confirmatory test, (assumes the screening Ab test is positive)
--             IN (SELECT DISTINCT component_id
--                 FROM XDR_FORD_LABDRV
--                 WHERE 
--                 --description like '% ANTIBODY %' 
--                 step = '2'
--                 )
AND RES.LAB_FLAG = 1
UNION
--i.    Amplicor, RealTime TaqMan V1, Taqman V2 HIV RNA >200 HIV mRNA copies/mL (in your list proc_id=107691); 
          SELECT --171
                distinct RES.pat_id, 1 as HIV_TEST
          FROM XDR_FORD_HIVlab_res RES
          WHERE --PROC_ID = '107691'
                step = '1A'
                AND (harm_num_val <> 9999999  AND harm_num_val >  200)--692
UNION
--ii, COBAS® AmpliPrep/COBAS TaqMan® HIV-1 (qPCR for HIV RNA) range is 20–10,000,000 HIV-1 RNA copies/mL (1.30-7.00 log copies/mL), so value >20 would be a positive (is this: 107665), 
          SELECT --434
                distinct RES.pat_id, 1 as HIV_TEST
          FROM XDR_FORD_HIVlab_res     RES
          WHERE --PROC_ID = '107665'
                step = '1B'
                AND ((harm_num_val <> 9999999  AND harm_num_val >  20)  --OR (REGEXP_LIKE (ORD_VALUE,'>(100|75)','i')))  --new addition  --for future dev it needs to strip numeric value and work for all
UNION
--iii, quantitative HIV PCR (107683) 
          SELECT --45
                distinct RES.pat_id, 1 as HIV_TEST
          FROM XDR_FORD_HIVlab_res    RES
          WHERE --PROC_ID = '107683' 
                step = '1C'
                AND ORD_VALUE = 'DETECTED'
UNION
--iv, positive p24 (107673)
          SELECT --3
                distinct RES.pat_id, 1 as HIV_TEST
          FROM XDR_FORD_HIVlab_res   RES   
          WHERE --PROC_ID = '107673' 
                step = '1D'
                AND LAB_FLAG = 1
UNION
--western blot (107639); 
          SELECT --702
                distinct pat_id, 1 as HIV_TEST
          FROM XDR_FORD_HIVlab_res     RES
          WHERE --PROC_ID = '107639' 
                step = '1'
                AND LAB_FLAG = 1 --600 (702)
UNION
--will return an interpretable test of positive if >600 copies/mL then positive for HIV 
--(requires >600 copies/mL and for interpretation 25% representative population of HIV virions circulating to properly assess for virus mutations)
            SELECT --590
                  distinct pat_id, 1 as HIV_TEST
            FROM XDR_FORD_HIVLAB     RES
            WHERE --PROC_ID = '107709' 
                step = '2'    
                AND ORD_VALUE = 'DETECTED'  --384  (590)
;
commit;
SELECT COUNT(*) FROM XDR_FORD_HIV_CONFIRMED;  --2468(10/04/2018)            1616(4/7/17)

INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_HIV_confirmed' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Counts for HIV labs where results were harmonized' AS DESCRIPTION
FROM xdr_FORD_HIV_confirmed;
COMMIT;









--DID WE FIND ANY PATIENT THAT WAS NOT ALREADY IN THE HIV DX PATIENT TABLE?
SELECT COUNT(DISTINCT PAT_ID) FROM xdr_FORD_HIV3 WHERE PAT_ID NOT IN (SELECT PAT_ID FROM XDR_FORD_HIV);   --333

--IF WE DID, INSERT PATIENT INTO 
alter table XDR_FORD_HIV add( lab_only number);

INSERT INTO XDR_FORD_HIV (pat_id, LAB_ONLY)
SELECT DISTINCT PAT_ID,1 as lab_only FROM xdr_FORD_HIV3 WHERE PAT_ID NOT IN (SELECT PAT_ID FROM XDR_FORD_HIV);
COMMIT;
--333 rows inserted
--------------------------------------------------------------------------------
-- Investigator Counts Review Pull ---------------------------------------------
--------------------------------------------------------------------------------
/*DROP TABLE xdr_Ford_labdrv PURGE;
CREATE TABLE xdr_Ford_labdrv AS
SELECT EXTRACT(YEAR FROM specimn_taken_time) AS year_specimn_taken_time
,proc_id, description, component_id, component_name, COUNT(*) AS total 
	FROM xdr_Ford_laball    
  GROUP BY EXTRACT(YEAR FROM specimn_taken_time),proc_id, description, component_id, component_name
  ORDER BY EXTRACT(YEAR FROM specimn_taken_time)
  ,component_name;
CREATE INDEX xdr_Ford_labdrv_idx ON xdr_Ford_labdrv (proc_id, component_id);

SELECT * FROM xdr_Ford_labdrv;
--------------------------------------------------------------------------------
-- Pull only labs selectd by Investigator --------------------------------------
--------------------------------------------------------------------------------
DROP TABLE xdr_Ford_lab PURGE;
CREATE TABLE xdr_Ford_lab AS 
SELECT DISTINCT lab.*
  FROM xdr_Ford_laball            lab  
  JOIN xdr_Ford_lab_sel            drv ON lab.proc_id = drv.proc_id AND lab.component_id = drv.component_id
;
CREATE INDEX xdr_Ford_lab_patidx ON xdr_Ford_lab (pat_id);
CREATE INDEX xdr_Ford_lab_labidx ON xdr_Ford_lab (proc_id, component_id);
SELECT COUNT(*) FROM xdr_Ford_lab;                                             --5823
SELECT COUNT(DISTINCT pat_id) FROM xdr_Ford_lab;                               --1789
*/

-- *******************************************************************************************************
-- STEP 
--   Create Medications List table for cohort
--      Note to remove MAR if inpatient meds are not required
--   Prerequisite(s): PatientDemographics.sql, Encounter.sql (Decide whether to link by pat_id or csn)
--
--   Modification Log
--      TT-4/6/2016: Added classes to standard pull per email from Dr. Bell on 4/5 titled "study"
--	RF- 08/16/16 - Modified to use Used_Med_ID instead of medication_id. and the meds rather than the meds2 table
--			See Setp3 in ETL for deets.
-- *******************************************************************************************************
---- For IP Meds (See ORDERING MODE) there must be a mar.taken_time or last_admin_inst.
--------------------------------------------------------------------------------
-- Pull all meds to get counts -------------------------------------------------
--------------------------------------------------------------------------------
--We only need to pull HIV drugs
-- I looked for HIV dx patients and pull all their meds and sent counts to PI (she wanted counts by year
-- to take into account protocols changes over time)
-- she finally provided a link to a list of meds with brand and genetric names
-- the final pull only captures meds for HIV patients since it is not used with the rest

DROP TABLE xdr_Ford_medall PURGE;
CREATE TABLE xdr_Ford_medall AS
SELECT DISTINCT pat.pat_id
               --,pat.pat_mrn_id
               --,pat.study_id
               ,med.pat_enc_csn_id
               ,med.order_med_id
               ,med.used_med_id as medication_id
               ,med.medication_name
               ,med.generic_name
               ,med.ordering_mode
               ,med.ordering_date
               ,med.start_date
               ,med.end_date
               ,med.order_status
               ,xmrs.NAME                                                       AS result
               ,nvl(mar.taken_time, med.ordering_date)                          AS taken_time_order_date
               ,nvl(mar.sig, med.hv_discrete_dose)                              AS dose
               ,mar.taken_time
               ,mar.sig
               ,med.dose_unit
               ,med.order_class
               ,med.last_admin_inst
               ,med.quantity
               ,med.pharm_class
               ,med.thera_class
               ,med.pharm_subclass
  FROM XDR_FORD_HIV                          pat
  JOIN i2b2.lz_clarity_meds                 med   ON pat.pat_id = med.pat_id
--  FROM xdr_Ford_enc                          pat
--  JOIN i2b2.lz_clarity_meds                 med   ON pat.pat_enc_csn_id  = med.pat_enc_csn_id
  LEFT JOIN CLARITY.mar_admin_info  mar   ON med.order_med_id = mar.order_med_id
  LEFT JOIN CLARITY.zc_mar_rslt     xmrs  ON mar.mar_action_c = xmrs.result_c
  WHERE ((med.ordering_mode = 'Inpatient'                                       
            AND nvl(mar.taken_time,to_date('01/01/0001')) <> '01/01/0001'       -- taken_time was valid
            AND nvl(mar.sig,-1) > 0                                             -- and SIG was valid and > 0
            AND nvl(mar.mar_action_c,-1) <> 125                                 -- and action was anything other than 'Not Given'
         ) 
         OR med.ordering_mode != 'Inpatient'
        )
    AND med.used_med_id IS NOT NULL
    --AND nvl(mar.taken_time, med.ordering_date) >= pat.HIV_DX_DATE
	AND(
  regexp_like(med.generic_name,'(abacavir|atazanavir|azidothymidine|cobicistat|darunavir|didanosine|dideoxyinosine|dolutegravir|efavirenz|elvitegravir|emtricitabine|enfuvirtide|etravirine|fosamprenavir|fumarate|indinavir|lamivudine|lopinavir|maraviroc|nelfinavir|nevirapine|raltegravir|rilpivirine|ritonavir|saquinavir|stavudine|tenofovir|tipranavir|zidovudine)','i')
	OR
	regexp_like(med.medication_name,'(Aptivus|Atripla|Combivir|Complera|Crixivan|Descovy|Edurant|Emtriva|Epivir|Epzicom|Evotaz|Fuzeon|Genvoya|Intelence|Invirase|Isentress|Kaletra|Lexiva|Norvir|Odefsey|Prezcobix|Prezista|Retrovir|Reyataz|Selzentry|Stribild|Sustiva|Tivicay|Triumeq|Trizivir|Truvada|Tybost|Videx|Viracept|Viramune|Viread|Vitekta|Zerit|Ziagen)','i')
	)
--  ORDER BY pat.PAT_ID, ordering_mode, nvl(mar.taken_time, med.ordering_date)
;
CREATE INDEX xdr_Ford_medall_patidx ON xdr_Ford_medall (pat_id);
--CREATE INDEX xdr_Ford_medall_medidx ON xdr_Ford_medall (used_med_id);
SELECT COUNT(*) FROM xdr_Ford_medall;                                          --56470(4/7/17)      420,952(4/3/17)
SELECT COUNT(DISTINCT pat_id) FROM xdr_Ford_medall;                            --2340(4/7/17)     2,876(4/3/17)
  
  
  
--------------------------------------------------------------------------------
-- Investigator Counts Review Pull ---------------------------------------------
--------------------------------------------------------------------------------
/*DROP TABLE xdr_Ford_meddrv PURGE;
CREATE TABLE xdr_Ford_meddrv AS
SELECT extract(year from med.TAKEN_TIME_ORDER_DATE) as year_TAKEN_TIME_ORDER_DATE
      ,med.medication_id
      ,med.medication_name
      ,med.generic_name
      ,pc.name              AS pharm_class
      ,tc.name              AS thera_class
      ,sc.name              AS pharm_subclass
      ,COUNT(*)             AS total 
	FROM xdr_Ford_medall                     med
  JOIN clarity.clarity_medication       cm  ON med.medication_id = cm.medication_id
  LEFT JOIN clarity.zc_pharm_class      pc  ON cm.pharm_class_c = pc.pharm_class_c
  LEFT JOIN clarity.zc_thera_class      tc  ON cm.thera_class_c = tc.thera_class_c
  LEFT JOIN clarity.zc_pharm_subclass   sc  ON cm.pharm_subclass_c = sc.pharm_subclass_c
  GROUP BY extract(year from med.TAKEN_TIME_ORDER_DATE),med.medication_id, med.medication_name, med.generic_name, pc.name, tc.name, sc.name
  ORDER BY extract(year from med.TAKEN_TIME_ORDER_DATE),med.medication_name;
CREATE INDEX xdr_Ford_meddrv_idx ON xdr_Ford_meddrv (medication_id);


SELECT * FROM xdr_Ford_meddrv;
--------------------------------------------------------------------------------
-- Pull only meds selected by Investigator --------------------------------------
--------------------------------------------------------------------------------
DROP TABLE xdr_Ford_med PURGE;
CREATE TABLE xdr_Ford_med AS 
SELECT DISTINCT med.*
               --,orm.refills           --Use only if requested by investigator
               --,omop.mapped_rxnorm    --Use only if requested by investigator
  FROM xdr_Ford_medall            med  
  JOIN xdr_Ford_meddrv            drv  ON med.medication_id = drv.medication_id 
  --JOIN order_med@ttacorda_clarityp orm  ON med.order_med_id = orm.order_med_id
  --LEFT JOIN i2b2.omop_med_mapping  omop ON med.medication_id = omop.medication_id
;
CREATE INDEX xdr_Ford_med_patidx ON xdr_Ford_med (pat_id);
CREATE INDEX xdr_Ford_med_medidx ON xdr_Ford_med (medication_id);
SELECT COUNT(*) FROM xdr_Ford_med;                                             --
SELECT COUNT(DISTINCT pat_id) FROM xdr_Ford_med;                               --
*/
-- *******************************************************************************************************
-- STEP 1
--   Create the Patient table
-- *******************************************************************************************************
drop  table xdr_ford_pat purge;
create table xdr_ford_pat as 
select DISTINCT enc.pat_id
,pat.MAPPED_RACE_NAME
/*
	
1	White or Caucasian
8	Unknown
2	Black or African American
6	Other
3	American Indian or Alaska Native
7	Patient Refused
4	Asian
900	Multiple Races
5	Native Hawaiian or Other Pacific Islander
*/
,pat.ETHNIC_GROUP        
		,pat.MARITAL_STATUS AS MARITAL_STATUS_DESC
		,CASE WHEN pat.MARITAL_STATUS_C = 1 THEN  'SINGLE'
				WHEN pat.MARITAL_STATUS_C = 2 THEN  'MARRIED'
				WHEN pat.MARITAL_STATUS_C = 4 THEN  'SEPARATED'
				ELSE 'OTHER'
/*  1-Single   
  2-Married   
  3-Divorced   
  4-Separated   
  5-Life Partner   
  6-Widowed   
  998-Unknown   
 */
 
		END MARITAL_STATUS
        
		,pat.LANGUAGE		AS LANGUAGE_DESC
		,CASE WHEN pat.LANGUAGE_C = 22 THEN 'ENGLISH'
		WHEN pat.LANGUAGE_C = 96 THEN 'SPANISH'
		ELSE 'OTHER'
		END LANGUAGE
        
		  --Employment status
					,zem.NAME as employment_status_DESC
					,CASE WHEN pt2.EMPY_STATUS_C IN (1,2,4,7) THEN 'EMPLOYED'
						  WHEN pt2.EMPY_STATUS_C = 5 THEN 'RETIRED'
						  ELSE 'OTHER_or_UNEMPLOYED'
					end EMPLOYMENT_STATUS
							/*
							1-Full Time   
							2-Part Time   
							3-Not Employed   
							4-Self Employed   
							5-Retired   
							6-On Active Military Duty   
							7-Student - Full Time   
							8-Student - Part Time   
							9-Unknown   
							*/
 

          
		  
        ,pat.BIRTH_DATE
        --Insurance type 
        ,pat.FINANCIAL_CLASS		AS FINANCIAL_CLASS_DESC
        		/*,CASE WHEN pat.FINANCIAL_CLASS = 'Commercial' THEN 'COMMERCIAL'
				WHEN pat.FINANCIAL_CLASS = 'Medicare' THEN 'MEDICARE'
				WHEN pat.FINANCIAL_CLASS = 'Medicaid' THEN 'MEDICAID'
				ELSE 'UNKNOWN'*/
				--WHEN FINANCIAL_CLASSC IN (1) THEN 'COMMERCIAL'
				  
/*
1-Commercial 
2-Medicare 
3-Medicaid 
4-Self-pay 
5-Worker's Comp 
6-Tricare 
7-Champva 
8-Group Health Plan 
9-FECA Black Lung 
10-Blue Shield 
11-Medigap 
12-Other 
*/ 

		--END FINANCIAL_CLASS
        ,pat.SEX
        --Incomplete info on patient’s address 
          ,pt2.ADD_LINE_1
          ,pt2.CITY
          ,pt2.ZIP
            ,CASE WHEN 
						pt2.ADD_LINE_1 is null
						or
						pt2.ADD_LINE_1 IN (' ','.',',','0','00','000','0000')
						-- no PO boxes
						or
						  (
						  UPPER(pt2.ADD_LINE_1) like '%BOX%'
						  and
						  UPPER(pt2.ADD_LINE_1) like '%PO%'
						  )
						or --no homeless
						UPPER(pt2.ADD_LINE_1) = 'HOMELESS'
						or --INVALID ADDRESSES
						UPPER(pt2.ADD_LINE_1) IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
						or--INVALID ADDRESSES
						UPPER(pt2.ADD_LINE_1) LIKE '%NO ADDRESS%'
						or
						UPPER(pt2.ADD_LINE_1) LIKE '%NOT KNOWN%'
						or
						UPPER(pt2.ADD_LINE_1) LIKE 'NO STREET'
						or
						UPPER(pt2.ADD_LINE_1) LIKE '%UNKNOWN%'
						or
						UPPER(pt2.ADD_LINE_1) LIKE '%0000%'
						OR
						pt2.CITY IS NULL
						or
						pt2.CITY  IN (' ','.',',','0','00','000','0000')
						or
						pt2.ZIP IS NULL  
						OR--HOMELESS
						UPPER(pt2.CITY) = 'HOMELESS'
						or--INVALID ADDRESSES
						UPPER(pt2.CITY) IN ('RETURN MAIL','MAIL RETURNED','BAD ADDRESS')
						or
						UPPER(pt2.CITY) LIKE '%NO CITY%'
						or
						UPPER(pt2.CITY) LIKE '%UNKNOWN%'
						or
						UPPER(pt2.CITY) LIKE '%#%' THEN 1
					ELSE 0
			END INCOMPLETE_ADDRESS
from xdr_Ford_coh                     enc
left join i2b2.lz_clarity_patient     pat ON enc.pat_id =  pat.pat_id
left join clarity.patient             pt2 ON enc.pat_id =  pt2.pat_id
left join clarity.ZC_EMPY_STAT        zem ON pt2.EMPY_STATUS_C = zem.EMPY_STAT_C
--WHERE enc.ENCOUNTER_FLAG IS NOT NULL;
;
ALTER TABLE xdr_Ford_pat ADD CONSTRAINT xdr_Ford_pat_pk PRIMARY KEY (pat_id);
SELECT COUNT(*) FROM xdr_Ford_pat;                                             --1070409(4/7/17)      1069238(4/4/17)
SELECT COUNT(DISTINCT pat_id) FROM xdr_Ford_pat;                               --1070409(4/7/17)      1069238(4/4/17)
