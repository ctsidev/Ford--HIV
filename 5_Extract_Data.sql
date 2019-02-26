-- ******************************************************************************************************* 
-- STEP 5
--   Export the investigator data
-- *******************************************************************************************************
--------------------------------------------------------------------------------
-- STEP 5.1: Create encounter ID key table 
--------------------------------------------------------------------------------
DROP TABLE XDR_FORD_ENCKEY PURGE;
CREATE TABLE XDR_FORD_ENCKEY AS
select rownum as encounter_id
		,enc.pat_enc_csn_id
from (SELECT DISTINCT pat_enc_csn_id
FROM xdr_FORD_enc) ENC
order by pat_enc_csn_id;

-- Create index to optimize final pull/join
CREATE INDEX XDR_FORD_ENCKEY_CSNIDIX ON XDR_FORD_DX(PAT_ENC_CSN_ID);

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_ENCKEY' AS TABLE_NAME
	,NULL AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 	
    ,'Create encounter ID key table' AS 	DESCRIPTION
FROM XDR_FORD_ENCKEY;
COMMIT;



--------------------------------------------------------------------------------
-- STEP 8.2: Identifiers Pull  - 
--
--------------------------------------------------------------------------------
SELECT DISTINCT coh.study_id
                ,pat.pat_Mrn_id as mrn
                ,to_char(coh.birth_date, 'mm/dd/yyyy') as birth_date
                ,pat2.pat_name as patient_name
                ,to_char(coh.FIRST_HIV_DX_DATE, 'mm/dd/yyyy') as FIRST_HIV_DX_DATE
                ,to_char(coh.FIRST_HIV_LAB_DATE, 'mm/dd/yyyy') as FIRST_HIV_LAB_DATE
                -- ,coh.EMAIL_ADDRESS
                -- ,coh.HOME_PHONE
                -- ,coh.WORK_PHONE
                ,geo.ADD_LINE_1 as address
                ,geo.CITY
                ,geo.ZIP
                ,coh.INCOMPLETE_ADDRESS
                ,geo.EDUCATION_CD                
                ,geo.INCOME_CD
                ,geo.STATE
                ,geo.X
                ,geo.Y
                ,geo.STATE_FIPS
                ,geo.CNTY_FIPS
                ,geo.STCOFIPS
                ,geo.TRACT
                ,geo.STCOFIPS || geo.TRACT as TRACT_FIPS
                ,geo.BLKGRP
                ,geo.FIPS
FROM xdr_FORD_PAT        	                COH
LEFT JOIN I2B2.LZ_CLARITY_PATIENT          pat on coh.pat_id = pat.pat_id
left join XDR_FORD_PAT_GEO                  geo on coh.pat_id = geo.pat_id
JOIN patient                                pat2 ON coh.pat_id = pat2.pat_id
;



--------------------------------------------------------------------------------
-- STEP 8.2: Demographics Pull  - 
--
--------------------------------------------------------------------------------

SELECT DISTINCT coh.study_id
               ,case when (coh.FIRST_HIV_DX_DATE is not null and coh.FIRST_HIV_LAB_DATE is null)
                                                                                then TRUNC(MONTHS_BETWEEN(coh.FIRST_HIV_DX_DATE,coh.birth_date)/12)
                     when (coh.FIRST_HIV_DX_DATE is null and coh.FIRST_HIV_LAB_DATE is not null)
                                                                                then TRUNC(MONTHS_BETWEEN(coh.FIRST_HIV_LAB_DATE,coh.birth_date)/12)

                    when (coh.FIRST_HIV_DX_DATE is not null and coh.FIRST_HIV_LAB_DATE is not null)
                        and (coh.FIRST_HIV_DX_DATE <= coh.FIRST_HIV_LAB_DATE ) then TRUNC(MONTHS_BETWEEN(coh.FIRST_HIV_DX_DATE,coh.birth_date)/12)
                    when (coh.FIRST_HIV_DX_DATE is not null and coh.FIRST_HIV_LAB_DATE is not null)
                        and (coh.FIRST_HIV_DX_DATE > coh.FIRST_HIV_LAB_DATE ) then TRUNC(MONTHS_BETWEEN(coh.FIRST_HIV_LAB_DATE,coh.birth_date)/12)
                END age
               ,pat.sex                            	AS gender
--               ,coh.mapped_race_name               	AS race
--               ,coh.ethnic_group                   	AS ethnicity
               ,pat.ETHNIC_GROUP
               ,pat.MAPPED_RACE_NAME
               ,coh.MARITAL_STATUS
               ,coh.SEXUAL_ORIENTATION
               ,coh.PATIENT_STATUS                 	AS vital_status
--			   ,extract(year from PAT.LAST_ENC_DATE) AS year_last_encounter
			   ,extract(year from enc.last_enc) AS year_last_encounter
               ,prov.PROV_STUDY_ID
		    	     ,prov.UC_PROVIDER
    		  	  --  ,clepp.BENEFIT_PLAN_NAME
              --  ,fincls.name as FINANCIAL_CLASS
               ,coh.cohort_type
FROM xdr_FORD_PAT        	                COH
LEFT JOIN I2B2.LZ_CLARITY_PATIENT          pat on coh.pat_id = pat.pat_id
left join clarity.pat_acct_cvg 			    pac on pat.pat_id = pac.pat_id AND pac.account_active_yn = 'Y'
left join clarity.clarity_epp 			    clepp on pac.plan_id = clepp.benefit_plan_id
left join clarity.zc_financial_class 		fincls on pac.fin_class = fincls.financial_class
left join xdr_FORD_prov              prov on pat.CUR_PCP_PROV_ID = prov.PROVIDER_ID
left join (SELECT pat_id
              ,MAX(effective_date_dt) as last_enc
            FROM xdr_FORD_enc
            GROUP BY PAT_ID) enc on pat.pat_Id = enc.pat_id
;



--------------------------------------------------------------------------------
-- STEP 8.4: Encounters Pull 
--				i2b2.lz_enc_visit_types is an ad hoc table used at UCLA to match encounters to 
--				their PCORNET visit type equivalent (other sites will have to leverage 
--				their own resources to calculate the visit_type)
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
--			,enck.ENCOUNTER_ID
            ,enc2.ENCOUNTER_num
            ,enc.ENCOUNTER_TYPE
            ,enc.disposition
            ,enc.ed_disposition
            ,to_char(enc.EFFECTIVE_DATE_DT, 'mm/dd/yyyy')  as encounter_date
            ,enc.HOSP_ADMSN_TIME as admit_date
            ,enc.HOSP_DISCHRG_TIME as discharge_date
            ,CASE evt.visit_type
					WHEN 'ED' THEN 'Emergency Department only'
					WHEN 'EI' THEN 'Emergency to Inpatient'
					WHEN 'ES' THEN 'Still in ED'
					WHEN 'IP' THEN 'Inpatient'
					WHEN 'AV' THEN 'Ambulatory Visit'
					WHEN 'OT' THEN 'Other'
					WHEN 'UN' THEN 'Unknown'
					WHEN 'OA' THEN 'Other Ambulatory Visit'
					WHEN 'NI' THEN 'No Information'
					WHEN 'IS' THEN 'Non-Acute Institutional Stay' 
					WHEN 'EO' THEN 'Observation'
					WHEN 'IO' THEN 'Observation'
                  ELSE NULL
                END                                                             AS visit_type
            ,prov.PROV_STUDY_ID AS VISIT_PROV_STUDY_ID
            ,prov2.PROV_STUDY_ID AS PCP_PROV_STUDY_ID
            ,enc.SPECIALTY as department_SPECIALTY
			,enc.DEPARTMENT_NAME
--            ,dep.SPECIALTY as department_SPECIALTY
            ,enc.LOC_NAME   location
FROM xdr_FORD_enc              		enc
JOIN xdr_FORD_PAT               		pat on enc.pat_id = pat.pat_id
-- LEFT JOIN XDR_FORD_ENCKEY       		enck on enc.pat_enc_csn_id = enck.pat_enc_csn_id
left join xdr_FORD_prov            	prov on enc.VISIT_PROV_ID = prov.PROVIDER_ID
left join xdr_FORD_prov            	prov2 on enc.PCP_PROV_ID = prov2.PROVIDER_ID
--This is an ad hoc table used at UCLA to match encounters to their PCORNET visit type 
LEFT JOIN i2b2.lz_enc_visit_types               evt ON enc.pat_enc_csn_id = evt.pat_enc_csn_id
left join i2b2.lz_clarity_Dept                   dep on enc.department_id = dep.department_id
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on enc.pat_enc_csn_id = enc2.encounter_ide
;




--------------------------------------------------------------------------------
-- STEP 8.5: Diagnoses Pull 
--			Use the reference table provided to map the ICD code to its description (lz_dx_px_lookup)
--------------------------------------------------------------------------------
select DISTINCT pat.study_id
            ,enc2.ENCOUNTER_num
--            ,enck.encounter_id
            ,to_char(dx.CONTACT_DATE, 'mm/dd/yyyy') as diagnosis_date
            ,dx.icd_type
            ,dx.icd_code
            ,DXC.ICD_DESC
            ,dx.PRIMARY_SEC_FLAG
            ,dx.poa_flag
            ,dx.hsp_final_dx_flag
            ,dx.ADMIT_DX_FLAG
--            ,case when dx.icd_type = 9 then icd9.icd_desc
--                else icd10.icd_desc
--            end icd_description
from XDR_FORD_DX     				dx
JOIN XDR_FORD_pat    				pat on dx.pat_id = pat.pat_id
--JOIN XDR_FORD_ENCKEY    				enck on dx.pat_enc_csn_id = enck.pat_enc_csn_id
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on dx.pat_enc_csn_id = enc2.encounter_ide
LEFT JOIN I2B2.LZ_DX_PX_LOOKUP             DXC ON DX.ICD_CODE = DXC.CODE and dx.icd_type  = dxc.ICD_TYPE
--LEFT JOIN XDR_FORD_DX_LOOKUP        	icd9  ON dx.icd_code = icd9.code
--                                              AND icd9.icd_type = 9
--LEFT JOIN XDR_FORD_DX_LOOKUP        	icd10  ON dx.icd_code = icd10.code
--                                              AND icd10.icd_type = 10
;


--------------------------------------------------------------------------------
-- STEP 8.6: Procedures Pull 
--------------------------------------------------------------------------------
select  DISTINCT pat.study_id
                ,enc2.ENCOUNTER_NUM
--              ,enck.encounter_id
               ,TO_CHAR(PRC.PROC_DATE, 'MM/DD/YYYY') AS PROC_DATE
               ,PRC.CODE_TYPE
               
--               ,prc.ICD_CODE_SET as code_type
               ,prc.PROC_CODE as procedure_code
               ,prc.PROC_NAME as PROCEDURE_NAME
               ,prov.PROV_STUDY_ID
               
from xdr_FORD_prc     		prc
JOIN XDR_FORD_pat           	pat  on prc.pat_id = pat.pat_id
-- JOIN XDR_FORD_ENCKEY        	enck on prc.pat_enc_csn_id = enck.pat_enc_csn_id
left join xdr_FORD_prov            	prov on prc.PROC_PERF_PROV_ID = prov.PROVIDER_ID
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on prc.pat_enc_csn_id = enc2.encounter_ide
;

--------------------------------------------------------------------------------
-- STEP 8.7: Flowsheets Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
--               ,enck.encounter_id
               ,enc2.ENCOUNTER_NUM
               ,flo.recorded_time
               ,flo.measure_name      AS vital_sign_type
               ,flo.measure_value     AS vital_sign_value
FROM xdr_ford_flo          flo
JOIN XDR_ford_pat          pat  on flo.pat_id = pat.pat_id
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on flo.pat_enc_csn_id = enc2.encounter_ide
;



--------------------------------------------------------------------------------
-- STEP 8.8: Lab Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
              --  ,enck.encounter_id
               ,enc2.encounter_num
               ,lab.proc_id                
               ,lab.description           
               ,lab.component_id       
               ,lab.component_name                     
               ,lab.order_time
               ,lab.RESULT_time
               ,lab.ord_value               AS results
               ,lab.reference_unit          
               ,lab.reference_low 
               ,lab.reference_high
               ,lab2.BIP_LOINC_MAPPING as loinc_code
FROM xdr_FORD_lab          			lab 
JOIN XDR_FORD_pat          			pat  on lab.pat_id = pat.pat_id
-- JOIN XDR_FORD_ENCKEY       			enck on lab.pat_enc_csn_id = enck.pat_enc_csn_id
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on lab.pat_enc_csn_id = enc2.encounter_ide
left join i2b2.lz_Clarity_labs lab2 on lab.order_Proc_id = lab2. order_proc_id
                                                    and lab.proc_id = lab2.proc_id
                                                    and lab.component_id = lab2.component_id
;


--------------------------------------------------------------------------------
-- STEP 8.9: medications Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
--               ,enck.encounter_id
               ,enc2.ENCOUNTER_NUM
               ,med.order_med_id
               ,nvl(med.taken_time, med.ORDER_INST)   AS taken_time_order_date
--               ,med.ORDER_INST
               ,med.ORDER_START_TIME
               ,med.ORDER_END_TIME
               ,med.medication_name
               ,med.generic_name
               ,med.sig
               ,med.HV_DISCRETE_DOSE            AS dose
               ,MED.REFILLS
               ,MED.QUANTITY
               ,med.DOSE_UNIT
               ,med.FREQ_NAME                   AS FREQUENCY        
               ,med.pharm_class
               ,med.pharm_subclass
--               ,MED.ORDER_STATUS
--               ,MED.ORDER_CLASS
               
FROM xdr_FORD_med          			med
JOIN XDR_FORD_pat          			pat  on med.pat_id = pat.pat_id
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on med.pat_enc_csn_id = enc2.encounter_ide
--JOIN XDR_FORD_ENCKEY       			enck on med.pat_enc_csn_id = enck.pat_enc_csn_id
--WHERE nvl(med.taken_time, med.ORDER_INST) BETWEEN '01/01/2006' AND '02/05/2018'
;


--------------------------------------------------------------------------------
-- STEP 8.12: Social History Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               ,soc.SEXUALLY_ACTIVE
               ,soc.FEMALE_PARTNER_YN
                ,soc.MALE_PARTNER_YN
                ,soc.iv_drug_user_yn 
               ,soc.illicit_drug_freq  
               ,soc.ILLICIT_DRUG_CMT
               
--                ,soc.SEXUALLY_ACTIVE
--                ,soc.SPERMICIDE_YN
--                ,soc.SPONGE_YN
  FROM xdr_FORD_soc          soc
  JOIN xdr_FORD_pat               pat on soc.pat_id = pat.pat_id
  ;

  --------------------------------------------------------------------------------
-- STEP 8.13: Providers table
--------------------------------------------------------------------------------
SELECT DISTINCT prov.PROV_STUDY_ID
		,pr.provider_name
        ,prov.PRIMARY_SPECIALTY
		,prov.PROVIDER_TYPE
		,prov.UC_PROVIDER
		,prov.ACTIVE_PROVIDERS
        ,prov.HIV_PROVIDER
FROM xdr_FORD_prov PROV     
left join v_cube_d_provider pr on prov.provider_id = pr.provider_id
order by 1
;
SELECT * FROM (
SELECT PROV.PROV_STUDY_ID
,'ETHNIC_GROUP' AS DEMO_TYPE
, PAT.ETHNIC_GROUP as demo_sub_type
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
GROUP BY PROV.PROV_STUDY_ID, PAT.ETHNIC_GROUP

UNION

SELECT PROV.PROV_STUDY_ID
,'MAPPED_RACE_NAME' AS DEMO_TYPE
, PAT.MAPPED_RACE_NAME
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
GROUP BY PROV.PROV_STUDY_ID, PAT.MAPPED_RACE_NAME

UNION

SELECT PROV.PROV_STUDY_ID
,'SEX' AS DEMO_TYPE
, PAT.SEX
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
GROUP BY PROV.PROV_STUDY_ID, PAT.SEX

UNION

SELECT PROV.PROV_STUDY_ID
,'MARITAL_STATUS' AS DEMO_TYPE
, PAT.MARITAL_STATUS
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
GROUP BY PROV.PROV_STUDY_ID, PAT.MARITAL_STATUS


UNION

SELECT PROV.PROV_STUDY_ID
,'GENDER_IDENT' AS DEMO_TYPE
, PAT.GENDER_IDENT
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
GROUP BY PROV.PROV_STUDY_ID, PAT.GENDER_IDENT


UNION

SELECT PROV.PROV_STUDY_ID
,'SEXUAL_ORIENT' AS DEMO_TYPE
, PAT.SEXUAL_ORIENT
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
GROUP BY PROV.PROV_STUDY_ID, PAT.SEXUAL_ORIENT

UNION

SELECT PROV.PROV_STUDY_ID
,'INCOME_CLASSIFICATION' AS DEMO_TYPE
,GEO.INCOME_CD
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
LEFT JOIN BIP_PAT_GEOCODE GEO ON PAT.PAT_ID = GEO.PAT_ID
GROUP BY PROV.PROV_STUDY_ID,GEO.INCOME_CD


UNION

SELECT PROV.PROV_STUDY_ID
,'EDUCATION_CLASSIFICATION' AS DEMO_TYPE
, GEO.EDUCATION_CD
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
LEFT JOIN BIP_PAT_GEOCODE GEO ON PAT.PAT_ID = GEO.PAT_ID
GROUP BY PROV.PROV_STUDY_ID,GEO.EDUCATION_CD
)ORDER BY 1,2,3;

--PROVIDER PANEL AGE MEAN
SELECT X.*
,coh.HIV_PATIENTS
FROM 
(SELECT 
PROV.PROV_STUDY_ID
,count(DISTINCT PAT_ID) AS TOTAL_PATIENTS
,AVG(MONTHS_BETWEEN(SYSDATE,PAT.BIRTH_DATE)/12) AVG_AGE
,MEDIAN(MONTHS_BETWEEN(SYSDATE,PAT.BIRTH_DATE)/12) MEDIAN_AGE
,STATS_MODE(MONTHS_BETWEEN(SYSDATE,PAT.BIRTH_DATE)/12) MODE_AGE
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
GROUP BY PROV.PROV_STUDY_ID) X
left join (
SELECT 
PROV.PROV_STUDY_ID
,COUNT(*) AS HIV_PATIENTS
FROM xdr_ford_pat PAT
JOIN xdr_FORD_prov PROV ON PAT.CUR_PCP_PROV_ID = PROV.PROVIDER_ID
GROUP BY PROV.PROV_STUDY_ID)          coh on x.PROV_STUDY_ID = coh.PROV_STUDY_ID
;

--PROVIDER PANEL GEOCODES
SELECT 
PROV.PROV_STUDY_ID
,GEO.STATE_FIPS
,GEO.CNTY_FIPS
,geo.STCOFIPS || geo.TRACT as TRACT_FIPS
,GEO.FIPS
,COUNT(DISTINCT PAT.PAT_ID) PAT_COUNT
FROM xdr_FORD_prov PROV
JOIN I2B2.LZ_CLARITY_PATIENT PAT ON PROV.PROVIDER_ID = PAT.CUR_PCP_PROV_ID
LEFT JOIN BIP_PAT_GEOCODE GEO ON PAT.PAT_ID = GEO.PAT_ID
GROUP BY PROV.PROV_STUDY_ID,GEO.STATE_FIPS,GEO.CNTY_FIPS,GEO.TRACT_FIPS,geo.STCOFIPS || geo.TRACT,GEO.FIPS
;



--------------------------------------------------------------------------------
--  STEP 8.14: Problem List Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               ,enc2.encounter_num
               ,pl.encounter_date
               ,pl.problem_list_id
               --,pl.dx_id               commented out on 6/9/17 to avoid confusion
               ,pl.prob_desc 
               ,pl.noted_date  
               ,pl.update_date               AS date_of_entry            
               ,pl.resolved_date 
               ,pl.problem_status
               ,pl.problem_cmt             
               ,pl.priority
               ,pl.hospital_problem
               ,principal_yn
               ,prov.PROVIDER_ID
  FROM xdr_FORD_pl           pl
  JOIN xdr_FORD_pat               pat on pl.pat_id = pat.pat_id
  left join I2B2.BIP_ENCOUNTER_LINK                enc2 on pl.pat_enc_csn_id = enc2.encounter_ide
  left join xdr_FORD_prov            	prov on pl.prov_id = prov.PROVIDER_ID;


--------------------------------------------------------------------------------
-- STEP 8.15: Problem List Diagnosis Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
--               ,enck.encounter_id
               ,enc2.ENCOUNTER_NUM
               ,pdx.diagnosis_source
               ,pdx.icd_type
               ,pdx.icd_code
--               ,pdx.icd_desc
               ,DX.ICD_DESC
               ,to_char(pdx.diagnosis_date) as diagnosis_date
               ,to_char(pdx.RESOLVED_DATE) as RESOLVED_DATE
               ,pdx.priority
               ,pdx.problem_status
               ,pdx.primary_dx_yn
  FROM xdr_FORD_pldx              pdx
  JOIN xdr_FORD_pat               pat on pdx.pat_id = pat.pat_id
--  LEFT JOIN XDR_FORD_ENCKEY       enck on pdx.pat_enc_csn_id = enck.pat_enc_csn_id
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on PDX.pat_enc_csn_id = enc2.encounter_ide
LEFT JOIN I2B2.LZ_DX_PX_LOOKUP          DX ON pdx.ICD_CODE = DX.CODE and pdx.icd_type = dx.icd_type
  ;
  
-------------------------------------------------------------------------------
-- STEP 8.15: ADT Pull
--------------------------------------------------------------------------------                    
SELECT DISTINCT pat.study_id
               ,ENC2.ENCOUNTER_NUM
               ,adt.event_type
               ,adt.time_in
               ,adt.time_out
               ,adt.department_id
               ,adt.department_name
               ,adt.department_specialty
               ,adt.loc_name                      AS location
  FROM XDR_FORD_ADT             adt
  JOIN xdr_ford_pat                  pat ON adt.pat_id = pat.pat_id
  left join I2B2.BIP_ENCOUNTER_LINK                enc2 on ADT.pat_enc_csn_id = enc2.encounter_ide
  ORDER BY  study_id, ENC2.ENCOUNTER_NUM, time_in
;

--------------------------------------------------------------------------------
-- STEP 8.16: Family History Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT fam.study_id
--                ,enck.encounter_id
                ,enc2.ENCOUNTER_NUM
                ,fam.line
                ,fam.medical_hx
                ,fam.relation
FROM xdr_FORD_fam        fam
JOIN XDR_FORD_pat    	pat on fam.pat_id = pat.pat_id
left join I2B2.BIP_ENCOUNTER_LINK                enc2 on FAM.pat_enc_csn_id = enc2.encounter_ide
--JOIN XDR_FORD_ENCKEY     enck on fam.pat_enc_csn_id = enck.pat_enc_csn_id
;


--------------------------------------------------------------------------------
-- STEP 8.16: Microbiology  Pull 
--------------------------------------------------------------------------------
SELECT  DISTINCT COH.study_id
                ,enc2.ENCOUNTER_NUM
                ,prc.order_proc_id           AS order_proc_id
                ,prc.order_time              AS order_time
                ,prc.result_time             AS result_time
                ,prc.proc_code               AS procedure_code
                ,prc.description             AS procedure_name
                ,xps.name                    AS specimen_source
                ,prc.specimen_type           AS specimen_type
                ,prc.line                    AS line
                ,prc.ord_value               AS results
                ,prc.component_name          AS component
                ,prc.component_comment       AS component_comment
                ,org.name                    AS organism_name
                ,mic.micro_sorting
                ,mic.micro_line_comment      AS line_comment
                ,mic.micro_results_cmt       AS results_cmt
  FROM xdr_FORD_opr			prc
  JOIN XDR_FORD_PAT           COH ON PRC.pat_id = COH.pat_id
  JOIN xdr_FORD_mic    		mic ON prc.order_proc_id = mic.micro_order_id and prc.line = mic.micro_line   
  LEFT JOIN clarity.zc_specimen_source  xps ON prc.specimen_source_c = xps.specimen_source_c
  LEFT JOIN clarity.clarity_organism	org ON prc.lrr_based_organ_id = org.organism_id
  left join I2B2.BIP_ENCOUNTER_LINK                enc2 on prc.pat_enc_csn_id = enc2.encounter_ide
  ORDER BY study_id, order_proc_id, mic.micro_sorting ,prc.line, mic.micro_line_comment;

--------------------------------------------------------------------------------
-- STEP 8.16: Microbiology SUSCEPTIBILITY/SENSITIVITY  Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT coh.study_id
               ,mic2.order_proc_id       
               ,mic2.organism_name      
               ,mic2.susceptibility
               ,mic2.sensitivity
               ,mic2.antibiotic
  FROM xdr_FORD_mic2 	      mic2
  JOIN XDR_FORD_opr           MIC ON mic2.ORDER_PROC_ID = mic.ORDER_PROC_ID
  JOIN XDR_FORD_PAT           COH ON mic.pat_id = COH.pat_id
  -- left join I2B2.BIP_ENCOUNTER_LINK                enc2 on mic.pat_enc_csn_id = enc2.encounter_ide
  ORDER BY study_id, order_proc_id;

--------------------------------------------------------------------------------
-- STEP 8.17: Pathology Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT coh.study_id
               ,enc2.ENCOUNTER_NUM
               ,prc.acc_num                 AS accession_num
               ,prc.order_proc_id           AS order_proc_id
               ,prc.order_time              AS order_time
               ,prc.result_time             AS result_time
               ,prc.proc_name               AS procedure_name
               ,xps.NAME                    AS specimen_source
               ,prc.specimen_type           AS specimen_type
               ,prc.line                    AS line
               ,prc.ord_value               AS results 
               ,prc.component_name          AS component
               ,prc.component_comment       AS component_comment
               ,pth.path_line_comment       AS line_comment
               ,pth.path_results_cmt        AS results_cmt
  FROM xdr_FORD_opr                              prc
  JOIN XDR_FORD_PAT           COH ON prc.pat_id = COH.pat_id
  JOIN xdr_FORD_path                             pth on prc.order_proc_id = pth.path_order_id and prc.line = pth.path_line
  LEFT JOIN zc_specimen_source  xps ON prc.specimen_source_c = xps.specimen_source_c
  left join I2B2.BIP_ENCOUNTER_LINK                enc2 on prc.pat_enc_csn_id = enc2.encounter_ide
  ORDER BY study_id, prc.order_proc_id, prc.line, pth.path_line_comment;

--------------------------------------------------------------------------------
-- STEP 8.18: Imaging Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT coh.study_id
               ,enc2.ENCOUNTER_NUM
               ,img.order_proc_id           AS order_proc_id
               ,img.order_time              AS order_time
               ,img.result_time             AS result_time
               ,img.proc_id                 AS proc_id
               ,img.proc_code               AS proc_code
               ,img.proc_name               AS procedure_name
               ,img.acc_num                 AS accession_num
  FROM xdr_FORD_opr                         prc
  JOIN XDR_FORD_PAT                         coh on prc.pat_id = coh.pat_id
  join xdr_FORD_img                         img ON prc.order_proc_id = img.order_proc_id
  left join I2B2.BIP_ENCOUNTER_LINK         enc2 on prc.pat_enc_csn_id = enc2.encounter_ide
  ORDER BY study_id, order_proc_id;

--------------------------------------------------------------------------------
-- STEP 8.19: Narratives Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT coh.study_id
               ,nar.order_proc_id           AS order_proc_id
               ,nar.narr_line               AS line
               ,nar.narr_narrative          AS narrative
  FROM xdr_FORD_imgnar       nar
  JOIN XDR_FORD_PAT          coh on nar.pat_id = coh.pat_id
  ORDER BY study_id, order_proc_id, narr_line;

--------------------------------------------------------------------------------
-- STEP 8.20: Impressions Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT coh.study_id
               ,imp.order_proc_id           AS order_proc_id
               ,imp.impr_line               AS line
               ,imp.impr_impression         AS impression
  FROM xdr_FORD_imgimp       imp 
  JOIN XDR_FORD_PAT          coh on imp.pat_id = coh.pat_id
  ORDER BY study_id, order_proc_id, impr_line;

--------------------------------------------------------------------------------
-- STEP 8.21: Appointments Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT coh.study_id
               ,enc2.ENCOUNTER_NUM
               ,appt.appt_dttm
               ,appt.appt_made_dttm
               ,appt.department_id
               ,appt.department_name
               ,appt.department_specialty
               ,appt.center_name
               ,appt.loc_name
               ,appt.prov_name_wid
               ,appt.referring_prov_name_wid
               ,appt.visit_type         AS procedure_name
               ,appt.appt_confirmation_status   
               ,appt.appt_status
               ,prov.PROV_STUDY_ID as appointment_PROV_STUDY_ID
               ,prov2.PROV_STUDY_ID as referring_PROV_STUDY_ID
  FROM XDR_FORD_APPT appt
  JOIN XDR_FORD_PAT                         coh on appt.pat_id = coh.pat_id
  left join I2B2.BIP_ENCOUNTER_LINK         enc2 on appt.pat_enc_csn_id = enc2.encounter_ide
  left join xdr_FORD_prov            	      prov on appt.PROV_ID = prov.PROVIDER_ID
  left join xdr_FORD_prov            	      prov2 on appt.referring_prov_id = prov2.PROVIDER_ID
  ORDER BY study_id, appt_dttm;

--------------------------------------------------------------------------------
-- STEP 8.23: Extract patient coverage
--------------------------------------------------------------------------------
SELECT pat.study_id
        -- ,cov.BENEFIT_PLAN_NAME
        -- ,cov.payor_name
        ,cov.financial_class_name
        ,cov.MEM_EFF_FROM_DATE
        ,cov.MEM_EFF_TO_DATE
FROM xdr_ford_pat pat 
join i2b2.lz_clarity_coverage_pat cov on pat.pat_id = cov.pat_id
order by pat.study_id,cov.MEM_EFF_FROM_DATE;

--------------------------------------------------------------------------------
-- STEP 8.23: Extract encounter coverage
--------------------------------------------------------------------------------
  SELECT pat.study_id
--        ,enc.pat_enc_csn_id
        ,lnk.encounter_num
        ,cov.line
        -- ,cov.BENEFIT_PLAN_NAME
        -- ,cov.payor_name
        ,cov.financial_class_name
        ,cov.MEM_EFF_FROM_DATE
        ,cov.MEM_EFF_TO_DATE
FROM xdr_ford_enc enc
join pat_enc enc2 on enc.pat_enc_csn_id = enc2.pat_enc_csn_id
join xdr_ford_pat pat on enc.pat_id = pat.pat_id 
join i2b2.lz_clarity_coverage_enc cov on enc2.HSP_ACCOUNT_ID = cov.HSP_ACCOUNT_ID
left join I2B2.BIP_ENCOUNTER_LINK  lnk on enc.pat_enc_csn_id = lnk.encounter_ide
where 
-- cov.BENEFIT_PLAN_NAME is not null
        -- or cov.payor_name is not null
        -- or 
        cov.financial_class_name is not null
order by pat.study_id,lnk.encounter_num,cov.line
;  
-------------------------------------------------------------------------------
-- STEP 8.22: Data counts 
--------------------------------------------------------------------------------
SELECT * FROM XDR_FORD_COUNTS;

-- *******************************************************************************************************
-- STEP 99
--   Create the HIPAA file
-- *******************************************************************************************************
SELECT DISTINCT pat.pat_mrn_id
               ,pat.pat_last_name
               ,pat.pat_first_name
  FROM XDR_ford_pat 		          p
  JOIN patient	pat ON p.pat_id = pat.pat_id
;
