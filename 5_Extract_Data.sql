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
-- STEP 8.2: Demographics Pull  - 
--
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id,
               CASE WHEN ROUND(MONTHS_BETWEEN('08/01/2018',pat.birth_date)/12) >= 90 THEN 90
                    ELSE   ROUND(MONTHS_BETWEEN('08/01/2018',pat.birth_date)/12)      END age,
               pat.sex                            	AS gender,
               pat.mapped_race_name               	AS race,
               pat.ethnic_group                   	AS ethnicity,
               pat.PATIENT_STATUS                 	AS vital_status,
			   extract(year from PAT.LAST_ENC_DATE) AS year_last_encounter,     
			   extract(year from PAT.FIRST_ENC_DATE) AS year_first_encounter,     
               prov.PROV_STUDY_ID,
			   prov.UC_PROVIDER,
			   clepp.BENEFIT_PLAN_NAME,
               fincls.name as FINANCIAL_CLASS,
FROM xdr_FORD_COH        	                pat
left join clarity.pat_acct_cvg 			    pac on pat.pat_id = pac.pat_id AND pac.account_active_yn = 'Y'
left join clarity.clarity_epp 			    clepp on pac.plan_id = clepp.benefit_plan_id
left join clarity.zc_financial_class 		fincls on pac.fin_class = fincls.financial_class
left join xdr_Wherry_preg_prov              prov on pat.CUR_PCP_PROV_ID = prov.PROVIDER_ID
WHERE pat.mom_child_mc = 'M'
;



--------------------------------------------------------------------------------
-- STEP 8.4: Encounters Pull 
--				i2b2.lz_enc_visit_types is an ad hoc table used at UCLA to match encounters to 
--				their PCORNET visit type equivalent (other sites will have to leverage 
--				their own resources to calculate the visit_type)
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
			,enck.ENCOUNTER_ID
            ,enc.ENCOUNTER_TYPE
            ,enc.disposition
            ,enc.ed_disposition
            ,enc.EFFECTIVE_DATE_DT
            ,enc.HOSP_ADMSN_TIME
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
            ,prov.PROV_STUDY_ID
            ,enc.SPECIALTY
			,enc.DEPARTMENT_NAME
            ,enc.LOC_NAME   location
FROM xdr_FORD_enc              		enc
JOIN xdr_FORD_coh               		pat on enc.pat_id = pat.pat_id
--This is an ad hoc table used at UCLA to match encounters to their PCORNET visit type 
LEFT JOIN i2b2.lz_enc_visit_types               evt ON enc.pat_enc_csn_id = evt.pat_enc_csn_id
LEFT JOIN XDR_FORD_ENCKEY       		enck on enc.pat_enc_csn_id = enck.pat_enc_csn_id
left join xdr_FORD_prov            	prov on enc.VISIT_PROV_ID = prov.PROVIDER_ID
;


--------------------------------------------------------------------------------
-- STEP 8.5: Diagnoses Pull 
--			Use the reference table provided to map the ICD code to its description (lz_dx_px_lookup)
--------------------------------------------------------------------------------
select DISTINCT pat.study_id
            ,enck.encounter_id
            ,dx.CONTACT_DATE
            ,dx.icd_type
            ,dx.icd_code
            ,dx.PRIMARY_SEC_FLAG
            ,dx.poa_flag
            ,dx.hsp_final_dx_flag
            ,dx.ADMIT_DX_FLAG
--            ,case when dx.icd_type = 9 then icd9.icd_desc
--                else icd10.icd_desc
--            end icd_description
from XDR_FORD_DX     				dx
JOIN XDR_FORD_pat    				pat on dx.pat_id = pat.pat_id
JOIN XDR_FORD_ENCKEY    				enck on dx.pat_enc_csn_id = enck.pat_enc_csn_id
LEFT JOIN xdr_FORD_pat 	    		mom  on pat.mom_pat_id = mom.pat_id AND pat.pat_id = 'C' AND mom.mom_child_mc = 'M'
--LEFT JOIN XDR_FORD_DX_LOOKUP        	icd9  ON dx.icd_code = icd9.code
--                                              AND icd9.icd_type = 9
--LEFT JOIN XDR_FORD_DX_LOOKUP        	icd10  ON dx.icd_code = icd10.code
--                                              AND icd10.icd_type = 10
;


--------------------------------------------------------------------------------
-- STEP 8.6: Procedures Pull 
--------------------------------------------------------------------------------
select  DISTINCT pat.study_id
               ,enck.encounter_id
               ,PRC.PROC_DATE
               ,prc.ICD_CODE_SET as code_type
               ,prc.PX_CODE as procedure_code
               ,prc.PROCEDURE_NAME
from xdr_FORD_prc     		prc
JOIN XDR_FORD_pat           	pat  on prc.pat_id = pat.pat_id
JOIN XDR_FORD_ENCKEY        	enck on prc.pat_enc_csn_id = enck.pat_enc_csn_id
;


--------------------------------------------------------------------------------
-- STEP 8.7: Flowsheets Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT flo.study_id
               ,enck.encounter_id
			   ,flo.recorded_time
               ,flo.measure_name      AS vital_sign_type
               ,flo.measure_value     AS vital_sign_value
FROM xdr_WHERRY_preg_flo          flo
JOIN XDR_WHERRY_preg_pat          pat  on flo.pat_id = pat.pat_id
JOIN XDR_WHERRY_preg_ENCKEY       enck on flo.pat_enc_csn_id = enck.pat_enc_csn_id
;



--------------------------------------------------------------------------------
-- STEP 8.8: Lab Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT lab.study_id
               ,enck.encounter_id
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
FROM xdr_FORD_lab          			lab 
JOIN XDR_FORD_pat          			pat  on lab.pat_id = pat.pat_id
JOIN XDR_FORD_ENCKEY       			enck on lab.pat_enc_csn_id = enck.pat_enc_csn_id
LEFT JOIN clarity.clarity_component                 cc ON lab.component_id = cc.component_id
;



--------------------------------------------------------------------------------
-- STEP 8.9: medications Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT med.study_id
               ,enck.encounter_id
               ,med.order_med_id
               ,nvl(med.taken_time, med.ORDER_INST)   AS taken_time_order_date
               ,med.ORDER_INST
               ,med.ORDER_START_TIME
               ,med.ORDER_END_TIME
               ,med.medication_name
               ,med.generic_name
               ,med.sig
               ,med.HV_DISCRETE_DOSE            AS dose
               ,med.DOSE_UNIT
               ,med.FREQ_NAME                   AS FREQUENCY        
               ,med.pharm_class
               ,med.pharm_subclass
               ,MED.ORDER_STATUS
               ,MED.ORDER_CLASS
               ,med.mom_child_mc
FROM xdr_FORD_med          			med
JOIN XDR_FORD_pat          			pat  on med.pat_id = pat.pat_id
JOIN XDR_FORD_ENCKEY       			enck on med.pat_enc_csn_id = enck.pat_enc_csn_id
--WHERE nvl(med.taken_time, med.ORDER_INST) BETWEEN '01/01/2006' AND '02/05/2018'
;



--------------------------------------------------------------------------------
-- STEP 8.12: Social History Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT soc.study_id
               ,soc.iv_drug_user_yn 
               ,soc.illicit_drug_freq  
               ,soc.FEMALE_PARTNER_YN
                ,soc.MALE_PARTNER_YN
                ,soc.SEXUALLY_ACTIVE
                ,soc.SPERMICIDE_YN
                ,soc.SPONGE_YN
  FROM xdr_FORD_soc          soc
  JOIN XDR_FORD_pat    	    pat on soc.pat_id = pat.pat_id
  ;


  --------------------------------------------------------------------------------
-- STEP 8.13: Providers table
--------------------------------------------------------------------------------
SELECT DISTINCT PROV_STUDY_ID
		,PRIMARY_SPECIALTY
		,PROVIDER_TYPE
		,UC_PROVIDER
		,ACTIVE_PROVIDERS
FROM xdr_FORD_prov
;


--------------------------------------------------------------------------------
-- STEP 8.14: Problem List Diagnosis Pull 
--------------------------------------------------------------------------------
SELECT DISTINCT pat.study_id
               ,enck.encounter_id
               ,pdx.diagnosis_source
               ,pdx.icd_type
               ,pdx.icd_code
               ,pdx.icd_desc
               ,pdx.diagnosis_date
               ,pdx.RESOLVED_DATE
               ,pdx.priority
               ,pdx.problem_status
               ,pdx.primary_dx_yn
			   ,pat.mom_child_mc
  FROM xdr_FORD_pldx              pdx
  JOIN xdr_FORD_pat               pat on pdx.pat_id = pat.pat_id
  LEFT JOIN XDR_FORD_ENCKEY       enck on pdx.pat_enc_csn_id = enck.pat_enc_csn_id
  ;
  

  --------------------------------------------------------------------------------
-- STEP 8.16: Data counts 
--------------------------------------------------------------------------------
SELECT * FROM XDR_FORD_COUNTS;
