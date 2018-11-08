/********************************************************************************************************

    Step 3
        Main datapull

********************************************************************************************************/

------------------------------------------------------
--  Step 3.1:   Pull encounters information
--
------------------------------------------------------
drop  table XDR_FORD_ENC purge;
create table xdr_ford_ENC as 
SELECT DISTINCT e.pat_id, 
            e.pat_enc_csn_id, 
            e.hsp_account_id, 
            e.inpatient_data_id, 
            e.ip_episode_id,
            e.effective_date_dt,
            e.hosp_admsn_time, 
            e.hosp_dischrg_time,
            nvl(e.visit_fc, -999) prim_fc, 
            nvl(fc.fin_class_title, 'Unknown') financial_class,
            nvl(e.enc_type_c,'-999') enc_type_c, 
            nvl(enctype.name, 'Unknown') encounter_type,
            e.department_id,
            e.visit_prov_id,
            e.appt_status_c, 
            e.pcp_prov_id,
            hsp.disch_disp_c, 
            dd.name disposition, 
            hsp.ed_disposition_c, 
            edd.name ed_disposition,
            dep.department_name,
            dep.specialty,
            loc.loc_name,
            e.APPT_PRC_ID,
            prc.PROC_CAT,
            prc.prc_name
        FROM clarity.pat_enc e
        JOIN XDR_ford_coh 			coh on e.pat_id = coh.pat_id 
        LEFT JOIN clarity.clarity_fc 		fc 		ON e.visit_fc = fc.financial_class
        LEFT JOIN clarity.ZC_DISP_ENC_TYPE 	enctype ON e.enc_type_c = enctype.disp_enc_type_c
        LEFT JOIN clarity.pat_enc_hsp       hsp 	ON e.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
        LEFT JOIN clarity.zc_disch_disp     dd  	ON hsp.disch_disp_c = dd.disch_disp_c
        LEFT JOIN clarity.zc_ed_disposition edd 	ON hsp.ed_disposition_c = edd.ed_disposition_c
        LEFT JOIN clarity.clarity_dep       dep 	ON e.department_id = dep.department_id
        left join clarity.clarity_loc       loc     ON dep.rev_loc_id = loc.loc_id
        left join clarity.clarity_PRC       prc     ON e.APPT_PRC_ID = prc.PRC_ID
        WHERE e.enc_type_c not in (2532, 2534, 40, 2514, 2505, 2506, 2512, 2507)
			AND e.effective_date_dt BETWEEN '03/02/2013' AND '02/28/2018';
--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_ENC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Encounter counts' AS DESCRIPTION
FROM XDR_FORD_ENC;
COMMIT;            


--PRIMARY CARE ENCOUNTERS BASED ON ENCOUNTER TYPE AND HIV PROVIDER
SELECT ENC.*
FROM XDR_FORD_ENC               ENC
JOIN XDR_FORD_PROVDRV           PROV ON ENC.VISIT_PROV_ID = PROV.PROVIDER_ID
WHERE ENC.ENCOUNTER_TYPE in (
'Evaluation'
,'Follow-Up'
,'Office Visit'
,'Treatment'
,'Canceled'
,'Non-UCLA Hosp and Clinic Visits'
,'Lab Visit'
,'Telephone'
,'myUCLAhealth Messaging'
)
;--1984 patients 26711 encounters
--------------------------------------------------------------------------------
--	STEP 3.3: Create Diagnoses table
--------------------------------------------------------------------------------
	--------------------------------------------------------------------------------
	--	STEP 3.3.1: Create destination table
	--------------------------------------------------------------------------------
DROP TABLE XDR_FORD_DX PURGE;
 CREATE TABLE XDR_FORD_DX
   (	"PAT_ID" VARCHAR2(18 BYTE), 
	"PAT_ENC_CSN_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"CONTACT_DATE" DATE, 
	"ICD_CODE" VARCHAR2(254 BYTE), 
	"ICD_TYPE" NUMBER, 
	"PRIMARY_SEC_FLAG" CHAR(1 BYTE), 
	"ADMIT_DX_FLAG" CHAR(1 BYTE), 
	"POA_FLAG" VARCHAR2(50 BYTE), 
	"HSP_FINAL_DX_FLAG" CHAR(1 BYTE)
   );
   
	--------------------------------------------------------------------------------
	--	STEP 3.3.2: Initial load from pat_enc_dx table
	--------------------------------------------------------------------------------
insert into XDR_FORD_DX
SELECT coh.pat_id, 
	   dx.pat_enc_csn_id, 
	   dx.contact_date, 
	   edg.code as icd_code,
     9 as icd_type,
     'P' as primary_sec_flag,
     null as admit_dx_flag,
     null as poa_flag,
     null as hsp_final_dx_flag
FROM XDR_FORD_COH coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd9 edg ON dx.dx_id = edg.dx_id
WHERE dx.primary_dx_yn = 'Y' 
  and edg.code not like 'IMO0%'
  AND dx.contact_date BETWEEN '03/02/2013' AND '02/28/2018'
UNION
SELECT coh.pat_id, 
	   dx.pat_enc_csn_id, 
	   dx.contact_date, 
	   edg.code as icd_code,
	   10 as icd_type,
     'P' as primary_sec_flag,
     null as admit_dx_flag,
     null as poa_flag,
     null as hsp_final_dx_flag
FROM XDR_FORD_PAT coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd10 edg ON dx.dx_id = edg.dx_id
WHERE dx.primary_dx_yn = 'Y' 
  and edg.code not like 'IMO0%'
  AND dx.contact_date BETWEEN '03/02/2013' AND '02/28/2018'
;
commit;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Diagnoses initial load from pat_enc_dx table' as DESCRIPTION
FROM XDR_FORD_DX;
COMMIT;    

	--------------------------------------------------------------------------------
	--	STEP 3.3.3: Now Load Secondary Dx if they don't already exist.
	--------------------------------------------------------------------------------
merge into XDR_FORD_DX lcd
using
(SELECT coh.pat_id, 
	   dx.pat_enc_csn_id, 
	   dx.contact_date, 
	   edg.code as icd_code,
     9 as icd_type
FROM XDR_FORD_PAT coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd9 edg ON dx.dx_id = edg.dx_id
WHERE (dx.primary_dx_yn is null or  dx.primary_dx_yn != 'Y')
    and edg.code not like 'IMO0%'
	AND dx.contact_date BETWEEN '03/02/2013' AND '02/28/2018'
UNION
SELECT coh.pat_id, 
	   dx.pat_enc_csn_id,
	   dx.contact_date, 
	   edg.code as icd_code,
	   10 as icd_type
FROM XDR_FORD_PAT coh
JOIN clarity.pat_enc_dx dx ON coh.pat_id = dx.pat_id
JOIN clarity.edg_current_icd10 edg ON dx.dx_id = edg.dx_id
WHERE (dx.primary_dx_yn is null or  dx.primary_dx_yn != 'Y')
    and edg.code not like 'IMO0%'
	AND dx.contact_date BETWEEN '03/02/2013' AND '02/28/2018'
) adm 
on (lcd.pat_id = adm.pat_id
  and lcd.pat_enc_csn_id = adm.pat_enc_csn_id
  and lcd.contact_date = adm.contact_date
  and lcd.icd_code = adm.icd_code)
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (adm.pat_id, adm.pat_enc_csn_id, adm.contact_date, adm.icd_code, adm.icd_type, 'S', null, null, null)
;
commit;


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Load Secondary Dx if they dont already exist' as DESCRIPTION	
FROM XDR_FORD_DX;
COMMIT;    


	--------------------------------------------------------------------------------
	--	STEP 3.3.4: Now Load Admit Dx
	--------------------------------------------------------------------------------
MERGE INTO XDR_FORD_DX lcd
using
  (select
		  coh.pat_id,
		  hd.pat_enc_csn_id,
		  trunc(dt.calendar_dt) as contact_date,
		  edg.code as icd_code,
		  9 as icd_type
    FROM XDR_FORD_COH                   coh
    join clarity.hsp_admit_diag         hd on coh.pat_id = hd.pat_id
    JOIN clarity.edg_current_icd9       edg ON hd.dx_id = edg.dx_id
    LEFT JOIN CLARITY.DATE_DIMENSION    dt ON hd.PAT_ENC_DATE_REAL = dt.epic_dte
      where hd.PAT_ENC_DATE_REAL is not null
      and hd.dx_id is not null
      and edg.code not like 'IMO0%'
	  AND trunc(dt.calendar_dt) BETWEEN '03/02/2013' AND '02/28/2018'
  UNION
  select
		  coh.pat_id,
		  hd.pat_enc_csn_id,
		  trunc(dt.calendar_dt) as contact_date,
		  edg.code as icd_code,
		  10 as icd_type
  FROM XDR_FORD_COH                 coh
  join clarity.hsp_admit_diag       hd on coh.pat_id = hd.pat_id
  JOIN clarity.edg_current_icd10    edg ON hd.dx_id = edg.dx_id
  LEFT JOIN CLARITY.DATE_DIMENSION  dt ON hd.PAT_ENC_DATE_REAL = dt.epic_dte
  where hd.PAT_ENC_DATE_REAL is not null
    and hd.dx_id is not null
    and edg.code not like 'IMO0%'
	AND trunc(dt.calendar_dt) BETWEEN '03/02/2013' AND '02/28/2018'
  ) adm
on (lcd.pat_id = adm.pat_id
  and lcd.pat_enc_csn_id = adm.pat_enc_csn_id
  and lcd.contact_date = adm.contact_date
  and lcd.icd_code = adm.icd_code
  and lcd.icd_type = adm.icd_type)
when matched then 
  update set admit_dx_flag = 'A'
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (adm.pat_id, adm.pat_enc_csn_id, adm.contact_date, adm.icd_code, adm.icd_type, null, 'A', null, null)
;
commit;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT 		
  ,'Load Admit Dx' as DESCRIPTION		
FROM XDR_FORD_DX;
COMMIT;

	--------------------------------------------------------------------------------
	--	STEP 3.3.5: Now Load HSP_ACCT_DX_LIST
	--				This table contains hospital account final diagnosis list information from the HAR master file  
	--      		final DX, Present on admission
	--  			Process line 1 (Primary final dx) first
	--------------------------------------------------------------------------------
MERGE INTO XDR_FORD_DX lcd
using
    (select
        coh.pat_id,
        t.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        9 as icd_type
    from XDR_FORD_PAT coh
    JOIN XDR_FORD_ENC t ON coh.pat_id = t.pat_id
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd9 edg ON hd.dx_id = edg.dx_id
    where t.hosp_admsn_time is not null
      and hd.line = 1
      and edg.code not like 'IMO0%'
	  AND trunc(t.hosp_admsn_time) BETWEEN '03/02/2013' AND '02/28/2018'
    UNION
    select
        coh.pat_id,
        t.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        10 as icd_type
    from XDR_FORD_PAT coh
    JOIN XDR_FORD_ENC t ON coh.pat_id = t.pat_id
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd10 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line = 1
      and edg.code not like 'IMO0%'
	  AND trunc(t.hosp_admsn_time) BETWEEN '03/02/2013' AND '02/28/2018'
    ) hsp
on (lcd.pat_id = hsp.pat_id
  and lcd.pat_enc_csn_id = hsp.pat_enc_csn_id
  and lcd.contact_date = hsp.contact_date
  and lcd.icd_code = hsp.icd_code
  and lcd.icd_type = hsp.icd_type)
when matched then 
  update set hsp_final_dx_flag = 1,
      primary_sec_flag = 'P'
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (hsp.pat_id, hsp.pat_enc_csn_id, hsp.contact_date, hsp.icd_code, hsp.icd_type,'P', null, null, 1)
;
commit;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
  ,'Load HSP_ACCT_DX_LIST' as DESCRIPTION		
FROM XDR_FORD_DX;
COMMIT;
	--------------------------------------------------------------------------------
	--	STEP 3.3.6: Process line 2-end, Secondary dx next
	--  			Don't update primary secondary flag
	--------------------------------------------------------------------------------
MERGE INTO XDR_FORD_DX lcd
using
    (select
        coh.pat_id,
        t.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        9 as icd_type
    from XDR_FORD_PAT coh
    JOIN XDR_FORD_ENC t ON coh.pat_id = t.pat_id
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd9 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
	  AND trunc(t.hosp_admsn_time) BETWEEN '03/02/2013' AND '02/28/2018'
    UNION
    select
        coh.pat_id,
        t.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        10 as icd_type
    from XDR_FORD_PAT coh
    JOIN XDR_FORD_ENC t ON coh.pat_id = t.pat_id
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd10 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
	  AND trunc(t.hosp_admsn_time) BETWEEN '03/02/2013' AND '02/28/2018'
    ) hsp
on (lcd.pat_id = hsp.pat_id
  and lcd.pat_enc_csn_id = hsp.pat_enc_csn_id
  and lcd.contact_date = hsp.contact_date
  and lcd.icd_code = hsp.icd_code
  and lcd.icd_type = hsp.icd_type)
when matched then 
  update set hsp_final_dx_flag = 1
when not matched then
  insert (pat_id, pat_enc_csn_id, contact_date, icd_code, icd_type, primary_sec_flag, admit_dx_flag, poa_flag, hsp_final_dx_flag)
  values (hsp.pat_id, hsp.pat_enc_csn_id, hsp.contact_date, hsp.icd_code, hsp.icd_type,'S', null, null, 1)
;
commit;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT 
    ,'Process line 2-end, Secondary dx next' AS DESCRIPTION
FROM XDR_FORD_DX;
COMMIT;

	--------------------------------------------------------------------------------
	--	STEP 3.3.7: Last but not least, update the POA flag if it's = 'Yes'
	--------------------------------------------------------------------------------
UPDATE XDR_FORD_DX lcd 
SET lcd.poa_flag = 'Y'
WHERE EXISTS (SELECT hsp.pat_id
              ,hsp.pat_enc_csn_id
              ,hsp.contact_date
              ,hsp.icd_code
              ,hsp.icd_type
            FROM 
            (select
        coh.pat_id,
        t.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        9 as icd_type
    from XDR_FORD_PAT coh
    JOIN XDR_FORD_ENC t ON coh.pat_id = t.pat_id
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd9 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
      and zdp.name = 'Yes'
	  AND trunc(t.hosp_admsn_time) BETWEEN '03/02/2013' AND '02/28/2018'
    UNION
    select
        coh.pat_id,
        t.pat_enc_csn_id,
        trunc(t.hosp_admsn_time) as contact_date,
        edg.code as icd_code,
        10 as icd_type
    from XDR_FORD_PAT coh
    JOIN XDR_FORD_ENC t ON coh.pat_id = t.pat_id
    join clarity.hsp_acct_dx_list hd on t.hsp_account_id = hd.hsp_account_id
    JOIN clarity.edg_current_icd10 edg ON hd.dx_id = edg.dx_id
    left join clarity.ZC_DX_POA zdp on hd.final_dx_poa_c = zdp.dx_poa_c
    where t.hosp_admsn_time is not null
      and hd.line > 1
      and edg.code not like 'IMO0%'
      and zdp.name = 'Yes'
	  AND trunc(t.hosp_admsn_time) BETWEEN '03/02/2013' AND '02/28/2018'
    ) hsp
            WHERE lcd.pat_id = hsp.pat_id
  and lcd.pat_enc_csn_id = hsp.pat_enc_csn_id
  and lcd.contact_date = hsp.contact_date
  and lcd.icd_code = hsp.icd_code
  and lcd.icd_type = hsp.icd_type);
commit;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_DX' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT  
	,COUNT(*) AS TOTAL_COUNT
  ,'update the POA flag if its = Yes' AS DESCRIPTION
FROM XDR_FORD_DX;
COMMIT;




----------------------------------------------------------------------------
-- STEP 4.4: Create Procedures table
--------------------------------------------------------------------------------
	--------------------------------------------------------------------------------
	--	STEP 4.4.1: Create destination table
	--------------------------------------------------------------------------------
DROP TABLE xdr_FORD_prc PURGE;
CREATE TABLE xdr_FORD_prc
   (	"PAT_ID" VARCHAR2(18 BYTE), 
	"PAT_ENC_CSN_ID" NUMBER(18,0), 
	"PROC_DATE" DATE, 
	"PROC_NAME" VARCHAR2(254 BYTE), 
	"PROC_CODE" VARCHAR2(254 BYTE), 
	"CODE_TYPE" VARCHAR2(254 BYTE) 
	 "PROC_PERF_PROV_ID" VARCHAR2(20 BYTE)
   );

    --------------------------------------------------------------------------------
    -- STEP 4.4.2: Insert ICD Procedures
    --------------------------------------------------------------------------------
insert into xdr_FORD_prc
SELECT distinct t.pat_id, 
		t.pat_enc_csn_id, 
		p.proc_date, 
		i.procedure_name        as PROC_NAME, 
		i.ref_bill_code         as PROC_CODE,
		zhcs.name               as code_type,
		p.proc_perf_prov_id as prov_id
FROM clarity.hsp_acct_px_list p  
      JOIN clarity.cl_icd_px i ON p.final_icd_px_id = i.icd_px_id 
      JOIN XDR_FORD_ENC t ON p.hsp_account_id = t.hsp_account_id 
      --JOIN XDR_FORD_PAT   coh on t.PAT_ENC_CSN_ID = coh.PAT_ENC_CSN_ID
      join clarity.ZC_HCD_CODE_SET zhcs on i.REF_BILL_CODE_SET_C = zhcs.CODE_SET_C
      WHERE p.proc_date BETWEEN '03/02/2013' AND '02/28/2018';
COMMIT;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PRC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT 	
    ,'Insert ICD Procedures'    as DESCRIPTION
FROM XDR_FORD_PRC;
COMMIT;

    --------------------------------------------------------------------------------
    -- STEP 4.4.3: Insert CPT Procedures - Professional
    --------------------------------------------------------------------------------
insert into xdr_FORD_prc
SELECT arpb.patient_id 								    AS pat_id
                      ,arpb.pat_enc_csn_id
                      ,arpb.service_date                AS proc_date 
                      ,eap.proc_name                    AS PROC_NAME
                      ,arpb.cpt_code                    AS PROC_CODE
                      ,'CPT-Professional'               AS code_type
                      ,arpb.SERV_PROVIDER_ID            AS prov_id
        FROM clarity.arpb_transactions  arpb 
        join XDR_FORD_ENC        enc on arpb.pat_enc_csn_id = enc.pat_enc_csn_id
        LEFT JOIN clarity_eap                   eap  ON arpb.cpt_code = eap.proc_code
        WHERE --patient_id is not null AND 
          tx_type_c = 1					-----  Charges only
          AND void_date is null
          AND arpb.service_date BETWEEN '03/02/2013' AND '02/28/2018'; 

COMMIT;
--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PRC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT 
	,COUNT(*) AS TOTAL_COUNT 
    ,'Insert CPT Procedures - Professional' AS DESCRIPTION
FROM XDR_FORD_PRC;
COMMIT;
    --------------------------------------------------------------------------------
    -- STEP 4.4.4: Insert CPT Procedures - Hospital
    --------------------------------------------------------------------------------
insert into xdr_FORD_prc
SELECT hsp.pat_id
                ,hspt.pat_enc_csn_id
                ,hspt.service_date                                      AS proc_date
                ,eap.proc_name                                          AS PROC_NAME
                ,substr(coalesce(hspt.hcpcs_code,hspt.cpt_code),1,5)    AS PROC_CODE
                ,'CPT-Hospital'                                         AS code_type   
                ,hspt.PERFORMING_PROV_ID                                AS prov_id
            FROM clarity.hsp_account       hsp   
            JOIN clarity.hsp_transactions           hspt  ON hsp.hsp_account_id = hspt.hsp_account_id
            LEFT JOIN clarity.f_arhb_inactive_tx    fait on hspt.tx_id = fait.tx_id
            join XDR_FORD_ENC                enc on hspt.pat_enc_csn_id = enc.pat_enc_csn_id
            LEFT JOIN clarity.CLARITY_EAP           eap ON hspt.proc_id = eap.proc_id
          where hspt.tx_type_ha_c = 1  
          and (length(hspt.cpt_code) = 5 or hspt.hcpcs_code is not null)
          and fait.tx_id is null
		  AND hspt.service_date BETWEEN '03/02/2013' AND '02/28/2018'; 
COMMIT;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_PRC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT 
	,COUNT(*) AS TOTAL_COUNT 
    ,'Insert CPT Procedures - Hospital' AS DESCRIPTION
FROM XDR_FORD_PRC;
COMMIT;




--------------------------------------------------------------------------------
-- STEP 3.5: Create Flowsheet table
--------------------------------------------------------------------------------
--		Codes for common flowsheets might differ among sites. Please confirm for: 
/*
				     '11'         --Height
                                    ,'14'         --Weight
                                    ,'5'          --Blood Pressure  
                                    ,'8'          --Pulse
                                    ,'6'          --Temperature
                                    ,'9'          --Respiratory Rate 
                                    ,'301070'     --BMI
                                    ,'10'         --Pulse Oximetry (SpO2)
*/
--------------------------------------------------------------------------------				    
DROP TABLE xdr_FORD_flo PURGE;
CREATE TABLE xdr_FORD_flo AS 
SELECT DISTINCT coh.pat_id
                       ,enc.pat_enc_csn_id
                       ,enc.INPATIENT_DATA_ID
                       ,meas.flt_id
                       ,meas.flo_meas_id
                       ,dta.display_name      AS template_name
                       ,gpd.disp_name         AS measure_name
                       ,gpd.flo_meas_name     
                       ,meas.recorded_time
                       ,meas.meas_value       AS measure_value
          FROM XDR_FORD_PAT           coh 
          JOIN XDR_FORD_ENC        enc   ON coh.pat_id = enc.pat_id
          JOIN clarity.ip_flwsht_rec      rec   ON enc.inpatient_data_id = rec.inpatient_data_id
          JOIN clarity.ip_flwsht_meas     meas  ON rec.fsd_id = meas.fsd_id
          JOIN clarity.ip_flo_gp_data     gpd   ON meas.flo_meas_id = gpd.flo_meas_id
          JOIN clarity.ip_flt_data        dta   ON meas.flt_id = dta.template_id
          WHERE meas.recorded_time IS NOT NULL 
            AND meas.meas_value IS NOT NULL
            AND meas.flo_meas_id IN ('11'         --Height
                                    ,'14'         --Weight
                                    ,'5'          --Blood Pressure  
                                    ,'8'          --Pulse
                                    ,'6'          --Temperature
                                    ,'9'          --Respiratory Rate 
                                    ,'301070'     --BMI
                                    ,'10'         --Pulse Oximetry (SpO2)
                                    )
			AND meas.recorded_time BETWEEN '03/02/2013' AND '02/28/2018'; 									

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_flo' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT 
	,COUNT(*) AS TOTAL_COUNT 
    ,'Insert Flowsheets records' AS DESCRIPTION
FROM xdr_FORD_flo;
COMMIT;



--------------------------------------------------------------------------------
-- STEP 3.6: Create Social History table
--------------------------------------------------------------------------------
DROP TABLE XDR_FORD_SOC PURGE;
CREATE TABLE XDR_FORD_SOC AS
SELECT DISTINCT coh.pat_id 
               ,soc.pat_enc_csn_id            
               ,soc.pat_enc_date_real
               ,pat.birth_date
               ,trunc(months_between(CURRENT_DATE, pat.birth_date)/12) AS age
               ,xsx.NAME                                               AS gender
            --Get sexual history below
               ,xsa.NAME                                               AS sexually_active
               ,soc.female_partner_yn                                           --never nulls; defaults to "N" when unchecked
               ,soc.male_partner_yn                                             --never nulls; defaults to "N" when unchecked
               ,soc.condom_yn
               ,soc.pill_yn
               ,soc.diaphragm_yn
               ,soc.iud_yn
               ,soc.surgical_yn
               ,soc.spermicide_yn
               ,soc.implant_yn
               ,soc.rhythm_yn
               ,soc.injection_yn
               ,soc.sponge_yn
               ,soc.inserts_yn
               ,soc.abstinence_yn
            --Get drug history below
               ,soc.iv_drug_user_yn 
               ,soc.illicit_drug_freq  
               ,soc.illicit_drug_cmt 
  FROM XDR_FORD_COH              coh
  JOIN clarity.patient                  pat ON coh.pat_id = pat.pat_id
  LEFT JOIN clarity.social_hx           soc ON pat.pat_id = soc.pat_id
  LEFT JOIN clarity.social_hx_alc_use   soa ON soc.pat_enc_csn_id = soa.pat_enc_csn_id
  LEFT JOIN clarity.zc_sexually_active  xsa ON soc.sexually_active_c = xsa.sexually_active_c
  LEFT JOIN clarity.zc_sex              xsx ON pat.sex_c = xsx.rcpt_mem_sex_c
  LEFT JOIN clarity.zc_tobacco_user     xtb ON soc.tobacco_user_c = xtb.tobacco_user_c
  LEFT JOIN clarity.zc_smoking_tob_use  xsm ON soc.smoking_tob_use_c = xsm.smoking_tob_use_c
  LEFT JOIN clarity.zc_alcohol_use      xal ON soc.alcohol_use_c = xal.alcohol_use_c
  LEFT JOIN clarity.zc_hx_drink_types   xdt ON soa.hx_drink_types_c = xdt.hx_drink_types_c
  WHERE soc.pat_enc_date_real = (SELECT MAX(soc.pat_enc_date_real) FROM social_hx soc WHERE soc.pat_id = coh.pat_id)
;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_SOC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT 
	,COUNT(*) AS TOTAL_COUNT 		
  ,'Create social history table' AS DESCRIPTION
FROM XDR_FORD_SOC;
COMMIT;
--------------------------------------------------------------------------------
-- STEP 3.7: Create Family History table
--------------------------------------------------------------------------------
DROP TABLE xdr_FORD_fam PURGE;
CREATE TABLE xdr_FORD_fam AS
SELECT DISTINCT pat.pat_id 
               ,fam.pat_enc_csn_id       
               ,fam.line
               ,fam.medical_hx_c
               ,xmh.NAME                  AS medical_hx
               ,fam.relation_c
               ,xrc.NAME                  AS relation
  FROM XDR_FORD_PAT              pat
  JOIN clarity.family_hx                fam ON pat.pat_id = fam.pat_id
  LEFT JOIN clarity.zc_medical_hx       xmh ON fam.medical_hx_c = xmh.medical_hx_c
  LEFT JOIN clarity.zc_msg_caller_rel   xrc ON fam.relation_c = xrc.msg_caller_rel_c 
  WHERE fam.pat_enc_date_real = (SELECT MAX(fam.pat_enc_date_real) FROM clarity.family_hx fam WHERE fam.pat_id = pat.pat_id)
;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_FAM' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT 
	,COUNT(*) AS TOTAL_COUNT 		
  ,'Create family history table' AS DESCRIPTION
FROM XDR_FORD_FAM;
COMMIT;



--------------------------------------------------------------------------------
-- STEP 3.8: Create Problem List table
--------------------------------------------------------------------------------
DROP TABLE xdr_FORD_pl purge;
CREATE TABLE xdr_FORD_pl AS
SELECT DISTINCT enc.pat_id
               ,enc.pat_enc_csn_id
               ,enc.effective_date_dt as encounter_date
               ,pl.problem_list_id
               ,pl.dx_id
               ,pl.noted_date                 AS noted_date
               ,pl.date_of_entry              AS update_date
               ,pl.resolved_date              AS resolved_date
               ,zps.name                      AS problem_status        
               ,zhp.name                      AS priority
               ,PL.PRINCIPAL_PL_YN            AS principal_yn
               ,pl.chronic_yn                 AS chronic_yn
  FROM xdr_FORD_enc                  enc
  JOIN clarity.problem_list                 pl    ON enc.pat_enc_csn_id = pl.problem_ept_csn AND rec_archived_yn = 'N'
  LEFT JOIN clarity.clarity_ser             ser   ON pl.entry_user_id = ser.user_id
  LEFT JOIN clarity.v_cube_d_provider       prv   ON ser.prov_id = prv.provider_id
  LEFT JOIN clarity.zc_problem_status       zps   ON pl.problem_status_c = zps.problem_status_c
  LEFT JOIN clarity.zc_hx_priority          zhp   ON pl.priority_c = zhp.hx_priority_c
  WHERE
		pl.noted_date BETWEEN '03/02/2013' AND '02/28/2018'; 

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_pl' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	        --30855
	,COUNT(*) AS TOTAL_COUNT 		                --301122
  ,'Create Problem list table' AS DESCRIPTION
FROM xdr_FORD_pl;
COMMIT;


--------------------------------------------------------------------------------
-- STEP 3.9: Create Problem List Diagnosis table
--------------------------------------------------------------------------------
DROP TABLE xdr_FORD_pldx PURGE;
CREATE TABLE xdr_FORD_pldx AS
SELECT DISTINCT pl.PAT_ID,
              pl.PAT_ENC_CSN_ID,
              'PROBLEM_LIST'           AS diagnosis_source, 
              --DX_ID,								commented out on 6/9/17 to avoid confusion
              pl.PROBLEM_LIST_ID,
              pl.NOTED_DATE               AS diagnosis_date,
              pl.PRINCIPAL_YN             AS primary_dx_yn, 
              pl.PRIORITY,
              pl.RESOLVED_DATE,
              pl.update_date,
              pl.problem_status,
              --NULL                     AS SOURCE,
              --ICD CODES
              case when NOTED_DATE <= '01/01/2015' then 9 else 10 end icd_type,
              case when NOTED_DATE <= '01/01/2015' then cin9.CODE else cin10.CODE end icd_code
--              case when NOTED_DATE <= '01/01/2015' then icd9.icd_desc else icd10.icd_desc end icd_desc
FROM xdr_FORD_pl               pl
  --ICD9 CODES JOIN
  LEFT JOIN clarity.edg_current_icd9                cin9  ON pl.dx_id = cin9.dx_id AND cin9.line = 1
  --ICD10 CODES JOIN
  LEFT JOIN clarity.edg_current_icd10               cin10 ON pl.DX_ID = cin10.dx_id AND cin10.line = 1
;


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_pldx' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
  ,'Create Problem list Diagnosis table' AS DESCRIPTION  
FROM xdr_FORD_pldx;
COMMIT;

--------------------------------------------------------------------------------
-- STEP 3.10: Create Providers table
--------------------------------------------------------------------------------
DROP TABLE xdr_FORD_prov PURGE;
CREATE TABLE xdr_FORD_prov AS
SELECT rownum as prov_study_id
        ,x.*
FROM (SELECT DISTINCT prov.prov_id               AS provider_id
                ,prv.provider_type
                ,prv.primary_specialty
                ,CASE WHEN   ser.ACTIVE_STATUS = 'Active'  AND  emp.USER_STATUS_C = 1 THEN 1
                    ELSE NULL 
                END active_providers
                ,CASE WHEN   emp.user_id IS NOT NULL THEN 1
                    ELSE NULL
                END UC_provider
                ,CASE WHEN hiv.PROVIDER_ID IS NOT NULL THEN 1
                    ELSE 0
                END HIV_PROVIDER
    FROM 
        (--All provider from encounters + procedures + patient PCP + HIV providers
        select visit_prov_id as prov_id from xdr_FORD_enc
        UNION
        select PROC_PERF_PROV_ID as prov_id from xdr_FORD_prc
        UNION
        select CUR_PCP_PROV_ID as prov_id from XDR_FORD_pat
        UNION
        SELECT PROVIDER_ID FROM XDR_FORD_PROVDRV
        ) prov
    LEFT JOIN clarity.v_cube_d_provider       prv   ON prov.prov_id = prv.provider_id
    LEFT JOIN XDR_FORD_PROVDRV                        hiv ON prov.PROV_ID = hiv.PROVIDER_ID 
    --check for active providers
    LEFT JOIN clarity.clarity_ser                     ser ON prov.prov_id = ser.PROV_ID
    LEFT JOIN clarity.CLARITY_EMP                     emp ON prov.PROV_ID = emp.PROV_ID 
    ) x
ORDER BY  dbms_random.value
;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_prov' AS TABLE_NAME
	,NULL AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
  ,'Create Providers Diagnosis table' AS DESCRIPTION  
FROM xdr_FORD_prov;
COMMIT;


--------------------------------------------------------------------------------
-- STEP 3.11: Create ADT table
--------------------------------------------------------------------------------
DROP TABLE XDR_FORD_ADT PURGE;
CREATE TABLE xdr_FORD_adt AS
select adt.pat_id,
                adt.pat_enc_csn_id,
               case when adt.event_type_c = 1 and adt.next_out_event_id = enc2.hsp_dis_event_id then 'Admit/Discharge'
                  when adt.event_type_c = 1 then 'Admit'
                  when adt.event_type_c = 3 and adt.next_out_event_id = enc2.hsp_dis_event_id then 'Discharge'
                  else 'Transfer' end as event_type,
               adt.event_id as in_event_id,
               adt.department_id,
               dept.department_name,
               dept.dept_abbreviation,
               dept.specialty     AS department_specialty,
               dept.rev_loc_id,
               loc.loc_name,
               adt.effective_time as time_in,
               adtout.effective_time as time_out,
               adt.next_out_event_id as out_event_id,
               --- adm_event_id and dis_event_id have been deprecated and replaced
               ---  by the pat_enc_hsp2 fields (hsp_adm_event_id and hsp_dis_event_id)
               enc2.hsp_adm_event_id as adm_event_id,
               enc2.hsp_dis_event_id as dis_event_id
          from clarity_adt adt
          join i2b2.lz_clarity_patient pat on adt.pat_id = pat.pat_id
          join i2b2.lz_clarity_enc enc on adt.pat_enc_csn_id = enc.pat_enc_csn_id
          join pat_enc_hsp_2 enc2 on adt.pat_enc_csn_id = enc2.pat_enc_csn_id
          left join clarity_adt adtout on adt.next_out_event_id = adtout.event_id
          left join clarity_dep dept on adt.department_id = dept.department_id
          left join clarity_loc loc on dept.rev_loc_id = loc.loc_id
          where adt.event_type_c in (1,3) 
           and adt.event_subtype_c != 2
           and enc2.hsp_adm_event_id is not null;

--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_ADT' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
  ,'Create ADT table' AS DESCRIPTION  
FROM XDR_FORD_ADT;
COMMIT;
           
--------------------------------------------------------------------------------
-- STEP 3.12: Create Appointments table
--------------------------------------------------------------------------------
DROP TABLE XDR_FORD_APPT PURGE;
CREATE TABLE xdr_FORD_appt AS
SELECT DISTINCT coh.pat_id
               ,vsa.pat_enc_csn_id
               ,vsa.appt_status_c
               ,vsa.appt_status_name    AS appt_status
               ,vsa.appt_conf_stat_c
               ,vsa.appt_conf_stat_name AS appt_confirmation_status 
               ,vsa.appt_block_c
               ,vsa.appt_block_name     AS appt_block
               ,vsa.appt_dttm
               ,vsa.appt_length
               ,vsa.appt_made_dttm
               ,vsa.prc_id              ----  This is the visit type !!!
               ,clprc.record_type       AS prc_v_type
               ,clprc.status            AS prc_v_type_status
               ,clprc.prc_name          AS visit_type
               ,vsa.checkin_dttm
               ,vsa.checkout_dttm
               ,vsa.cancel_reason_c
               ,vsa.cancel_reason_name  AS cancel_reason
               ,appt_serial_num
               ,vsa.department_id
               ,vsa.department_name
               ,vsa.dept_specialty_c   
               ,vsa.dept_specialty_name AS department_specialty
               ,vsa.loc_id            
               ,vsa.loc_name
               ,vsa.center_c      
               ,vsa.center_name
               ,vsa.prov_id
               ,vsa.prov_name_wid
               ,vsa.referring_prov_id
               ,vsa.referring_prov_name_wid
  FROM xdr_FORD_coh                            coh
  JOIN v_sched_appt           vsa     ON coh.pat_id = vsa.pat_id
  LEFT JOIN clarity_prc       clprc   ON vsa.prc_id = clprc.prc_id
  WHERE checkin_dttm BETWEEN '03/02/2013' AND '02/28/2018'
; 


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_APPT' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT	
	,COUNT(*) AS TOTAL_COUNT 		
  ,'Create appointments table' AS DESCRIPTION  
FROM XDR_FORD_APPT;
COMMIT;


-
