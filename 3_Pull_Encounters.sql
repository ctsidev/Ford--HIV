/********************************************************************************************************

    Step 2
        Create Encounters table

********************************************************************************************************/


-- *******************************************************************************************************
-- STEP 2.1
--          Pull encounters information
--
-- *******************************************************************************************************
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
			AND e.effective_date_dt BETWEEN '03/02/2013' AND '03/31/2018';