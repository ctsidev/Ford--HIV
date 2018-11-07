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
			AND e.effective_date_dt BETWEEN '03/02/2013' AND '03/31/2018';
--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_ENC' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Encounter counts' AS DESCRIPTION
FROM XDR_FORD_ENC;
COMMIT;            

------------------------------------------------------
--  Step 3.2:   Pull medications
--
------------------------------------------------------
DROP TABLE XDR_FORD_med PURGE;
CREATE TABLE XDR_FORD_med as
select DISTINCT med1.*,
            cm.pharm_class_c,
            zpc.name as pharm_class,
            cm.thera_class_c,
            ztc.name as thera_class,
            cm.pharm_subclass_c,
            zsc.name as pharm_subclass,
            cm.name medication_name, 
            cm.generic_name
FROM (
        SELECT  m.pat_id,
                m.pat_enc_csn_id, 
                m.order_med_id, 
                /*m.ordering_date, 
                m.start_date,
                m.end_date,*/
                m.ORDER_INST,
                m.ORDER_START_TIME,
                m.ORDER_END_TIME,
            /*
            In some circumstances, for example when Intelligent Medication Selection selects an IMS mixture, this column may contain template records that do not represent real
        medications. For this reason, it is recommended to use ORDER_MEDINFO. DISPENSABLE_MED_ID when reporting on medication orders.
        Additionally, in some cases where dispensable_med_id is not populated, user_sel_med_id is the field form where to obtain the medication_id
        */
              case when m.medication_id != 800001 then m.medication_id
                   else coalesce(omi.dispensable_med_id, m.user_sel_med_id) end as used_med_id,        
                m.medication_id, 
              --omi.dispensable_med_id,
              --m.user_sel_med_id,
                m.hv_discrete_dose,
                zmu.name as dose_unit,
                m.MED_DIS_DISP_QTY,
                zmudis.name as dis_dose_unit,
                zos.name as order_status,
                zom.name as ordering_mode,
                zoc.name as order_class,
                omi.last_admin_inst,
                m.sig,
                m.quantity,
                ipf.freq_name,
                m.refills,
                rou.NAME                    AS route_name,
                rou.abbr                    AS route_abbreviation,
                mar.INFUSION_RATE,
                mar.MAR_INF_RATE_UNIT_C,
                mar.taken_time,
                zmudis.name as inf_rate_dose_unit
        FROM clarity.order_med m 
        JOIN XDR_FORD_COH                   coh ON m.pat_id = coh.pat_id
        LEFT JOIN clarity.order_medinfo     omi ON m.order_med_id = omi.order_med_id
        LEFT JOIN clarity.mar_admin_info    mar ON m.order_med_id = mar.order_med_id
        LEFT JOIN clarity.zc_admin_route    rou ON mar.route_c = rou.MED_ROUTE_C
        left join clarity.ip_frequency      ipf ON m.hv_discr_freq_id = ipf.freq_id
        left join clarity.zc_med_unit       zmu ON m.hv_dose_unit_c = zmu.disp_qtyunit_c
        left join clarity.zc_order_status   zos ON m.order_status_c = zos.order_status_c
        left join clarity.zc_ordering_mode  zom ON m.ordering_mode_c = zom.ordering_mode_c
        left join clarity.zc_med_unit       zmudis ON m.MED_DIS_DISP_UNIT_C = zmudis.disp_qtyunit_c
        left join clarity.zc_med_unit       zmudis2 ON mar.MAR_INF_RATE_UNIT_C = zmudis2.disp_qtyunit_c
        left join clarity.zc_order_class    zoc ON m.order_class_C = zoc.order_class_c
        WHERE m.ordering_date is not null
                OR m.start_date is not null
        ) med1
LEFT JOIN clarity.clarity_medication    cm ON med1.used_med_id = cm.medication_id
left join clarity.zc_pharm_class        zpc ON cm.pharm_class_c = zpc.pharm_class_c
left join clarity.zc_thera_class        ztc ON cm.thera_class_c = ztc.thera_class_c
left join clarity.zc_pharm_subclass     zsc ON cm.pharm_subclass_c = zsc.pharm_subclass_c
WHERE med1.ORDER_START_TIME between to_date('03/02/2006','mm/dd/yyyy') and to_date('03/31/2018','mm/dd/yyyy')  
;


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_MED' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Medication counts' AS DESCRIPTION
FROM XDR_FORD_med;
COMMIT;      