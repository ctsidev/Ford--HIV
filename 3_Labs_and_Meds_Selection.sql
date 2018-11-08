/********************************************************************************************************

    Step 3
        Pull all meds and labs, generate drivers to send to PI, and apply selection

********************************************************************************************************/

------------------------------------------------------
--  Step 3.1:   Process Medications
--
------------------------------------------------------

    ------------------------------------------------------
    --  Step 3.1.1:   Pull medications
    --
    ------------------------------------------------------
DROP TABLE XDR_FORD_medall PURGE;
CREATE TABLE XDR_FORD_medall as
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
SELECT 'XDR_FORD_MEDALL' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Create table with all med results' as DESCRIPTION
FROM XDR_FORD_MEDALL;
COMMIT;      

    ------------------------------------------------------
    --  Step 3.1.2:   Export Meds driver to a file
    --
    ------------------------------------------------------
SELECT med.medication_id
      ,med.medication_name
      ,med.generic_name
      ,pc.name              AS pharm_class
      ,tc.name              AS thera_class
      ,sc.name              AS pharm_subclass
      ,COUNT(*)             AS total 
	FROM xdr_ford_medall                     med
  JOIN clarity_medication       cm  ON med.medication_id = cm.medication_id
  LEFT JOIN zc_pharm_class      pc  ON cm.pharm_class_c = pc.pharm_class_c
  LEFT JOIN zc_thera_class      tc  ON cm.thera_class_c = tc.thera_class_c
  LEFT JOIN zc_pharm_subclass   sc  ON cm.pharm_subclass_c = sc.pharm_subclass_c
  GROUP BY med.medication_id, med.medication_name, med.generic_name, pc.name, tc.name, sc.name;


    --------------------------------------------------------------------------------
    --	STEP 3.1.3: Create Meds driver table to load the PI selection
    --------------------------------------------------------------------------------  
DROP TABLE xdr_FORD_meddrv PURGE;
CREATE TABLE xdr_FORD_meddrv
   (	"PROC_ID" NUMBER(18,0), 
	"DESCRIPTION" VARCHAR2(254 BYTE), 
	"COMPONENT_ID" NUMBER(18,0), 
	"COMPONENT_NAME" VARCHAR2(75 BYTE));


    --------------------------------------------------------------------------------
    --	STEP 3.1.4: Load Meds driver table with selections made by PI
    --------------------------------------------------------------------------------
    -- Your site will receive a file with the layout used above that shall contain ONLY
    -- the medication records selected by the PI. This selection is based on the output file
    -- generated by step 3.2
    -- You shall use the utility of your choice to load this file into xdr_FORD_meddrv
    -- which is used on step 3.6 to pull the appropiate set of records.
    -- The file shall be formatted as a CSV with double quotation marks as text identifier.


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_MED' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Create table with all med results' as DESCRIPTION
FROM XDR_FORD_med;
COMMIT;      

    --------------------------------------------------------------------------------
    --	STEP 3.2.5: Apply Labs driver selection to final selection
    --------------------------------------------------------------------------------   


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT,TOTAL_COUNT, DESCRIPTION)
SELECT 'XDR_FORD_MED' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Create table with all med results' as DESCRIPTION
FROM XDR_FORD_med;
COMMIT;      

--------------------------------------------------------------------------------
--	STEP 3.2: Process Labs
--------------------------------------------------------------------------------

    --------------------------------------------------------------------------------
    --	STEP 3.2.1: Create Labs table
    -------------------------------------------------------------------------------- 
DROP TABLE xdr_FORD_laball PURGE;
CREATE TABLE xdr_FORD_laball AS 
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
              FROM clarity.order_results        o
              JOIN XDR_FORD_coh          coh ON o.pat_id = coh.pat_id
              JOIN clarity.order_proc           p   ON p.order_proc_id = o.order_proc_id 
              JOIN clarity.clarity_component    cc  ON o.component_id = cc.component_id
              LEFT JOIN clarity.order_proc_2    op2 ON p.ORDER_PROC_ID = op2.ORDER_PROC_ID
              where p.order_type_c in (7, 26, 62, 63)			--doulbe check this codes
                      and p.ordering_date between to_date('03/02/2013','mm/dd/yyyy') and to_date('02/28/2018','mm/dd/yyyy')
                      and o.ord_value is not null
                      and o.order_proc_id is not null
;



--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,PAT_COUNT ,PAT_COUNT, TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_laball' AS TABLE_NAME
	,COUNT(distinct pat_id) AS PAT_COUNT
	,COUNT(*) AS TOTAL_COUNT
    ,'Create table with all lab results' as DESCRIPTION
FROM xdr_FORD_laball;
COMMIT;


    --------------------------------------------------------------------------------
    --	STEP 3.2.2: Export Labs driver to a file
    --------------------------------------------------------------------------------  
SELECT proc_id, description, component_id, component_name, COUNT(*) AS total 
	FROM xdr_FORD_laball    
  GROUP BY proc_id, description, component_id, component_name
  ORDER BY component_name;



    --------------------------------------------------------------------------------
    --	STEP 3.2.3: Create Labs driver table to load the PI selection
    --------------------------------------------------------------------------------  
DROP TABLE xdr_FORD_labdrv PURGE;
CREATE TABLE xdr_FORD_labdrv
   (	"PROC_ID" NUMBER(18,0), 
	"DESCRIPTION" VARCHAR2(254 BYTE), 
	"COMPONENT_ID" NUMBER(18,0), 
	"COMPONENT_NAME" VARCHAR2(75 BYTE));



    --------------------------------------------------------------------------------
    --	STEP 3.2.4: Load Labs driver table with selections made by PI
    --------------------------------------------------------------------------------
    -- Your site will receive a file with the layout used above that shall contain ONLY
    -- the medication records selected by the PI. This selection is based on the output file
    -- generated by step 6.2
    -- You shall use the utility of your choice to load this file into xdr_Wherry_preg_labdrv
    -- which is used on step 6.5 to pull the appropiate set of records.
    -- The file shall be formatted as a CSV with double quotation marks as text identifier.


--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_labdrv' AS TABLE_NAME
	,COUNT(*) AS TOTAL_COUNT	
  ,'Load PI lab driver selection' as DESCRIPTION
FROM xdr_FORD_labdrv;
COMMIT;

    --------------------------------------------------------------------------------
    --	STEP 3.2.5: Apply Labs driver selection to final selection
    --------------------------------------------------------------------------------    
DROP TABLE xdr_FORD_lab PURGE;
CREATE TABLE xdr_FORD_lab AS 
SELECT DISTINCT lab.*
  FROM xdr_FORD_laball            lab  
  JOIN xdr_FORD_labdrv            drv ON lab.proc_id = drv.proc_id AND lab.component_id = drv.component_id
;



--Add counts for QA
INSERT INTO XDR_FORD_COUNTS(TABLE_NAME,TOTAL_COUNT, PAT_COUNT, DESCRIPTION)
SELECT 'xdr_FORD_lab' AS TABLE_NAME
	,COUNT(*) AS TOTAL_COUNT	
    ,COUNT(DISTINCT PAT_ID) AS PAT_COUNT
    ,'Final lab selection based on driver selection' as DESCRIPTION
FROM xdr_FORD_lab;
COMMIT;
