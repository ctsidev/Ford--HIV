-- *******************************************************************************************************
-- Step 1 COHORT COUNTS

--   1.1 Create temp table with patients with a HIV diagnosis
--   1.2 Create temp table with patients with a HIV Labs
--	 1.3 Merge HIV DX + HIV Lab

-- *******************************************************************************************************

-- *******************************************************************************************************
-- STEP 1.1
--          Create temp table with patients with a HIV diagnosis codes B20 or 042 from '03/02/2013' AND '02/28/2018'
--
-- *******************************************************************************************************

SELECT con.person_id
		,min(con.condition_start_date) AS FIRST_HIV_DATE
INTO #XDR_FORD_DX_HIV_coh
FROM [OMOP].[dbo].[CONDITION_occurrence] con
-- HIV diagnosis
JOIN  (SELECT DISTINCT [concept_id] FROM [OMOP].[dbo].[concept]			 
  WHERE 
		(
		  (concept_code LIKE 'B20%' AND VOCABULARY_ID = 'ICD10')
		  OR
		  (concept_code LIKE '042%' AND VOCABULARY_ID = 'ICD9CM')
		 )
		  and domain_id = 'Condition'
		) dx ON con.condition_concept_id = dx.concept_id
WHERE 
	con.condition_start_date BETWEEN '03/02/2013' AND '02/28/2018'
GROUP BY con.person_id;
--(3262 rows affected) Clarity 3,551

SELECT COUNT(*), COUNT(DISTINCT PERSON_ID) FROM #XDR_FORD_DX_HIV_coh;--3262	3262

-- *******************************************************************************************************
-- STEP 1.1
--          Create temp table with patients with a HIV labs from '03/02/2013' AND '02/28/2018'
--
-- *******************************************************************************************************
drop table #xdr_ford_labs;
SELECT distinct meas.measurement_id,
meas.person_id,
meas.measurement_date,
conc.concept_code as lab_concept_code, 
conc.concept_name as lab_concept_name,
cVal.concept_code as value_concept_code,
cVal.concept_name as value_concept_name,
meas.range_high,
meas.range_low,
meas.value_as_number,
meas.value_as_concept_id,
meas.value_source_value,
meas.unit_source_value,
meas.unit_concept_id,
cUnit.concept_name as unit_concept_name,
cRange.concept_name as range_concept_name,
cOper.concept_name as operator_concept_name,
cType.concept_name as measure_type_concept_name
into #xdr_ford_labs
  FROM [OMOP].[dbo].[measurement]		meas
  join [OMOP].[dbo].[concept]			conc on meas.measurement_concept_id = conc.concept_id
  LEFT JOIN [OMOP].[dbo].[concept]		cVal ON meas.value_as_concept_id = cVal.concept_id 
  LEFT JOIN [OMOP].[dbo].[concept]		cType ON meas.measurement_type_concept_id = cType.concept_id 
  LEFT JOIN [OMOP].[dbo].[concept]		cUnit ON meas.unit_concept_id = cUnit.concept_id 
  LEFT JOIN [OMOP].[dbo].[concept]		cRange ON meas.value_as_concept_id = cRange.concept_id 
  LEFT JOIN [OMOP].[dbo].[concept]		cOper ON meas.operator_concept_id = cOper.concept_id
  where 
  meas.measurement_date BETWEEN '2013-03-02' AND '2018-02-28'
  AND conc.concept_code in ('74856-6',
'49573-9',
'48558-1',
'53798-5',
'34700-5',
'33630-5',
'30554-0',
'45176-5',
'45175-7',
'85380-4',
'9836-8',
'44533-8',
'75666-8',
'58900-2',
'56888-1',
'48346-1',
'48345-3',
'57975-5',
'22357-8',
'7918-6',
'44873-8',
'5223-3',
'31201-7',
'80387-4',
'73906-0',
'43009-0',
'40733-8',
'73905-2',
'43008-2',
'41290-8',
'85361-4',
'85368-9',
'32602-5',
'54086-4',
'43010-8',
'42600-7',
'49580-4',
'80695-0',
'80694-3',
'80692-7',
'80693-5',
'80691-9',
'79380-2',
'79379-4',
'74854-1',
'9837-6',
'44871-2',
'48023-6',
'78007-2',
'78009-8',
'88212-6',
'78010-6',
'78008-0',
'77368-9',
'74855-8',
'74853-3',
'21007-0',
'44531-2',
'47359-5',
'53923-9',
'88453-6',
'19110-6',
'35452-2',
'35564-4',
'35565-1',
'31072-2',
'21332-2',
'32842-7',
'32827-8',
'33508-3',
'12855-3',
'12857-9',
'12858-7',
'12870-2',
'12871-0',
'12872-8',
'12875-1',
'12876-9',
'12893-4',
'12894-2',
'12895-9',
'43013-2',
'14126-7',
'40439-2',
'16132-3',
'83325-1',
'80689-3',
'81122-4',
'80690-1',
'50790-5',
'57182-8',
'80688-5',
'87963-5',
'44532-0',
'9661-0',
'35441-5',
'9660-2',
'35440-7',
'43012-4',
'9662-8',
'40438-4',
'35446-4',
'9663-6',
'35449-8',
'12859-5',
'35450-6',
'44872-0',
'16978-9',
'43011-6',
'9664-4',
'21331-4',
'40437-6',
'9665-1',
'9821-0',
'53601-1',
'42339-2',
'18396-2',
'33660-2',
'16979-7',
'49718-0',
'35448-0',
'9666-9',
'35447-2',
'9667-7',
'35445-6',
'9668-5',
'35444-9',
'12856-1',
'35443-1',
'9669-3',
'35442-3',
'87962-7',
'73658-7',
'49483-1',
'44607-0',
'22356-0',
'43599-0',
'7917-8',
'14092-1',
'5220-9',
'29893-5',
'5221-7',
'68961-2',
'86233-4',
'85686-4',
'13499-9',
'16975-5',
'40732-0',
'16976-3',
'24012-7',
'5222-5',
'5017-9',
'23876-6',
'29539-4',
'59419-2',
'70241-5',
'24013-5',
'21333-0',
'10351-5',
'62469-2',
'41516-6',
'41514-1',
'29541-0',
'51780-5',
'48510-2',
'48552-4',
'21008-8',
'41513-3',
'41515-8',
'20447-9',
'48551-6',
'48511-0',
'25835-0',
'41145-4',
'33866-5',
'29327-4',
'34591-8',
'34592-6',
'16974-8',
'57974-8',
'42627-0',
'31430-2',
'28004-0',
'28052-9',
'16977-1',
'41497-9',
'41498-7',
'42917-5',
'41143-9',
'35438-1',
'41144-7',
'35437-3',
'35439-9',
'77369-7',
'32571-2',
'53379-4',
'89374-3',
'49905-3',
'49890-7',
'25836-8',
'5018-7',
'42768-2',
'69668-2',
'80203-3',
'43185-8',
'77685-6',
'73659-5',
'45212-8',
'57976-3',
'57977-1',
'57978-9',
'62456-9',
'10901-7',
'10902-5',
'11078-3',
'11079-1',
'11080-9',
'11081-7',
'11082-5',
'13920-4',
'21334-8',
'21335-5',
'21336-3',
'21337-1',
'21338-9',
'21339-7',
'21340-5',
'22358-6',
'7919-4',
'5225-8',
'5224-1',
'30361-0',
'81641-3',
'31073-0',
'51786-2',
'33807-9',
'33806-1',
'86548-5',
'69354-9',
'81652-0',
'69353-1',
'47029-4',
'86549-3',
'86547-7',
'80695-0',
'80694-3',
'6429-5',
'6430-3',
'6431-1',
'49573-9',
'48558-1',
'45182-3',
'83326-9',
'83327-7',
'53798-5',
'34700-5',
'30554-0',
'45176-5',
'88544-2',
'45175-7',
'49659-6',
'49664-6',
'33630-5',
'49656-2',
'49661-2',
'88543-4',
'88542-6',
'61199-6',
'21009-6',
'85037-0',
'89365-1',
'75622-1',
'49965-7',
'51866-2',
'30245-5',
'10682-3',
'50624-6',
'79155-8',
'83101-6',
'53825-6',
'38998-1',
'59052-1',
'25841-8',
'34699-9',
'25842-6',
'81246-1',
'48559-9',
'72560-6',
'72559-8',
'49657-0',
'49662-0',
'49658-8',
'49663-8',
'73695-9',
'49660-4',
'49665-3'
);
--(213490 rows affected)
--(284385 rows affected)

-- *******************************************************************************************************
-- STEP 1.1
--          Create lab driver table
--
-- *******************************************************************************************************
CREATE TABLE #xdr_ford_labdrv(
	lab_concept_code [varchar](18) NULL,
	lab_concept_name [varchar](150) NULL,
	result_concept_code [varchar](100) NULL,
	threshold [varchar](100) NULL);
	INSERT INTO #xdr_ford_labdrv
VALUES('20447-9','HIV 1 RNA [#/volume] (viral load) in Serum or Plasma by Probe and target amplification method',NULL,'20')
		,('29541-0','HIV 1 RNA [Log #/volume] (viral load) in Plasma by Probe and target amplification method',NULL,'1.3')
		,('29893-5','HIV 1 Ab [Presence] in Serum or Plasma by Immunoassay','260385009','positive/negative')
		,('30361-0','HIV 2 Ab [Presence] in Serum or Plasma by Immunoassay',NULL,'positive/negative')
		,('41497-9','HIV 1 RNA [Log #/volume] (viral load) in Cerebral spinal fluid by Probe and target amplification method',NULL,'1.3')
		,('41498-7','HIV 1 RNA [#/volume] (viral load) in Cerebral spinal fluid by Probe and target amplification method',NULL,'20')
		,('44871-2','HIV 1 proviral DNA [Presence] in Blood by Probe and target amplification method','10828004','positive/negative')
		,('48558-1','HIV genotype [Susceptibility]','260385009','positive/negative')
		,('5221-7','HIV 1 Ab [Presence] in Serum or Plasma by Immunoblot',NULL,'positive/negative')
		,('56888-1','HIV 1+2 Ab+HIV1 p24 Ag [Presence] in Serum or Plasma by Immunoassay',NULL,'Reactive/non reactive')
		,('7918-6','HIV 1+2 Ab [Presence] in Serum','131194007','Reactive/non reactive')
		,('9665-1','HIV 1 p24 Ag [Units/volume] in Serum',NULL,'positive/negative');

select count(*) from #xdr_ford_labdrv;

select top 100 * from #xdr_ford_labs;
SELECT COUNT(*), COUNT(DISTINCT Person_ID)  FROM  #xdr_ford_labs;--284385	191374

-- *******************************************************************************************************
-- STEP 
--          Harmonize lab results to apply HIV algorithm
--
-- *******************************************************************************************************
--select count(*), count(distinct x.person_id) from (
drop table #XDR_FORD_HIVLAB
SELECT DISTINCT lab.measurement_date, lab.person_id
INTO #XDR_FORD_HIVLAB
FROM #xdr_ford_labs		lab
JOIN #xdr_ford_labdrv	drv on lab.lab_concept_code = drv.lab_concept_code
WHERE
	(drv.threshold = 'positive/negative' and lab.value_concept_name = 'Positive')
	OR
	(drv.threshold = 'Reactive/non reactive' and lab.value_concept_name = 'Reactive')
	OR
	(drv.threshold = '20' and 
							(lab.value_as_number > 20 or lab.value_concept_name = 'Positive' ))
	OR
	(drv.threshold = '1.3' and 
							(lab.value_as_number > 1.3 OR lab.value_concept_name = 'Positive'))
--)x	
;
--(5912 rows affected)

select count(*), count(distinct person_id) from #XDR_FORD_HIVLAB;--5912	2179		7677	2626

----------------------------------------------------------------------------
--Step 2.5:     Create final cohor table
----------------------------------------------------------------------------
drop TABLE #XDR_FORD_COH;
CREATE TABLE #XDR_FORD_COH (
PERSON_ID [varchar](18) NULL,
first_hiv_dx_date DATE,
first_hiv_lab_date DATE,
COHORT_TYPE [varchar](18) NULL
);
----------------------------------------------------------------------------
--Step 2.4:     Flag positive HIV patients based on DX, or DX AND lab results
----------------------------------------------------------------------------
INSERT INTO #XDR_FORD_COH(person_id, first_hiv_dx_date,COHORT_TYPE, first_hiv_lab_date)
select DISTINCT dx.person_id
        ,dx.FIRST_HIV_DATE as first_hiv_dx_date
        ,CASE WHEN lab.person_id is null then 'ONLY DX' 
                else 'DX + LAB'
                END COHORT_TYPE
        ,lab.first_hiv_lab_date
from #XDR_FORD_DX_HIV_coh    dx
LEFT JOIN (select person_id
                ,MIN(measurement_date) AS first_hiv_lab_date
            from #XDR_FORD_HIVLAB
            group by person_id) lab on dx.person_id = lab.person_id
;--(3262 rows affected)

SELECT COUNT(*), COUNT(DISTINCT person_id)  FROM #XDR_FORD_COH;--3262	3262
----------------------------------------------------------------------------
--Step 2.4:     Flag positive HIV patients based on lab results ONLY
----------------------------------------------------------------------------
INSERT INTO #XDR_FORD_COH(person_id, COHORT_TYPE, first_hiv_lab_date)
select lab.person_id
                ,'LAB ONLY' AS COHORT_TYPE
                ,MIN(lab.measurement_date) AS first_hiv_lab_date
            from #XDR_FORD_HIVLAB       lab
            LEFT JOIN #XDR_FORD_COH          coh on lab.person_id = coh.person_id
            WHERE COH.person_id IS NULL
            group by lab.person_id;
			--(461 rows affected)		(804 rows affected)
SELECT COUNT(*), COUNT(DISTINCT person_id)  FROM #XDR_FORD_COH;--3723	3723		4066	4066
SELECT COHORT_TYPE,COUNT(*) as tot_count, COUNT(DISTINCT person_id) as pat_count  FROM #XDR_FORD_COH group by COHORT_TYPE ;
/*
OMOP COUNTS
--------------------------------------
COHORT_TYPE	tot_count	pat_count
DX + LAB	1,718		1,718
LAB ONLY	  461			  461
ONLY DX		1,544		1,544
TOTAL COUNT 3,613
DX + LAB or ONLY DX = 3,262

CLARITY COUNTS
--------------------------------------
COHORT_TYPE		pat_count
DX + LAB		  834
LAB ONLY		  150
ONLY DX			2,687	
TOTAL PAT COUNT 3,701
DX + LAB or ONLY DX = 3,551
*/


SELECT * FROM #XDR_FORD_COH where cohort_type = 'LAB ONLY' order by person_id;--4066	4066