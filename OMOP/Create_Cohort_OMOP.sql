-- dx DRIVER
SELECT [concept_id]
      ,[concept_name]
      ,[domain_id]
      ,[vocabulary_id]
      ,[concept_class_id]
      ,[standard_concept]
      ,[concept_code]
      ,[valid_start_date]
      ,[valid_end_date]
      ,[invalid_reason]
into #XDR_FORST_DXDRV
FROM  [OMOP].[dbo].[concept]
WHERE 
  (
  (concept_code LIKE 'B20%' AND VOCABULARY_ID = 'ICD10')
  OR
  (concept_code LIKE '042%' AND VOCABULARY_ID = 'ICD9CM'
  )
  )
--  AND VOCABULARY_ID NOT IN ( 'ICD10', 'ICD10CM','SNOMED','ICDO3', 'ICD9CM')
  and domain_id = 'Condition';
  SELECT * FROM #XDR_FORST_DXDRV;


  --drop table #XDR_FORD_DX_HIV_coh;
SELECT con.person_id
		,min(con.condition_start_date) AS FIRST_HIV_DATE
INTO #XDR_FORD_DX_HIV_coh
FROM [OMOP].[dbo].[CONDITION_occurrence] con
JOIN #XDR_FORST_DXDRV			dx on con.condition_concept_id = dx.concept_id
WHERE 
	con.condition_start_date BETWEEN '03/02/2013' AND '02/28/2018'
	--AND OB.observation_concept_id = 45571458
GROUP BY con.person_id;
--(3262 rows affected)

SELECT COUNT(*), COUNT(DISTINCT PERSON_ID) FROM #XDR_FORD_DX_HIV_coh;--3262	3262

