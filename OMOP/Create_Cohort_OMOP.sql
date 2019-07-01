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
--(3262 rows affected)

SELECT COUNT(*), COUNT(DISTINCT PERSON_ID) FROM #XDR_FORD_DX_HIV_coh;--3262	3262

-- *******************************************************************************************************
-- STEP 1.1
--          Create temp table with patients with a HIV labs from '03/02/2013' AND '02/28/2018'
--
-- *******************************************************************************************************
select
from concept
where
