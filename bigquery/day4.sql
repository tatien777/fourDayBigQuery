-- first cte - remembering "With cte1 as()," 
with country_population as (
SELECT
    IF(country = "United States","US",IF(country="Iran, Islamic Rep.","Iran",country)) AS country,
    year_2018 as population 
   FROM
    `bigquery-public-data.world_bank_global_population.population_by_country`
),
-- second cte - remembering "cte2 as()" 
country_covid_confirmed as (
SELECT country_region, DATE_TRUNC(DATE, MONTH) AS month ,max(confirmed) as confirmed 
FROM  `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between '2022-01-01' and '2022-01-31'
GROUP BY 1,2
)

-- join 2 cte 
SELECT pop.country,confirm.month,confirm.confirmed,pop.population,
confirm.confirmed/pop.population  * 100 as infection_over_population  
FROM country_population pop JOIN
country_covid_confirmed confirm ON confirm.country_region = pop.country
ORDER BY 5 DESC 

-- Declare 
DECLARE country STRING DEFAULT "US";
DECLARE start_date datetime DEFAULT '2022-01-01';
DECLARE end_date datetime DEFAULT '2022-01-31'; 


SELECT country_region,date ,confirmed    
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country


-- temporaty table 
CREATE TEMP TABLE covid_confirmed
  AS
  SELECT country_region, DATE_TRUNC(DATE, MONTH) AS month ,max(confirmed) as confirmed 
  FROM  `bigquery-public-data.covid19_jhu_csse.summary`
  WHERE date between start_date and end_date
  GROUP BY 1,2
  ;
  -- Update 
  DROP TABLE IF EXISTS covid_confirmed  ;

  -- character 
CREATE TEMPORARY FUNCTION FiveFirstChar(word STRING)
  RETURNS STRING
  AS (SUBSTR(word, 0, 5));

SELECT 
  FiveFirstChar(name) AS five_chars_name, 
FROM  UNNEST(["David Ta"]) as name
# using js 

CREATE TEMPORARY FUNCTION js_FindFiveChars(word STRING)
  RETURNS STRING
  LANGUAGE js
  AS "return word.substring(0, 5);";


SELECT 
  js_FindFiveChars(name) AS five_chars_name, 
FROM  UNNEST(["David Ta"]) as name

-- https://stackoverflow.com/questions/47795464/using-lead-in-bigquery


DECLARE country STRING DEFAULT "Vietnam";
DECLARE start_date datetime DEFAULT '2022-01-01';
DECLARE end_date datetime DEFAULT '2022-01-31'; 

## LEAD exaample 

SELECT date ,confirmed as cummulative_confirmed
,LEAD(confirmed, 1) OVER ( ORDER BY date) AS next_cummulative_day    
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country
ORDER BY 2 

## LAG exaample 
SELECT date ,confirmed as cummulative_confirmed
,LAG(confirmed, 1) OVER ( ORDER BY date) AS last_cummulative_day    
,confirmed - IFNULL(LAG(confirmed, 1) OVER ( ORDER BY date),confirmed) AS new_confirmed    
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country
ORDER BY 2 ;