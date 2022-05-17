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