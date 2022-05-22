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




## LEAD exaample 

DECLARE country STRING DEFAULT "Vietnam";
DECLARE start_date datetime DEFAULT '2022-01-01';
DECLARE end_date datetime DEFAULT '2022-01-31'; 

SELECT date ,confirmed as cummulative_confirmed
,LEAD(confirmed, 1) OVER ( ORDER BY date) AS next_cummulative_day    
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country
ORDER BY 2 

## LAG exaample 
DECLARE country STRING DEFAULT "Vietnam";
DECLARE start_date datetime DEFAULT '2022-01-01';
DECLARE end_date datetime DEFAULT '2022-01-31'; 

SELECT date ,confirmed as cummulative_confirmed
,LAG(confirmed, 1) OVER ( ORDER BY date) AS last_cummulative_day    
,confirmed - IFNULL(LAG(confirmed, 1) OVER ( ORDER BY date),confirmed) AS new_confirmed    
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country
ORDER BY 2 ;

-- FIRST VALUE 
DECLARE country STRING DEFAULT "Vietnam";
DECLARE start_date datetime DEFAULT '2022-01-01';
DECLARE end_date datetime DEFAULT '2022-01-31'; 

SELECT date ,confirmed as cummulative_confirmed
,FIRST_VALUE(confirmed) OVER ( ORDER BY date) AS confirmed_firstDayOfMonth    
,confirmed - FIRST_VALUE(confirmed) OVER (ORDER BY date) AS diff_firstDayofMonth    
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country
ORDER BY 2 ;

-- LAST VALUE 
DECLARE country STRING DEFAULT "Vietnam";
DECLARE start_date datetime DEFAULT '2022-01-01';
DECLARE end_date datetime DEFAULT '2022-01-31'; 

SELECT date ,confirmed as cummulative_confirmed    
 ,LAST_VALUE(confirmed) OVER(
        ORDER BY date
         RANGE BETWEEN 
            UNBOUNDED PRECEDING AND 
            UNBOUNDED FOLLOWING
    ) confirmed_lastDayOfMonth
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country
ORDER BY 2 ;

SELECT 
 LAST_VALUE(confirmed) OVER(
        ORDER BY date
         RANGE BETWEEN 
            UNBOUNDED PRECEDING AND 
            UNBOUNDED FOLLOWING
    ) confirmed_lastDayOfMonth
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE date between start_date and end_date
AND country_region = country
ORDER BY 2 ;
-- ROW NUMBER 
WITH history_order as (
SELECT * 
FROM  UNNEST(
  [struct(1 as customer_id,"apple" as product_id,date('2022-01-01') as  order_time ,1 AS quantity)
  ,(1,'banana',date '2022-02-01',2),(2,'banana',date '2022-02-01',4),(3,'apple',date '2022-01-02',2)
  ,(2,'banana',date '2022-01-02',2),(1,'apple',date '2022-01-03',4),(3,'banana',date '2022-01-04',2)
  ,(1,'apple',date '2022-02-02',2)]
  )
)
,firstProduct_customer as (  
SELECT 
customer_id,product_id,order_time 
,row_number() over (PARTITION BY customer_id,product_id ORDER BY order_time) as row_num
FROM history_order
)

SELECT * FROM firstProduct_customer

## RANK THE

,rankQuantity as (  
SELECT 
customer_id,product_id,quantity 
,RANK() over (PARTITION BY customer_id ORDER BY quantity DESC ) as rankQuant
FROM history_order
)
## highest quantity by each customer
SELECT * 
FROM rankQuantity
WHERE  rankQuant = 1