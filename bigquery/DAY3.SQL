-- numbering function 

-- min max 
SELECT min(date) as min_date ,max(date) as max_date
FROM `bigquery-public-data.covid19_jhu_case_summary`
WHERE TRUE 
AND date between date_sub(current_date()) and date_sub(current_date(),INTERVAL 1 month) and curr_date()

-- avg, mean 
SELECT avg(x) as mean 
FROM UNNEST([0,3,NULL,1,2]) as x 
LIMIT 1 

--  percentile 
SELECT  percentil_cont(x,0.25) as percentile25,
        percentil_cont(x,0.5) as percentile50,
        percentil_cont(x,0.75) as percentile75,
FROM UNNEST([0,3,NULL,1,2]) as x 
LIMIT 1 

-- mod & round
SELECT MOD(50,26) as mod_val,
        ROUND(50/26,2) as round_val

-- length, start_with/end_with, trim 
SELECT legth(str) as len_str,
        start_with(str,"s") as strart_str ,
        end_with(str,"s") as end_str,
        trim(str) as trim_str,
FROM UNNEST(["example..."]) as str

-- substring, replace, concat , split
SELECT 
substring(str,2,3) as substring , # from the 2rd character, taking 3 characters 
replace(str,"e","s") as repstring , # replace e character by s character
concat(str,"CONCAT") as concatString  # concat 2 strings 
split(str,"___") as splitString  # split the characters into 2 parts  
FROM UNNEST(["example..."]) as str

-- aggerate functions 


-- COUNT 
SELECT 
DATE_SUB(current_date(),interval 1 month) as start_date, 
current_date() as end_date, 
count(date) as all_values,
count(distinct (date)) as unique_values
FROM `bigquery-public-data.covid19_jhu_csse.summary`
WHERE TRUE 
AND date between DATE_SUB(current_date(),interval 1 month) and current_date()
GROUP BY 1,2
-- SUM
SELECT 
SUM(x) as sum, 
SUM(distinct(x)) as sum_distinct 
FROM UNNEST([1,2,3,4,null,5,4,3,1,null]) as str

## contional functions 
SELECT 
COALESCE('A','B','C') as res1, -- show the first value of it's not null  
COALESCE(null,'B','C') as res2, -- not selecting null
COALESCE(cast(x as string),'g') as res3, -- replace null value by "g" 
FROM UNNEST([1,null,2,3]) as x 

## IFNULL 
SELECT 
COALESCE(cast(x as string),'g') as coalesce_res 
IFNULL(cast(x as string),'g') as ifnull_res # same result 
FROM UNNEST([1,null,2,3]) as x 

## CASE WHEN AND IF
SELECT CASE WHEN fruit = 'banana' then weight * 10 else weight *15 END as total_price_case,
if (fruit = 'banana' , weight * 10 , weight *15) END as total_price_if,
COUNT (CASE WHEN fruit = 'apple' then weight END ) END as count_apple,
SUM (CASE WHEN fruit = 'apple' then weight ELSE 0 END ) END as sum_apple_weight,
FROM UNNEST([struct("apple" as fruit, 1 AS weight),('banana',2),('banana',4),('apple',2)])
GROUP BY 1,2

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