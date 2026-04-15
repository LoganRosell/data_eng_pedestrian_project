---------------------------------------------------------------------------
-- Create census_pops_imputed                                                                                                                                                                                                                                                                           
---------------------------------------------------------------------------

DELETE FROM census_city_pops 
WHERE census_state_id = '37' 
  AND census_place_id = '01520';

DROP TABLE IF EXISTS census_pops_imputed;

CREATE TABLE census_pops_imputed (
  city_year_id TEXT PRIMARY KEY,    
  year INTEGER,
  city_name VARCHAR(100),
  census_state_id CHAR(2),
  census_place_id CHAR(5),
  census_pop_imputed NUMERIC
);

INSERT INTO census_pops_imputed (
    city_year_id, 
    year, 
    city_name, 
    census_state_id, 
    census_place_id, 
    census_pop_imputed
)
WITH year_range AS (
  SELECT MIN(year) AS min_year, MAX(year) AS max_year
  FROM census_city_pops
), 
year_series AS (
  SELECT generate_series(min_year, max_year, 1) AS year_slot
  FROM year_range
),
unique_cities AS (
  SELECT DISTINCT census_city, census_state_id, census_place_id 
  FROM census_city_pops
),
joined_data AS (
  SELECT
    y.year_slot AS year,
    uc.census_city,
    uc.census_state_id,
    uc.census_place_id,
    c.census_pop
  FROM year_series y
  CROSS JOIN unique_cities uc
  LEFT JOIN census_city_pops c 
    ON y.year_slot = c.year 
    AND uc.census_place_id = c.census_place_id
    AND uc.census_state_id = c.census_state_id
)
SELECT 
  census_state_id || census_place_id || year::TEXT AS city_year_id,
  year, 
  census_city,
  census_state_id,
  census_place_id,
  CASE 
    WHEN year = 2020 THEN 
      (LAG(census_pop) OVER (PARTITION BY census_city ORDER BY year) + 
       LEAD(census_pop) OVER (PARTITION BY census_city ORDER BY year)) / 2
    ELSE census_pop
  END AS census_pop_imputed
FROM joined_data;