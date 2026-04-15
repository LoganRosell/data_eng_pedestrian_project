-- incident EDA

SELECT COUNT(*) FROM incidents; -- 7539

-- count of incidents by city
SELECT
	c.city,
	c.state,
	COUNT(i.city_id) AS count
FROM incidents AS i
JOIN cities AS c
	ON i.city_id = c.city_id
GROUP BY c.city, c.state
ORDER BY count DESC;

-- per-capita incidents
WITH counts AS (
	SELECT
		c.city,
		c.state,
		ccp.census_pop AS population,
		COUNT(i.city_id) AS incident_count
	FROM incidents AS i
	JOIN cities AS c
		ON i.city_id = c.city_id
	JOIN census_city_pops AS ccp
		ON ccp.census_place_id = c.fips_place
		AND ccp.census_state_id = c.fips_state_id
	WHERE ccp.year = 2023
	GROUP BY c.city, c.state, ccp.census_pop
	ORDER BY incident_count DESC
)
SELECT 
	*,
	incident_count / population AS per_capita_incidents
FROM counts;

-- day of week counts
SELECT 
	EXTRACT('dow' FROM date) AS dow,
	COUNT(case_num) AS count
FROM incidents
GROUP BY dow
ORDER BY dow;

-- weekend vs weekday counts
SELECT 
	CASE WHEN EXTRACT('dow' FROM date) = 0 
			  OR EXTRACT('dow' FROM date) = 6 
		THEN 'weekend'
	ELSE 'weekday'
	END AS dow,
	COUNT(case_num) AS count
FROM incidents
GROUP BY dow;

-- distributions of incidents by date (month/day for all years) across cities (potential vizualization)












-- number of incidents over time across cities (potential vizualization)
-- statistical analysis (correlation, regressions) between variables


