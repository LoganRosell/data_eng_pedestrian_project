-- fars migration 
SELECT * FROM fars_staging
LIMIT 5;

---- create normalized tables

create table weather_conditions (
  condition_id bigserial primary key,
  city_id int not null references cities,
  date date,
  wind_speed_max float,
  rainfall float,
  snowfall float,
  visibility int,
  lighting_condition text,
  precipitation_sum float,
  temp_max float,
  temp_min float,
  precipitation_hours float
);


-- not sure if we need this actually
-- create table roads (
--   road_id bigserial primary key,
--   road_characteristics text,
--   surface_condition text,
--   speed_limit int,
--   road_alignment int,
--   num_lanes int
-- );

drop table if exists incidents;
create table incidents (
  case_num int primary key,
  city_id int references cities,
  longitude float,
  latitude float,
  date date,
  time time,
  impact_speed_mph int,
  drunk_driver bool,
  fatalities int,
  peds int,
  fars_lighting text,
  fars_weather text,
  fars_weather2 text,
  rel_roadway text
);

-- alter table cities
-- add column fips_state_id int;
-- add column FIPS_place int,
-- add column viz_join_year int,
-- add column longitude float,
-- add column latitude float,
-- add column elevation float;

SELECT * FROM cities LIMIT 10;

-----------------------------------------------------------

-- create backup
DROP TABLE IF EXISTS fars_backup CASCADE;
CREATE TABLE fars_backup AS
SELECT * FROM fars_staging;

-- drop cities we don't need from the city table
BEGIN TRANSACTION;

-- cities under population threshold
WITH drop_cities AS (
	SELECT 
		c.city
		-- , p.census_city
	FROM cities AS c
	FULL OUTER JOIN census_city_pops AS p
		ON p.census_place_id::numeric = c.fips_place
	WHERE census_city IS NULL
)
DELETE FROM cities AS c
USING drop_cities AS d
WHERE c.city = d.city;

COMMIT;

-- migrate fars data into incidents
BEGIN TRANSACTION;

-- columns in FARS that should move directly to incidents:
-- st_case
-- cityname
-- datetime 
-- latitude
-- longitude
-- lgt_condition
-- weather1name
-- weather2name
-- fatals
-- drunk_dr

-- only move rows where: 
-- relevant cities (plus state 11, Washington DC)
-- fatals > 0 
-- involving a pedestrian, where peds > 0

-- first the citiies that aren't Washington DC...
INSERT INTO incidents (
  case_num,
  city_id,
  longitude,
  latitude,
  date,
  time,
  drunk_driver,
  fatalities,
  peds,
  fars_lighting,
  fars_weather,
  fars_weather2,
  rel_roadway
)
SELECT 
	CONCAT(f.caseyear, f.st_case)::int AS year_st_case,
	c.city_id,
	f.latitude,
	f.longitud AS longitude,
	make_date(f.year, f.month, f.day) AS date,
	CASE WHEN f.hour < 24
		THEN 
		make_time(f.hour, f.minute, 0.0) 
		ELSE NULL
	END AS time,
	CASE WHEN f.drunk_dr <> 0 
		THEN TRUE 
		ELSE FALSE 
	END AS drunk_driver,
	f.fatals,
	f.peds,
	f.lgt_condname AS fars_lightinng,
	f.weather1name AS fars_weather,
	f.weather2name AS fars_weather2,
	f.rel_roadname AS rel_roadway
FROM fars_staging as f
JOIN cities as c
	ON c.city = INITCAP(f.cityname)
WHERE 
	fatals > 0 AND peds > 0;

-- ...then washington DC
INSERT INTO incidents (
  case_num,
  city_id,
  longitude,
  latitude,
  date,
  time,
  drunk_driver,
  fatalities,
  peds,
  fars_lighting,
  fars_weather,
  fars_weather2,
  rel_roadway
)
SELECT 
	CONCAT(f.caseyear, f.st_case)::int AS year_st_case,
	c.city_id,
	f.latitude,
	f.longitud AS longitude,
	make_date(f.year, f.month, f.day) AS date,
	CASE WHEN f.hour < 24
		THEN 
		make_time(f.hour, f.minute, 0.0) 
		ELSE NULL
	END AS time,
	CASE WHEN f.drunk_dr <> 0 
		THEN TRUE 
		ELSE FALSE 
	END AS drunk_driver,
	f.fatals,
	f.peds,
	f.lgt_condname AS fars_lightinng,
	f.weather1name AS fars_weather,
	f.weather2name AS fars_weather2,
	f.rel_roadname AS rel_roadway
FROM fars_staging as f
JOIN cities as c
	ON fips_state_id::numeric = f.state::numeric
WHERE 
	f.state = 11 
	AND (f.fatals > 0 AND f.peds > 0);
	
SELECT COUNT(*) FROM incidents;
-- 7539

COMMIT;

SELECT * FROM incidents LIMIT 15;



































