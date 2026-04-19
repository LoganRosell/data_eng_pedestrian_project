-- railway public url: postgresql://postgres:qpLvYmayXdYcSkhrAbbLxYAYdQORXeXU@monorail.proxy.rlwy.net:26251/railway

-----------------------------------------------------------
-- FULL PIPELINE
-- overview:
-- 0. 
-- 1. create staging tables 
-- 2. import data from sources into staging tables
-- 3. create normalized tables
-- 4. migrate data from staging tables into normalized tables
-- 5. perform checks to validate normalized data
-----------------------------------------------------------

BEGIN TRANSACTION;

-- 1. staging tables

DROP TABLE IF EXISTS fars_staging;
CREATE TABLE IF NOT EXISTS fars_staging
(
    caseyear integer,
    state integer,
    st_case integer,
    county integer,
    cityname character varying(255),
    day integer,
    month integer,
    year integer,
    hour integer,
    minute integer,
    latitude double precision,
    longitud double precision,
    lgt_condname character varying(255),
    weather1name character varying(255),
    weather2name character varying(255),
    fatals integer,
    drunk_dr double precision,
    peds integer,
    routename character varying(255),
    rur_urbname character varying(255),
    tway_id character varying(255),
    harm_evname character varying(255),
    reljct2name character varying(255),
    typ_intname character varying(255),
    rel_roadname character varying(255)
);

DROP TABLE IF EXISTS states_lookup;
CREATE TABLE IF NOT EXISTS states_lookup (
 abbr char(2),
 state_name varchar(50)
);

INSERT INTO states_lookup VALUES
('AL','ALABAMA'),
('AK','ALASKA'),
('AB','ALBERTA'),
('AS','AMERICAN SAMOA'),
('AZ','ARIZONA'),
('AR','ARKANSAS'),
('BC','BRITISH COLUMBIA'),
('CA','CALIFORNIA'),
('PW','CAROLINE ISLANDS'),
('CO','COLORADO'),
('CT','CONNECTICUT'),
('DE','DELAWARE'),
('DC','DISTRICT OF COLUMBIA'),
('FM','FEDERATED STATE'),
('FL','FLORIDA'),
('GA','GEORGIA'),
('GU','GUAM'),
('HI','HAWAII'),
('ID','IDAHO'),
('IL','ILLINOIS'),
('IN','INDIANA'),
('IA','IOWA'),
('KS','KANSAS'),
('KY','KENTUCKY'),
('LA','LOUISIANA'),
('ME','MAINE'),
('MB','MANITOBA'),
('MP','MARIANA ISLANDS'),
('MH','MARSHALL ISLANDS'),
('MD','MARYLAND'),
('MA','MASSACHUSETTS'),
('MI','MICHIGAN'),
('MN','MINNESOTA'),
('MS','MISSISSIPPI'),
('MO','MISSOURI'),
('MT','MONTANA'),
('NE','NEBRASKA'),
('NV','NEVADA'),
('NB','NEW BRUNSWICK'),
('NH','NEW HAMPSHIRE'),
('NJ','NEW JERSEY'),
('NM','NEW MEXICO'),
('NY','NEW YORK'),
('NF','NEWFOUNDLAND'),
('NC','NORTH CAROLINA'),
('ND','NORTH DAKOTA'),
('NT','NORTHWEST TERRITORIES'),
('NS','NOVA SCOTIA'),
('NU','NUNAVUT'),
('OH','OHIO'),
('OK','OKLAHOMA'),
('ON','ONTARIO'),
('OR','OREGON'),
('PA','PENNSYLVANIA'),
('PE','PRINCE EDWARD ISLAND'),
('PR','PUERTO RICO'),
('PQ','QUEBEC'),
('RI','RHODE ISLAND'),
('SK','SASKATCHEWAN'),
('SC','SOUTH CAROLINA'),
('SD','SOUTH DAKOTA'),
('TN','TENNESSEE'),
('TX','TEXAS'),
('UT','UTAH'),
('VT','VERMONT'),
('VI','VIRGIN ISLANDS'),
('VA','VIRGINIA'),
('WA','WASHINGTON'),
('WV','WEST VIRGINIA'),
('WI','WISCONSIN'),
('WY','WYOMING'),
('YT','YUKON TERRITORY'),
('AE','ARMED FORCES - EUROPE'),
('AA','ARMED FORCES - AMERICAS'),
('AP','ARMED FORCES - PACIFIC'),
('DC', 'WASHINGTON DC');

DROP TABLE IF EXISTS us_geocodes;
CREATE TABLE IF NOT EXISTS us_geocodes (
  usps varchar(50),
  geoid numeric PRIMARY KEY,
  geoidfq varchar(50),
  ansicode numeric,
  name text,
  lsad varchar(50),
  funcstat varchar(10),
  aland numeric,
  awater numeric,
  aland_sqmi numeric,
  awater_sqmi numeric,
  lat numeric,
  lon numeric
);

DROP TABLE IF EXISTS weather_data_staging_table;
CREATE TABLE IF NOT EXISTS weather_data_staging_table (
  location_id integer,
  date date,
  weather_code integer,
  precipitation_sum_inches numeric,
  rain_sum_inches numeric,
  snowfall_sum_inches numeric,
  precipitation_hours integer,
  temperature_2m_min_f numeric,
  temperature_2m_max_f numeric,
  wind_speed_10m_max_mph numeric
);

DROP TABLE IF EXISTS weather_data_location_lookup;
CREATE TABLE IF NOT EXISTS weather_data_location_lookup (
  location_id integer PRIMARY KEY,
  latitude numeric,
  longitude numeric,
  elevation integer,
  utc_offset_seconds integer,
  timezone varchar(10),
  timezone_abbreviation varchar(10)
);

-- census_id,census_city,census_pop,fips_state_id,fips_place_id
DROP TABLE IF EXISTS census_city_pops;
CREATE TABLE IF NOT EXISTS census_city_pops
(
    year integer,
    census_city character varying(100),
    census_pop integer,
    fips_state_id character varying(2),
    fips_place_id character varying(5)
);

DROP TABLE IF EXISTS cities CASCADE;
CREATE TABLE IF NOT EXISTS cities
(
    city_id integer,
    city text,
    state text,
    county text,
    state_abbr character(2) ,
    fips_place_id character varying(5),
    viz_join_year integer,
    elevation double precision,
    fips_state_id character varying(2),
    CONSTRAINT cities_pkey PRIMARY KEY (city_id)
);

COMMIT;
  
-----------------------------------------------------------

BEGIN TRANSACTION;

-- 2. import data
-- run copy commands in terminal with psql if you run into errors

-- weather data
\COPY us_geocodes FROM '/tmp/2025_Gaz_place_national.txt' WITH (FORMAT CSV, HEADER, DELIMITER '|');
\COPY weather_data_staging_table FROM '/tmp/daily_weather_data.csv' WITH (FORMAT CSV, HEADER);
\COPY weather_data_location_lookup FROM '/tmp/daily_weather_data_location_lookup.csv' WITH (FORMAT CSV, HEADER);

-- census data
\COPY census_city_pops FROM '/tmp/census_city_pops.csv' WITH (FORMAT CSV, HEADER);

-- city data (created by us)
-- the city table is kind of like a normalized staging table
\COPY cities FROM '/tmp/vz_communities.csv' WITH (FORMAT CSV, HEADER);

-- FARS data
\COPY fars_staging FROM '/tmp/FARS_import.csv' WITH (FORMAT CSV, HEADER);

COMMIT;

-- -----------------------------------------------------------

-- -- 3. create normalized tables

BEGIN TRANSACTION;

-- fill in state abbreviation table
UPDATE cities AS c
SET state_abbr = sl.abbr
FROM states_lookup AS sl
WHERE UPPER(c.state) = sl.state_name;

COMMIT;

BEGIN TRANSACTION;

DROP TABLE IF EXISTS weather_conditions CASCADE;
CREATE TABLE weather_conditions (
  condition_id bigserial primary key,
  city_id int not null references cities,
  date date,
  wind_speed_max float,
  rainfall float,
  snowfall float,
  precipitation_sum float,
  temp_max float,
  temp_min float,
  precipitation_hours float
);

DROP TABLE IF EXISTS incidents CASCADE;
CREATE TABLE incidents (
  case_num int primary key,
  city_id int references cities,
  condition_id int references weather_conditions(condition_id),
  incident_latitude float,
  incident_longitude float,
  date date,
  time time,
  -- impact_speed_mph int, -- may add later
  drunk_driver bool,
  fatalities int,
  peds int,
  fars_lighting text,
  fars_weather text,
  fars_weather2 text,
  rel_roadway text
);

DROP TABLE IF EXISTS lat_lon_city_lookup CASCADE;
CREATE TABLE IF NOT EXISTS lat_lon_city_lookup (
  geocode_lat numeric,
  geocode_lon numeric,
  weather_lat numeric,
  weather_lon numeric,
  city_id integer REFERENCES cities (city_id),
  location_id integer
);

COMMIT;

-- -----------------------------------------------------------

-- 4. migrate data from staging tables into normalized tables

-- create census_pops_imputed
-- with populations from 2020 imputed from 2019 and 2021 data

BEGIN TRANSACTION;

DELETE FROM census_city_pops 
WHERE fips_state_id = '37' 
  AND fips_place_id = '01520';

DROP TABLE IF EXISTS census_pops_imputed;
CREATE TABLE census_pops_imputed (
  city_year_id TEXT PRIMARY KEY,    
  year INTEGER,
  city_name VARCHAR(100),
  fips_state_id CHAR(2),
  fips_place_id CHAR(5),
  census_pop_imputed NUMERIC
);

INSERT INTO census_pops_imputed (
    city_year_id, 
    year, 
    city_name, 
    fips_state_id, 
    fips_place_id, 
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
  SELECT DISTINCT census_city, fips_state_id, fips_place_id 
  FROM census_city_pops
),
joined_data AS (
  SELECT
    y.year_slot AS year,
    uc.census_city,
    uc.fips_state_id,
    uc.fips_place_id,
    c.census_pop
  FROM year_series y
  CROSS JOIN unique_cities uc
  LEFT JOIN census_city_pops c 
    ON y.year_slot = c.year 
    AND uc.fips_place_id = c.fips_place_id
    AND uc.fips_state_id = c.fips_state_id
)
SELECT 
  fips_state_id || fips_place_id || year::TEXT AS city_year_id,
  year, 
  census_city,
  fips_state_id,
  fips_place_id,
  CASE 
    WHEN year = 2020 THEN 
      (LAG(census_pop) OVER (PARTITION BY census_city ORDER BY year) + 
       LEAD(census_pop) OVER (PARTITION BY census_city ORDER BY year)) / 2
    ELSE census_pop
  END AS census_pop_imputed
FROM joined_data;

COMMIT;

-- drop cities under population threshold
BEGIN TRANSACTION;

-- cities that do not appear in census_pop_imputed are smaller than 65000
WITH drop_cities AS (
    SELECT 
		c.city
	FROM cities AS c
	FULL OUTER JOIN census_pops_imputed AS p
		ON p.fips_place_id = c.fips_place_id
	WHERE p.city_name IS NULL
)
DELETE FROM cities AS c
USING drop_cities AS d
WHERE c.city = d.city;

SELECT COUNT(*) FROM cities;

COMMIT;

-- FARS migration to incident table

BEGIN TRANSACTION;

-- only move rows where: 
-- relevant cities (plus state 11, Washington DC)
-- fatals > 0 
-- involving a pedestrian, where peds > 0

-- first the citiies that aren't Washington DC...
INSERT INTO incidents (
  case_num,
  city_id,
  incident_latitude,
  incident_longitude,
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
  incident_latitude,
  incident_longitude,
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
	
COMMIT;

-- normalize weather tables

-- add city ids to lat_long lookup
BEGIN;

INSERT INTO lat_lon_city_lookup(city_id)
  SELECT city_id
  FROM cities;

COMMIT;

-- add geocode lon/lat, weather station lon/lat, and location_id to lat_lon_city_lookup

BEGIN;

WITH weather_geocode_unified AS (
  WITH city_names_table AS (
  WITH RankedMatches AS (
      SELECT 
          c.city_id,
          c.city AS vision_zero_city_name,
          c.state,
          c.county,
          c.state_abbr,
          g.geoid,
          g.name AS geocode_city_name,
          g.lat AS geocode_lat,
          g.lon AS geocode_lon,
          ROW_NUMBER() OVER (
              PARTITION BY c.city, c.state_abbr 
              ORDER BY (CASE WHEN g.funcstat = 'A' THEN 1 ELSE 2 END) ASC
          ) as priority
      FROM cities c
      LEFT JOIN us_geocodes g
        ON (UPPER(g.name) LIKE UPPER(REPLACE(c.city, ' DC', '')) || '%')
          AND (g.usps = c.state_abbr)
  )
  SELECT *
  FROM RankedMatches
  WHERE priority = 1
)
SELECT 
    cnt.city_id,
    cnt.vision_zero_city_name,
    cnt.geocode_city_name,
    cnt.geocode_lat,
    cnt.geocode_lon,
    weather_loc.location_id,
    weather_loc.latitude AS weather_lat,
    weather_loc.longitude AS weather_lon
FROM city_names_table AS cnt
CROSS JOIN LATERAL (
      SELECT 
          *
      FROM weather_data_location_lookup wll
      ORDER BY 
          ST_SetSRID(ST_MakePoint(cnt.geocode_lon::double precision, cnt.geocode_lat::double precision), 4326) <-> 
          ST_SetSRID(ST_MakePoint(wll.longitude::double precision, wll.latitude::double precision), 4326)
      LIMIT 1
  ) AS weather_loc
)
UPDATE lat_lon_city_lookup AS llcl
SET 
    geocode_lat = wgu.geocode_lat,
    geocode_lon = wgu.geocode_lon,
    weather_lat = wgu.weather_lat,
    weather_lon = wgu.weather_lon,
    location_id = wgu.location_id
FROM weather_geocode_unified AS wgu
WHERE llcl.city_id = wgu.city_id;

COMMIT;

-- Add elevation values (in feet) to cities table

BEGIN;

UPDATE cities AS c 
SET elevation = wdll.elevation * 3.28084
FROM lat_lon_city_lookup AS llcl 
JOIN weather_data_location_lookup AS wdll ON llcl.location_id = wdll.location_id
WHERE c.city_id = llcl.city_id;

COMMIT;

-- populate weather conditions using lat_long lookup
BEGIN;

INSERT INTO weather_conditions (city_id, date, wind_speed_max, rainfall, snowfall, precipitation_sum, temp_max, temp_min, precipitation_hours)
SELECT 
  llcl.city_id,
  wdst.date,
  wdst.wind_speed_10m_max_mph,
  wdst.rain_sum_inches,
  wdst.snowfall_sum_inches,
  wdst.precipitation_sum_inches,
  wdst.temperature_2m_max_f,
  wdst.temperature_2m_min_f,
  wdst.precipitation_hours
FROM weather_data_staging_table AS wdst
JOIN weather_data_location_lookup AS wdll
ON wdst.location_id = wdll.location_id
JOIN lat_lon_city_lookup AS llcl
ON wdst.location_id = llcl.location_id;

COMMIT;

-----------------------------------------------------------

-- 5. validate normalized tables

SELECT COUNT(*) FROM census_pops_imputed; -- 366

SELECT COUNT(*) FROM cities; -- 61

SELECT COUNT(*) FROM incidents; -- 7539

SELECT COUNT(*) FROM weather_conditions; -- 133651
SELECT COUNT(*) FROM weather_data_staging_table; -- 164325 

SELECT COUNT(*) FROM weather_data_staging_table WHERE location_id = 0; -- 2191
-- SELECT COUNT(*) FROM weather_data_staging_table GROUP BY location_id; -- 75 rows of 2191
-- SELECT COUNT(*) FROM weather_conditions GROUP BY city_id; -- 61 rows of 2191

