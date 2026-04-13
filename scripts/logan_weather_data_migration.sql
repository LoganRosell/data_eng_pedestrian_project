-- Author: Logan Rosell
-- connect to railway database in terminal with the following command: psql postgresql://postgres:lupJXLSDiUiKBosiMsFNyPhjuWbkkdrk@turntable.proxy.rlwy.net:31000/railway

--===============================================
-- Create State Abberivation Lookup Table
--===============================================

--drop table states_lookup;
--Add state abbriviations to cities table
CREATE TABLE IF NOT EXISTS states_lookup (
 abbr char(2),
 state_name varchar(50)
);

BEGIN;

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

COMMIT;

--==============================================
-- Add state abbreviation column to cities table
--==============================================

SELECT *, sl.abbr AS state_abbr
  FROM cities c
  JOIN states_lookup sl ON UPPER(c.state) = sl.state_name;

ALTER TABLE cities 
ADD COLUMN IF NOT EXISTS state_abbr CHAR(2);

UPDATE cities AS c
SET state_abbr = sl.abbr
FROM states_lookup AS sl
WHERE UPPER(c.state) = sl.state_name;

SELECT state, state_abbr 
FROM cities 
WHERE state_abbr IS NULL;

--==============================================
-- Drop metro areas from cities table
--==============================================

DELETE FROM cities
  WHERE city = 'Denver Regional Council of Governments' OR 
    city = 'Oregon Metro' OR 
    city = 'Montgomery County' OR 
    city = 'Hillsborough County'



--====================
--Geocoding data
--====================

-- \copy us_geocodes 
-- FROM '/Users/loganrosell/Desktop/WU_Data_Eng/data_eng_pedestrian_project/data_sources/2025_Gaz_place_national.txt' 
-- WITH (FORMAT CSV, HEADER, DELIMITER '|');

drop table us_geocodes;

CREATE TABLE us_geocodes (
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



--===================================================
--Inspecting Data Quality for us_geocode data
--===================================================

SELECT COUNT(*)
  FROM us_geocodes
  WHERE funcstat = 'A';

--checking for duplicate cites names in the same state
WITH duplicates AS(
  SELECT 
    name,
    COUNT(name) AS num_dups,
    usps AS state
  FROM us_geocodes
  WHERE funcstat = 'A'
  GROUP BY name, state
)
  SELECT *
    FROM duplicates
    WHERE num_dups > 1
    ORDER By num_dups DESC;



--===================================================
--Joinning vision zero cities with geo-code data
--===================================================

SELECT 
    g.name AS census_name, 
    c.city AS vision_zero_name,
    g.lon,
    g.lat,
    g.geoid
FROM cities c
LEFT JOIN us_geocodes g
  ON (UPPER(g.name) LIKE UPPER(c.city) || '%')
    AND (g.usps = c.state_abbr)
    AND g.funcstat = 'A'
    AND (g.lsad = '25' OR g.lsad = '57')
ORDER BY vision_zero_name;

SELECT COUNT(*)
FROM cities;


WITH RankedMatches AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY c.city, c.state_abbr 
            ORDER BY (CASE WHEN g.funcstat = 'A' THEN 1 ELSE 2 END) ASC
        ) as priority
    FROM cities c
    LEFT JOIN us_geocodes g
      ON (UPPER(g.name) LIKE UPPER(REPLACE(c.city, ' DC', '')) || '%')
        AND (g.usps = c.state_abbr)
)
SELECT 
    *
FROM RankedMatches
WHERE priority = 1;


--============================
-- create timezone table for each us city
--============================
drop table if exists us_city_timezones;

CREATE TABLE us_city_timezones (
  city varchar(100),
  city_ascii varchar(100),
  state_id varchar(2),
  state_name varchar(100),
  timezone text
);

-- \copy us_city_timezones
--   FROM '/Users/loganrosell/Desktop/WU_Data_Eng/data_eng_project_files/us_city_timezones.csv'
--   WITH (FORMAT CSV, HEADER);


--============================
-- Ingesting Weather Data
--============================


-- DROP TABLE IF EXISTS weather_data_staging_table;
-- DROP TABLE IF EXISTS weather_data_location_lookup;

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


-- \copy weather_data_staging_table
--   FROM '/Users/loganrosell/Desktop/WU_Data_Eng/data_eng_project_files/daily_weather_data.csv'
--   WITH (FORMAT CSV, HEADER);

SELECT COUNT(*)
  FROM weather_data_staging_table;

--===========================================
-- Ingesting Weather API location lookup Data
--===========================================

CREATE TABLE IF NOT EXISTS weather_data_location_lookup (
  location_id integer PRIMARY KEY,
  latitude numeric,
  longitude numeric,
  elevation integer,
  utc_offset_seconds integer,
  timezone varchar(10),
  timezone_abbreviation varchar(10)
);

-- \copy weather_data_location_lookup
--   FROM '/Users/loganrosell/Desktop/WU_Data_Eng/data_eng_project_files/daily_weather_data_location_lookup.csv'
--   WITH (FORMAT CSV, HEADER);

SELECT COUNT(*)
  FROM weather_data_location_lookup;


--===================================================
-- Checking for data quality issues in weather data
--===================================================

-- making sure all locations have the same number of observations
SELECT location_id, COUNT(*)
  FROM weather_data_staging_table
  GROUP BY location_id;

-- Checking for missing data
SELECT 
    COUNT(CASE WHEN location_id IS NULL THEN 1 END) AS location_null_count,
    COUNT(CASE WHEN date IS NULL THEN 1 END) AS date_null_count,
    COUNT(CASE WHEN weather_code IS NULL THEN 1 END) AS weather_code_null_count,
    COUNT(CASE WHEN precipitation_sum_inches IS NULL THEN 1 END) AS preip_sum_null_count,
    COUNT(CASE WHEN rain_sum_inches IS NULL THEN 1 END) AS rain_sum_null_count,
    COUNT(CASE WHEN snowfall_sum_inches IS NULL THEN 1 END) AS snow_sum_null_count,
    COUNT(CASE WHEN precipitation_hours IS NULL THEN 1 END) AS precip_hours_null_count,
    COUNT(CASE WHEN temperature_2m_min_f IS NULL THEN 1 END) AS temp_min_null_count,
    COUNT(CASE WHEN temperature_2m_max_f IS NULL THEN 1 END) AS temp_max_null_count,
    COUNT(CASE WHEN wind_speed_10m_max_mph IS NULL THEN 1 END) AS wind_max_null_count
  FROM weather_data_staging_table;

-- Checking precipitation values across locations
SELECT location_id,
    ROUND(AVG(precipitation_sum_inches), 2) AS mean_precip,
    MAX(precipitation_sum_inches) AS max_precip,
    ROUND(AVG(precipitation_hours), 2) AS mean_hours_of_precip
  FROM weather_data_staging_table
  GROUP BY location_id
  ORDER BY mean_precip DESC;

-- Checking temp values across locations
SELECT
    location_id,
    ROUND(AVG(temperature_2m_min_f), 2) AS mean_low_temp,
    ROUND(AVG(temperature_2m_max_f), 2) AS mean_high_temp
  FROM weather_data_staging_table
  GROUP BY location_id
  ORDER BY mean_high_temp DESC;


SELECT 
    EXTRACT(month FROM date) AS month,
    ROUND(AVG(temperature_2m_max_f), 2) AS mean_high_temp
  FROM weather_data_staging_table
  GROUP BY month
  ORDER BY month;




--============================================================
-- migrate weather data from staging table to nomralized table
--============================================================

CREATE EXTENSION IF NOT EXISTS postgis;

--validate that distance based search matching works

SELECT 
    w.location_id,
    w.latitude AS weather_lat,
    w.longitude AS weather_lon,
    geo.name AS city_name,
    geo.usps AS state_code,
    ST_Distance(
        ST_SetSRID(ST_MakePoint(w.longitude::double precision, w.latitude::double precision), 4326)::geography,
        ST_SetSRID(ST_MakePoint(geo.lon::double precision, geo.lat::double precision), 4326)::geography
    ) / 1609.34 AS distance_miles
FROM weather_data_location_lookup AS w
  CROSS JOIN LATERAL (
      SELECT 
          name, 
          usps, 
          lat, 
          lon
      FROM us_geocodes
      ORDER BY 
          ST_SetSRID(ST_MakePoint(lon::double precision, lat::double precision), 4326) <-> 
          ST_SetSRID(ST_MakePoint(w.longitude::double precision, w.latitude::double precision), 4326)
      LIMIT 1
  ) AS geo;


--============================
-- Normalizing Weather Data
--============================

-- reset weather_conidtion table
-- BEGIN;
-- TRUNCATE TABLE weather_conditions CASCADE;
-- COMMIT;


BEGIN;

WITH city_names_table AS (
  WITH RankedMatches AS (
      SELECT 
          *,
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
INSERT INTO weather_conditions(
                                city_id,
                                date,
                                wind_speed_max,
                                rainfall,
                                snowfall,
                                visibility,
                                lighting_condition,
                                precipitation_sum,
                                temp_max,
                                temp_min,
                                precipitation_hours)
  SELECT DISTINCT 
    cnt.city_id,
    wst.date,
    wst.wind_speed_10m_max_mph,
    wst.rain_sum_inches,
    wst.snowfall_sum_inches,
    0,
    'idk',
    wst.precipitation_sum_inches,
    wst.temperature_2m_max_f,
    wst.temperature_2m_min_f,
    wst.precipitation_hours
  
  FROM weather_data_location_lookup AS wl
    JOIN weather_data_staging_table wst ON wst.location_id = wl.location_id
    CROSS JOIN LATERAL (
        SELECT 
            name, 
            usps, 
            lat, 
            lon,
            geoid
        FROM us_geocodes g
        ORDER BY 
            ST_SetSRID(ST_MakePoint(g.lon::double precision, g.lat::double precision), 4326) <-> 
            ST_SetSRID(ST_MakePoint(wl.longitude::double precision, wl.latitude::double precision), 4326)
        LIMIT 1
    ) AS geo
JOIN city_names_table cnt ON cnt.geoid = geo.geoid;
  

COMMIT;

--====================================
-- Validating Normalizing Weather Data
--====================================

SELECT COUNT(*) FROM weather_conditions;
SELECT COUNT(*) FROM weather_data_staging_table;

SELECT COUNT(*) FROM weather_data_staging_table WHERE location_id = 0;
SELECT COUNT(*) FROM weather_data_staging_table GROUP BY location_id;

SELECT COUNT(*) FROM weather_conditions GROUP BY city_id;


-- WITH city_names_table AS (
--   WITH RankedMatches AS (
--       SELECT 
--           c.city_id,
--           c.city AS vision_zero_city_name,
--           c.state,
--           c.county,
--           c.state_abbr,
--           g.geoid,
--           g.name AS geocode_city_name,
--           g.lat AS geocode_lat,
--           g.lon AS geocode_lon,
--           ROW_NUMBER() OVER (
--               PARTITION BY c.city, c.state_abbr 
--               ORDER BY (CASE WHEN g.funcstat = 'A' THEN 1 ELSE 2 END) ASC
--           ) as priority
--       FROM cities c
--       LEFT JOIN us_geocodes g
--         ON (UPPER(g.name) LIKE UPPER(REPLACE(c.city, ' DC', '')) || '%')
--           AND (g.usps = c.state_abbr)
--   )
--   SELECT *
--   FROM RankedMatches
--   WHERE priority = 1
-- )
-- SELECT 
--     city_id,
--     geocode_lat,
--     geocode_lon
--   FROM city_names_table;


WITH condition_to_location_join AS (
  SELECT 
    wst.*,
    wl.latitude,
    wl.longitude,
    wl.elevation
  FROM weather_data_location_lookup AS wl
  JOIN weather_data_staging_table wst ON wst.location_id = wl.location_id
)
SELECT COUNT(*)
  FROM condition_to_location_join
  GROUP BY location_id;
  

--====================================
-- creating lat_lon_city_lookup_table
--====================================

-- DROP TABLE lat_lon_city_lookup;

CREATE TABLE IF NOT EXISTS lat_lon_city_lookup (
 geocode_lat numeric,
 geocode_lon numeric,
  weather_lat numeric,
  weather_lon numeric,
  city_id integer,
  location_id integer
);

-- add city IDs to table
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

--================================================
-- Add elevation values (in feet) to cities table
--================================================

BEGIN;

UPDATE cities AS c 
  SET elevation = wdll.elevation * 3.28084
  FROM lat_lon_city_lookup AS llcl 
  JOIN weather_data_location_lookup AS wdll ON llcl.location_id = wdll.location_id
  WHERE c.city_id = llcl.city_id

COMMIT;

--================================================
-- Drop lon and lat columns from cities table
--================================================
ALTER TABLE cities 
DROP COLUMN IF EXISTS longitude,
DROP COLUMN IF EXISTS latitude;





