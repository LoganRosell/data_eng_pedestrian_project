-----------------------------------------------------------
-- FULL PIPELINE
-- overview:
-- 1. create staging tables 
-- 2. import data from sources into staging tables
-- 3. create normalized tables
-- 4. migrate data from staging tables into normalized tables
-- 5. perform checks to validate normalized data
-----------------------------------------------------------

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
CREATE TABLE states_lookup (
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

-----------------------------------------------------------

-- 2. import data
-- run copy commands in terminal with psql if you run into errors

-- weather data

-- COPY us_geocodes 
-- FROM '/tmp/2025_Gaz_place_national.txt' 
-- WITH (FORMAT CSV, HEADER, DELIMITER '|');

-- COPY weather_data_staging_table
-- FROM '/tmp/daily_weather_data.csv'
-- WITH (FORMAT CSV, HEADER);

-- COPY weather_data_location_lookup
-- FROM '/tmp/daily_weather_data_location_lookup.csv'
-- WITH (FORMAT CSV, HEADER);

-- -- census data


-- -- FARS data

-- COPY fars_staging
-- FROM '/tmp/FARS_import.csv'
-- WITH (FORMAT CSV, HEADER);

-----------------------------------------------------------

-- 3. create normalized tables

DROP TABLE IF EXISTS cities CASCADE;
CREATE TABLE IF NOT EXISTS cities
(
    city_id integer,
    city text,
    state text,
    county text,
    state_abbr character(2) ,
    fips_place character varying(5) ,
    viz_join_year integer,
    elevation double precision,
    fips_state_id character varying(2) ,
    CONSTRAINT cities_pkey PRIMARY KEY (city_id)
);

DROP TABLE IF EXISTS weather_conditions;
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

DROP TABLE IF EXISTS incidents;
CREATE TABLE incidents (
  case_num int primary key,
  city_id int references cities,
  condition_id int references weather_conditions(condition_id),
  incident_longitude float,
  incident_latitude float,
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

DROP TABLE IF EXISTS lat_lon_city_lookup;
CREATE TABLE IF NOT EXISTS lat_lon_city_lookup (
  geocode_lat numeric,
  geocode_lon numeric,
  weather_lat numeric,
  weather_lon numeric,
  city_id integer REFERENCES cities (city_id),
  location_id integer
);

DROP TABLE IF EXISTS census_pops_imputed;
CREATE TABLE IF NOT EXISTS census_pops_imputed
(
    city_year_id text PRIMARY KEY,
    year integer,
    city_id integer REFERENCES cities(city_id),
    city_name character varying(100),
    census_state_id character(2),
    census_place_id character(5),
    census_pop_imputed numeric
);

-----------------------------------------------------------

-- 4. migrate data from staging tables into normalized tables

-- first, populate the cities table made by us
-- COPY cities
-- FROM '/tmp/vz_communities.csv'
-- WITH (FORMAT CSV, HEADER);















