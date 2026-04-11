SELECT 
  v.phenomenal_id AS id
  , v.state
  , v.city
  , v.vz_join_year
  , c.census_pop AS population
FROM census_city_pops c
JOIN vz_communities v 
ON c.census_state_id = v.fips_state_id
AND c.census_place_id = v.fips_place_id
ORDER BY state, city ASC;









