--  Trino queries for the weather medallion (catalog: delta, schema: solar)
--  Open the Trino CLI with:
--      docker exec -it solar-trino trino --catalog delta --schema solar
--  then paste ONE statement at a time (each ends with ;). Quit with:  quit

SHOW SCHEMAS FROM delta;
SHOW TABLES FROM delta.solar;
DESCRIBE delta.solar.gold_solar_potential_by_location;

-- Rows + time range per layer (Bronze should keep growing while poller + streaming run)
SELECT 'bronze_weather' AS layer, count(*) AS row_count,
       min(observation_timestamp) AS first_observation, max(observation_timestamp) AS last_observation
FROM delta.solar.bronze_weather
UNION ALL
SELECT 'silver_weather', count(*), min(observation_timestamp), max(observation_timestamp)
FROM delta.solar.silver_weather;

-- Coverage per source (open-meteo should show 27 governorates)
SELECT source, count(DISTINCT location_id) AS governorates, count(*) AS row_count
FROM delta.solar.silver_weather
GROUP BY source;

-- Silver must contain NO duplicates -> expect 0
SELECT count(*) AS duplicate_groups
FROM (
    SELECT source, location_id, observation_timestamp
    FROM delta.solar.silver_weather
    GROUP BY source, location_id, observation_timestamp
    HAVING count(*) > 1
);

-- Gold row counts
SELECT 'gold_solar_radiation_daily' AS table_name, count(*) AS row_count FROM delta.solar.gold_solar_radiation_daily
UNION ALL SELECT 'gold_solar_potential_by_location', count(*) FROM delta.solar.gold_solar_potential_by_location
UNION ALL SELECT 'gold_peak_production_hours', count(*) FROM delta.solar.gold_peak_production_hours
UNION ALL SELECT 'gold_dim_governorate', count(*) FROM delta.solar.gold_dim_governorate;

-- 3.1 Which governorates have the best solar potential? (ranked inside each source)
SELECT p.solar_potential_rank, p.location_id, d.name_ar, p.site_name, p.source,
       p.avg_ghi_w_m2, p.max_ghi_w_m2, p.avg_dni_w_m2, p.avg_cloud_cover_pct,
       p.days_observed, p.total_observations
FROM delta.solar.gold_solar_potential_by_location p
LEFT JOIN delta.solar.gold_dim_governorate d ON d.location_id = p.location_id
WHERE p.source = 'open-meteo'
ORDER BY p.solar_potential_rank, p.location_id;

-- 3.2 Daily GHI trend for one governorate
SELECT observation_date, source, avg_ghi_w_m2, max_ghi_w_m2, avg_temperature_c, avg_cloud_cover_pct, observation_count
FROM delta.solar.gold_solar_radiation_daily
WHERE location_id = 'aswan'
ORDER BY observation_date, source;

-- 3.3 Best 3 production hours (UTC) per governorate
SELECT location_id, site_name, observation_hour, avg_ghi_w_m2, avg_dni_w_m2, peak_hour_rank
FROM delta.solar.gold_peak_production_hours
WHERE peak_hour_rank <= 3
ORDER BY location_id, peak_hour_rank;

-- 3.4 Hour x governorate heat-map data
SELECT location_id, observation_hour, avg_ghi_w_m2
FROM delta.solar.gold_peak_production_hours
ORDER BY location_id, observation_hour;

-- 3.5 Seasonality: month x governorate
SELECT location_id, source, month(observation_date) AS month_no, round(avg(avg_ghi_w_m2), 2) AS avg_ghi_w_m2
FROM delta.solar.gold_solar_radiation_daily
GROUP BY location_id, source, month(observation_date)
ORDER BY location_id, source, month_no;

-- 3.6 Egypt-wide KPIs
SELECT source,
       round(avg(avg_ghi_w_m2), 2) AS egypt_avg_ghi_w_m2,
       max(max_ghi_w_m2)           AS egypt_max_ghi_w_m2,
       count(DISTINCT observation_date) AS days_observed,
       count(DISTINCT location_id)      AS governorates
FROM delta.solar.gold_solar_radiation_daily
GROUP BY source;

-- 3.7 Latest reading per governorate (always ROW_NUMBER, never a raw MAX() across columns)
SELECT location_id, site_name, observation_timestamp, ghi_w_m2, temperature_c, cloud_cover_pct
FROM (
    SELECT s.*,
           row_number() OVER (PARTITION BY location_id ORDER BY observation_timestamp DESC) AS rn
    FROM delta.solar.silver_weather s
    WHERE source = 'open-meteo'
)
WHERE rn = 1
ORDER BY ghi_w_m2 DESC;
