WITH t_female AS (
  SELECT
    CAST(start_station_id AS STRING) AS start_station_id,
    start_station_name,
    CAST(end_station_id AS STRING) AS end_station_id,
    end_station_name,
    AVG(duration_sec) AS female_avg_trip_duration
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  WHERE member_gender = 'Female'
  GROUP BY
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name
),
t_male AS (
  SELECT
    CAST(start_station_id AS STRING) AS start_station_id,
    start_station_name,
    CAST(end_station_id AS STRING) AS end_station_id,
    end_station_name,
    AVG(duration_sec) AS male_avg_trip_duration
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  WHERE member_gender = 'Male'
  GROUP BY
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name
)
SELECT
  t_female.start_station_id,
  bsi_start.short_name AS start_station_short_name,
  t_female.end_station_id,
  bsi_end.short_name AS end_station_short_name,
  AVG(t_female.female_avg_trip_duration) AS avg_duration_female,
  AVG(t_male.male_avg_trip_duration) AS avg_duration_male
FROM
  t_female
JOIN
  bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info bsi_start ON t_female.start_station_id = bsi_start.station_id
JOIN
  t_male ON t_female.start_station_id = t_male.start_station_id
  AND t_female.end_station_id = t_male.end_station_id
JOIN
  bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info bsi_end ON t_female.end_station_id = bsi_end.station_id
GROUP BY
  t_female.start_station_id, bsi_start.short_name, t_female.end_station_id, bsi_end.short_name;
