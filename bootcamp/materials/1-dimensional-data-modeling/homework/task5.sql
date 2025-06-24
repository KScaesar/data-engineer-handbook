-- Incremental Update

WITH
last_scd AS ( -- Get the most recent SCD segments
  SELECT *
  FROM actors_history_scd
  WHERE end_date = 1971
),
historical_scd AS ( -- Get historical SCD records
  select
  'historical' as kind,
   *
  FROM actors_history_scd
  WHERE end_date < 1971
),
this_year AS ( -- Get current year actors data
  SELECT *
  FROM actors
  WHERE current_year = 1972
),
unchanged_records AS ( -- Records where status did not change, just update the end_date
  select
    'unchanged' as kind,
    t.actorid,
    l.start_date,
    t.current_year AS end_date,
    t.quality_class,
    t.is_active
  FROM this_year t
  JOIN last_scd l
    ON t.actorid = l.actorid
  WHERE t.quality_class = l.quality_class
    AND t.is_active = l.is_active
),
changed_records AS ( -- Records where status changed, split into old and new segments
  select
    t.actorid,
    l.quality_class AS old_quality_class,
    l.is_active AS old_is_active,
    l.start_date AS old_start_date,
    (t.current_year - 1)  AS old_end_date,
    t.quality_class AS new_quality_class,
    t.is_active AS new_is_active,
    t.current_year AS new_start_date,
    t.current_year AS new_end_date
  FROM this_year t
  JOIN last_scd l
    ON t.actorid = l.actorid
  WHERE t.quality_class <> l.quality_class
     OR t.is_active <> l.is_active
),
insert_records AS ( -- Records for actors who did not appear in last year's SCD
  select
    'insert_records' as kind,
    t.actorid,
    t.current_year AS start_date,
    t.current_year AS end_date,
    t.quality_class, 
    t.is_active
  FROM this_year t
  LEFT JOIN last_scd l
    ON t.actorid = l.actorid
  WHERE l.actorid IS NULL
),
split_changed_records AS ( -- Split changed records into old segment and new segment
  select
    'old_changed' as kind,
    actorid,
    old_start_date AS start_date,
    old_end_date AS end_date,
    old_quality_class AS quality_class,
    old_is_active AS is_active
  FROM changed_records
    UNION ALL
  select
    'new_changed' as kind,
    actorid,
    new_start_date AS start_date,
    new_end_date AS end_date,
    new_quality_class AS quality_class,
    new_is_active AS is_active
  FROM changed_records
),
combined_data AS ( -- Combine all segments into one result set
  SELECT * FROM historical_scd
    UNION ALL
  SELECT * FROM unchanged_records
    UNION ALL
  SELECT * FROM split_changed_records
    UNION ALL
  SELECT * FROM insert_records
)
SELECT * FROM combined_data ORDER BY actorid, start_date;
