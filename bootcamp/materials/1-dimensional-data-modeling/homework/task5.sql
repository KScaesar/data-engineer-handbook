-- Incremental Update

WITH
last_scd AS ( -- Get the most recent SCD records
  SELECT *
  FROM actors_history_scd
  WHERE snapshot_date = 1973 AND end_date = 9999
),
this_year AS ( -- Get current year actors data
  SELECT *
  FROM actors
  WHERE current_year = 1974
),
unchanged_records AS ( -- Records where status did not change, just update the end_date and snapshot_date
  select
    'unchanged' as kind,
    t.actorid,
    l.start_date,
    9999 AS end_date,
    t.current_year AS snapshot_date,
    t.quality_class,
    t.is_active
  FROM this_year t
  JOIN last_scd l
    ON t.actorid = l.actorid
  WHERE t.quality_class = l.quality_class
    AND t.is_active = l.is_active
),
changed_records AS ( -- Records where status changed, split into before and after records
  select
    t.actorid,
    t.current_year AS snapshot_date,
    -- before
    l.start_date AS before_start_date,
    (t.current_year - 1)  AS before_end_date,
    l.quality_class AS before_quality_class,
    l.is_active AS before_is_active,
    -- after
    t.current_year AS after_start_date,
    9999 AS after_end_date,
    t.quality_class AS after_quality_class,
    t.is_active AS after_is_active
  FROM this_year t
  JOIN last_scd l
    ON t.actorid = l.actorid
  WHERE t.quality_class <> l.quality_class
     OR t.is_active <> l.is_active
),
split_changed_records AS ( -- Split changed records into before records and after records
  select
    'before_changed' as kind,
    actorid,
    before_start_date AS start_date,
    before_end_date AS end_date,
    snapshot_date,
    before_quality_class AS quality_class,
    before_is_active AS is_active
  FROM changed_records
    UNION ALL
  select
    'after_changed' as kind,
    actorid,
    after_start_date AS start_date,
    after_end_date AS end_date,
    snapshot_date,
    after_quality_class AS quality_class,
    after_is_active AS is_active
  FROM changed_records
),
new_records AS ( -- Records for actors who did not appear in last year's SCD
  select
    'new_records' as kind,
    t.actorid,
    t.current_year AS start_date,
    9999 AS end_date,
    t.current_year AS snapshot_date,
    t.quality_class,
    t.is_active
  FROM this_year t
  LEFT JOIN last_scd l
    ON t.actorid = l.actorid
  WHERE l.actorid IS NULL
),
historical_scd AS ( -- Get historical SCD records
  select
  'historical' as kind,
   *
  FROM actors_history_scd
  WHERE end_date < 9999
),
combined_data AS ( -- Combine all records into one result set
  SELECT * FROM unchanged_records
    UNION ALL
  SELECT * FROM split_changed_records
    UNION ALL
  SELECT * FROM new_records
    UNION ALL
  SELECT * FROM historical_scd
)
INSERT INTO actors_history_scd
--SELECT * FROM combined_data ORDER BY actorid, start_date
SELECT actorid, start_date, end_date, snapshot_date, quality_class, is_active FROM combined_data ORDER BY actorid, start_date
ON CONFLICT (actorid, start_date)
DO UPDATE SET
  quality_class = EXCLUDED.quality_class,
  is_active = EXCLUDED.is_active,
  end_date = EXCLUDED.end_date,
  snapshot_date = EXCLUDED.snapshot_date;

-- Perform a full SCD update for records where current_year <= 1973,
-- then perform an incremental SCD update using this_year = 1974.
--
-- SELECT * FROM combined_data ORDER BY actorid, start_date;
-- kind          |actorid  |start_date|end_date|snapshot_date|quality_class|is_active|
-- --------------+---------+----------+--------+-------------+-------------+---------+
-- new_records   |nm0000001|      1974|    9999|         1974|average      |true     |
-- new_records   |nm0000002|      1974|    9999|         1974|good         |true     |
-- historical    |nm0000003|      1970|    1971|         1973|bad          |true     |
-- historical    |nm0000003|      1972|    1972|         1973|bad          |false    |
-- before_changed|nm0000003|      1973|    1973|         1974|bad          |true     |
-- after_changed |nm0000003|      1974|    9999|         1974|bad          |false    |
-- historical    |nm0000005|      1972|    1972|         1973|star         |true     |
-- unchanged     |nm0000005|      1973|    9999|         1974|star         |false    |
-- historical    |nm0000006|      1970|    1970|         1973|average      |true     |
-- historical    |nm0000006|      1971|    1972|         1973|average      |false    |
-- before_changed|nm0000006|      1973|    1973|         1974|average      |true     |
-- after_changed |nm0000006|      1974|    9999|         1974|good         |true     |
-- historical    |nm0000008|      1971|    1971|         1973|bad          |true     |
-- historical    |nm0000008|      1972|    1972|         1973|star         |true     |
-- unchanged     |nm0000008|      1973|    9999|         1974|star         |false    |

-- Perform multiple incremental SCD updates, one per year from current_year = 1970 to 1974.
--
-- SELECT * FROM combined_data ORDER BY actorid, start_date;
-- kind          |actorid  |start_date|end_date|snapshot_date|quality_class|is_active|
-- --------------+---------+----------+--------+-------------+-------------+---------+
-- new_records   |nm0000001|      1974|    9999|         1974|average      |true     |
-- new_records   |nm0000002|      1974|    9999|         1974|good         |true     |
-- historical    |nm0000003|      1970|    1971|         1972|bad          |true     |
-- historical    |nm0000003|      1972|    1972|         1973|bad          |false    |
-- before_changed|nm0000003|      1973|    1973|         1974|bad          |true     |
-- after_changed |nm0000003|      1974|    9999|         1974|bad          |false    |
-- historical    |nm0000005|      1972|    1972|         1973|star         |true     |
-- unchanged     |nm0000005|      1973|    9999|         1974|star         |false    |
-- historical    |nm0000006|      1970|    1970|         1971|average      |true     |
-- historical    |nm0000006|      1971|    1972|         1973|average      |false    |
-- before_changed|nm0000006|      1973|    1973|         1974|average      |true     |
-- after_changed |nm0000006|      1974|    9999|         1974|good         |true     |
-- historical    |nm0000008|      1971|    1971|         1972|bad          |true     |
-- historical    |nm0000008|      1972|    1972|         1973|star         |true     |
-- unchanged     |nm0000008|      1973|    9999|         1974|star         |false    |
