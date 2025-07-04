-- Full Update
-- https://chatgpt.com/share/685a19f7-c7ac-800d-a942-75ca99d13f75

WITH consts AS (
  SELECT 1974 AS load_year
),
with_prev AS (
  SELECT
    actorid,
    current_year,
    quality_class,
    is_active,
    LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_quality,
    LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_active,
    c.load_year AS snapshot_year
  FROM actors
  JOIN consts c ON TRUE
  WHERE actors.current_year <= c.load_year
),
with_change AS (
  SELECT *,
    CASE
      -- first insert
      WHEN prev_quality IS NULL THEN 1
      -- value changed
      WHEN quality_class <> prev_quality OR is_active <> prev_active THEN 1
      -- no change
      ELSE 0
    END AS is_change
  FROM with_prev
),
-- "streak" is commonly used to describe the number of consecutive days or occurrences of an event or state.
-- Here, we use it to identify segments of consecutive years with the same quality_class and is_active status.
with_segment AS (
  SELECT *,
    SUM(is_change) OVER (PARTITION BY actorid ORDER BY current_year) AS segment_id
  FROM with_change
),
agg_data AS (
  SELECT
    actorid,
    MIN(current_year) AS start_date,
    -- set end_date to 9999 if it's the latest segment
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY actorid ORDER BY segment_id DESC) = 1 THEN 9999
      ELSE MAX(current_year)
    END AS end_date,
    -- The following fields use MIN() only to satisfy SQL aggregation syntax (all values in segment should be same)
    MIN(snapshot_year) AS snapshot_date,
    (MIN(quality_class::TEXT))::quality_class AS quality_class,
    -- Use BOOL_AND for is_active (all values in segment should be same)
    BOOL_AND(is_active) AS is_active
  FROM with_segment
  GROUP BY actorid, segment_id
  ORDER BY actorid, start_date
)
INSERT INTO actors_history_scd
SELECT * FROM agg_data
ON CONFLICT (actorid, start_date)
DO UPDATE SET
  quality_class = EXCLUDED.quality_class,
  is_active = EXCLUDED.is_active,
  end_date = EXCLUDED.end_date,
  snapshot_date = EXCLUDED.snapshot_date;

-- Perform a full SCD update for records where current_year <= 1974.
--
-- SELECT * FROM agg_data;
-- actorid  |start_date|end_date|snapshot_date|quality_class|is_active|
-- ---------+----------+--------+-------------+-------------+---------+
-- nm0000001|      1974|    9999|         1974|average      |true     |
-- nm0000002|      1974|    9999|         1974|good         |true     |
-- nm0000003|      1970|    1971|         1974|bad          |true     |
-- nm0000003|      1972|    1972|         1974|bad          |false    |
-- nm0000003|      1973|    1973|         1974|bad          |true     |
-- nm0000003|      1974|    9999|         1974|bad          |false    |
-- nm0000005|      1972|    1972|         1974|star         |true     |
-- nm0000005|      1973|    9999|         1974|star         |false    |
-- nm0000006|      1970|    1970|         1974|average      |true     |
-- nm0000006|      1971|    1972|         1974|average      |false    |
-- nm0000006|      1973|    1973|         1974|average      |true     |
-- nm0000006|      1974|    9999|         1974|good         |true     |
-- nm0000008|      1971|    1971|         1974|bad          |true     |
-- nm0000008|      1972|    1972|         1974|star         |true     |
-- nm0000008|      1973|    9999|         1974|star         |false    |
--
-- SELECT * FROM with_segment;
-- actorid  |current_year|quality_class|is_active|prev_quality|prev_active|snapshot_year|is_change|segment_id|
-- ---------+------------+-------------+---------+------------+-----------+-------------+---------+----------+
-- nm0000001|        1974|average      |true     |            |           |         1974|        1|         1|
-- nm0000002|        1974|good         |true     |            |           |         1974|        1|         1|
-- nm0000003|        1970|bad          |true     |            |           |         1974|        1|         1|
-- nm0000003|        1971|bad          |true     |bad         |true       |         1974|        0|         1|
-- nm0000003|        1972|bad          |false    |bad         |true       |         1974|        1|         2|
-- nm0000003|        1973|bad          |true     |bad         |false      |         1974|        1|         3|
-- nm0000003|        1974|bad          |false    |bad         |true       |         1974|        1|         4|
-- nm0000005|        1972|star         |true     |            |           |         1974|        1|         1|
-- nm0000005|        1973|star         |false    |star        |true       |         1974|        1|         2|
-- nm0000005|        1974|star         |false    |star        |false      |         1974|        0|         2|
-- nm0000006|        1970|average      |true     |            |           |         1974|        1|         1|
-- nm0000006|        1971|average      |false    |average     |true       |         1974|        1|         2|
-- nm0000006|        1972|average      |false    |average     |false      |         1974|        0|         2|
-- nm0000006|        1973|average      |true     |average     |false      |         1974|        1|         3|
-- nm0000006|        1974|good         |true     |average     |true       |         1974|        1|         4|
-- nm0000008|        1971|bad          |true     |            |           |         1974|        1|         1|
-- nm0000008|        1972|star         |true     |bad         |true       |         1974|        1|         2|
-- nm0000008|        1973|star         |false    |star        |true       |         1974|        1|         3|
-- nm0000008|        1974|star         |false    |star        |false      |         1974|        0|         3|
