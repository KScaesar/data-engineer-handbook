-- Full Update
-- https://chatgpt.com/share/685a19f7-c7ac-800d-a942-75ca99d13f75

WITH with_prev AS (
  SELECT
    actorid,
    current_year,
    quality_class,
    is_active,
    LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_quality,
    LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_active
  FROM actors
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
with_segment AS (
  SELECT *,
    SUM(is_change) OVER (PARTITION BY actorid ORDER BY current_year) AS segment_id
  FROM with_change
),
agg_data AS (
  SELECT
    actorid,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    -- Use ANY_VALUE or MIN/MAX on the text representation
    (MIN(quality_class::TEXT))::quality_class AS quality_class,
    -- Use BOOL_AND for is_active (all values in segment should be same)
    BOOL_AND(is_active) AS is_active
  FROM with_segment
  GROUP BY actorid, segment_id
  ORDER BY actorid, start_date
)
INSERT INTO actors_history_scd
SELECT * FROM agg_data
ON CONFLICT (actorid, start_date, end_date)
DO UPDATE SET
  quality_class = EXCLUDED.quality_class,
  is_active = EXCLUDED.is_active;

-- SELECT * FROM agg_data;
-- actorid  |start_date|end_date|quality_class|is_active|
-- ---------+----------+--------+-------------+---------+
-- nm0000003|      1970|    1971|bad          |true     |
-- nm0000003|      1972|    1972|bad          |false    |
-- nm0000005|      1972|    1972|star         |true     |
-- nm0000006|      1970|    1970|average      |true     |
-- nm0000006|      1971|    1972|average      |false    |
-- nm0000008|      1971|    1971|bad          |true     |
-- nm0000008|      1972|    1972|star         |true     |
--
-- SELECT * FROM with_segment;
-- actorid  |current_year|quality_class|is_active|prev_quality|prev_active|is_change|segment_id|
-- ---------+------------+-------------+---------+------------+-----------+---------+----------+
-- nm0000003|        1970|bad          |true     |            |           |        1|         1|
-- nm0000003|        1971|bad          |true     |bad         |true       |        0|         1|
-- nm0000003|        1972|bad          |false    |bad         |true       |        1|         2|
-- nm0000005|        1972|star         |true     |            |           |        1|         1|
-- nm0000006|        1970|average      |true     |            |           |        1|         1|
-- nm0000006|        1971|average      |false    |average     |true       |        1|         2|
-- nm0000006|        1972|average      |false    |average     |false      |        0|         2|
-- nm0000008|        1971|bad          |true     |            |           |        1|         1|
-- nm0000008|        1972|star         |true     |bad         |true       |        1|         2|
