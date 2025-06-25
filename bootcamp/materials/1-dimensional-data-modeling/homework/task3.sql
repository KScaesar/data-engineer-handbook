-- DROP TABLE IF EXISTS actors_history_scd;

CREATE TABLE actors_history_scd (
  actorid        TEXT NOT NULL,
  start_date     INTEGER NOT NULL,
  end_date       INTEGER NOT NULL,
  snapshot_date  INTEGER NOT NULL,
  quality_class  quality_class,
  is_active      BOOLEAN,
  PRIMARY KEY (actorid, start_date),
  CHECK (end_date >= start_date)
);

-- https://github.com/copilot/share/4a3e5096-4ba0-8426-9813-880860df40b8
-- https://discord.com/channels/1106357930443407391/1386647289002594314

-- start_date
-- Whose time? Business data time
-- Description:
-- When this state started being valid

-- end_date
-- Whose time? Business data time
-- Description:
-- When this state stopped being valid

-- snapshot_date
-- Whose time? Source data time
-- Description:
-- This ETL process extracts business data from a specific point in time in the source system, capturing the whole window of the data.

-- process_date
-- Whose time? ETL system time
-- Description:
-- When this row was processed and written into the table
