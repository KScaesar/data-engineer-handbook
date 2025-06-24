-- DROP TABLE IF EXISTS actors_history_scd;

CREATE TABLE actors_history_scd (
  actorid       TEXT NOT NULL,
  start_date    INTEGER NOT NULL,
  end_date      INTEGER NOT NULL,
  quality_class quality_class,
  is_active     BOOLEAN,
  PRIMARY KEY (actorid, start_date, end_date)
);

-- https://github.com/copilot/share/4a3e5096-4ba0-8426-9813-880860df40b8
-- https://discord.com/channels/1106357930443407391/1386647289002594314
