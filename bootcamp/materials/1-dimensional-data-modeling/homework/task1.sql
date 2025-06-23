CREATE TYPE film AS (
    film   TEXT,
    votes  INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors (
    actorid       TEXT,
    films         film[],
    quality_class quality_class,
    is_active     BOOLEAN,
    current_year  INTEGER,
    PRIMARY KEY (actorid, current_year)
);