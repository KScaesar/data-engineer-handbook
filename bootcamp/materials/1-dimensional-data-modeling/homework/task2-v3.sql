-- query first year => 1970
select distinct year
from actor_films af
order by "year"
limit 1;

-- https://discord.com/channels/1106357930443407391/1385049443912519689
-- make data pipeline idempotent

-- success
-- cumulative table
with last_year as (
  select * from actors
  where current_year = 1969
),
this_year as (
  select
    actorid,
    year,
    avg(rating) as avg_rating,
    ARRAY_AGG(ROW(film, votes, rating, filmid)::film) AS films
  from actor_films
  where year = 1970
  group by actorid, year
),
cum_data as (
  select
    coalesce(ly.actorid, ty.actorid) as actorid,
    coalesce(ly.films, array[]::film[]) || ty.films as films,
    case
      when ty.avg_rating is not null then
      (
        case
          when ty.avg_rating > 8 then 'star'
          when ty.avg_rating > 7 then 'good'
          when ty.avg_rating > 6 then 'average'
          else 'bad'
        end
      )::quality_class
      else ly.quality_class
    end as quality_class,
    ty.year is not null as is_active,
    1970 as current_year
  from last_year ly
  full outer join this_year ty
  on ly.actorid = ty.actorid
)
INSERT INTO actors
select * from cum_data
ON CONFLICT (actorid, current_year)
DO UPDATE SET
  films = EXCLUDED.films,
  quality_class = EXCLUDED.quality_class,
  is_active = EXCLUDED.is_active;

-- select * from cum_data;
-- actorid  |films                                                                          |quality_class|is_active|current_year|
-- ---------+-------------------------------------------------------------------------------+-------------+---------+------------+
-- nm0000003|{"(The Bear and the Doll,431,6.4,tt0064779)","(Les novices,219,5.1,tt0066164)"}|bad          |true     |        1970|
-- nm0000006|{"(A Walk in the Spring Rain,696,6.2,tt0066542)"}                              |average      |true     |        1970|
-- nm0000012|{"(Connecting Rooms,585,6.9,tt0066943)"}                                       |average      |true     |        1970|
-- nm0000014|{"(The Adventurers,656,5.5,tt0065374)"}                                        |bad          |true     |        1970|
-- nm0000018|{"(There Was a Crooked Man...,4138,7.0,tt0066448)"}                            |average      |true     |        1970|
