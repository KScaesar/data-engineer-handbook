-- query first year => 1970
select distinct year
from actor_films af
order by "year"
limit 1;

-- fail
-- cumulative table
with last_year as (
	select * from actors
	where current_year = 1969
),
this_year as (
	select * from actor_films
	where year = 1970
),
this_year_rate as (
	select
	  actorid,
	  avg(rating) as avg_rating
	from actor_films
	where year = 1970
	group by actorid
)
--INSERT INTO actors
select
	coalesce(ly.actorid, ty.actorid) as actorid,
	coalesce(ly.films, array[]::film[])
	||
	case
		when ty.film is not null then
			array[row(
				ty.film,
				ty.votes,
				ty.rating,
				ty.filmid
			)::film]
		else array[]::film[]
	end as films,
	case
		when ty_rate.avg_rating is not null then
		(
			case
				when ty_rate.avg_rating > 8 then 'star'
				when ty_rate.avg_rating > 7 then 'good'
				when ty_rate.avg_rating > 6 then 'average'
				else 'bad'
			end
		)::quality_class
		else ly.quality_class
	end as quality_class,
	ty.year is not null as is_active,
	1970 as current_year
from last_year ly
left join this_year_rate ty_rate
on ly.actorid = ty_rate.actorid
full outer join this_year ty
on ly.actorid = ty.actorid;

-- actorid  |films                                                             |quality_class|is_active|current_year|
-- ---------+------------------------------------------------------------------+-------------+---------+------------+
-- nm0000003|{"(The Bear and the Doll,431,6.4,tt0064779)"}                     |             |true     |        1970|
-- nm0000003|{"(Les novices,219,5.1,tt0066164)"}                               |             |true     |        1970|
-- nm0000006|{"(A Walk in the Spring Rain,696,6.2,tt0066542)"}                 |             |true     |        1970|
-- nm0000012|{"(Connecting Rooms,585,6.9,tt0066943)"}                          |             |true     |        1970|
-- nm0000014|{"(The Adventurers,656,5.5,tt0065374)"}                           |             |true     |        1970|
-- nm0000018|{"(There Was a Crooked Man...,4138,7.0,tt0066448)"}               |             |true     |        1970|
-- nm0000020|{"(There Was a Crooked Man...,4138,7.0,tt0066448)"}               |             |true     |        1970|
-- nm0000020|{"(The Cheyenne Social Club,4085,6.9,tt0065542)"}                 |             |true     |        1970|
