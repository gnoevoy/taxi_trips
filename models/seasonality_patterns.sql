-- Seasonality Patterns
-- Taxi Demand by Hour and Day of Week

with cte as (
    select
        hour,
        day_name,
        day_of_week_num,
        count(*) as trips_count,

    from {{ ref('source_data') }}
    group by 1, 2, 3
),

result as (
    select *,
        -- added pct of trips by hour + ranking for days
        round(trips_count / sum(trips_count) over (partition by hour) * 100, 2) as trips_pct_by_hour,
        row_number() over (partition by hour order by trips_count desc) as day_rank
    from cte
    order by hour, day_of_week_num
)


-- Extra task: Identify for each day the best hour by number of trips 
select 
    day_of_week_num,
    day_name,
    hour as best_hour_by_trips,
from result 
where day_rank = 1
order by day_of_week_num asc, best_hour_by_trips asc





