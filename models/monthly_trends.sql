-- Timeseries Analysis
-- Ride Trends by Month and Time of Day

with result as (
    select
        year,
        month_num,
        month_name,

        -- pivoting number or trips for each daytime bucket 
        sum(case when time_of_day = 'Morning' then 1 else 0 end) as morning_trips,
        sum(case when time_of_day = 'Afternoon' then 1 else 0 end) as afternoon_trips,
        sum(case when time_of_day = 'Evening' then 1 else 0 end) as evening_trips,
        sum(case when time_of_day = 'Night' then 1 else 0 end) as night_trips,

        count(*) as trips_count,
    from {{ ref('source_data') }}
    group by 1, 2, 3
)

-- Extra task: Create moving averages for number of rides by 3 and 6 months
select *,
    round(avg(trips_count) over (partition by year order by month_num asc rows between 2 preceding and current row), 2) as moving_avg_3_months,
    round(avg(trips_count) over (partition by year order by month_num asc rows between 5 preceding and current row),2) as moving_avg_6_months,
from result
order by year, month_num