-- Source table for conducting further analysis

with cte as (
    select 
        row_number() over() as ride_id,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,

        -- Extracted date and time features
        extract(year from trip_start_timestamp) AS year,
        extract(month from trip_start_timestamp) AS month_num,
        format_timestamp('%B', trip_start_timestamp) AS month_name,
        format_timestamp('%A', trip_start_timestamp) AS day_name,
        cast(format_date('%u', trip_start_timestamp) as numeric) as day_of_week_num,
        extract(hour from trip_start_timestamp) AS hour,

        round(safe_divide(trip_seconds, 60), 2) AS trip_minutes,
        trip_miles,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        company,

        -- payment method buckets
        case when payment_type not in ("Cash", "Credit Card") then "Other" else payment_type end as payment_method,

    from {{ source('taxi_data', 'taxi_trips') }}

    -- remove outliers based on eda and observations in Excel
    where safe_divide(trip_seconds, 60) > 0
        and trip_miles > 0 and trip_miles < 200
        and fare > 0 and round(safe_divide(fare, trip_miles),2) < 50 

    limit 2000000
)

select *,
    -- day type flag
    case when day_of_week_num in (6, 7) then 0 else 1 end as is_workday,

    -- time of day buckets
    case when hour >= 5 and hour < 12 then 'Morning'
        when hour >= 12 and hour < 17 then 'Afternoon'
        when hour >= 17 and hour < 21 then 'Evening'
        else 'Night'
    end as time_of_day,

    -- trip duration buckets
    case when trip_minutes < 5 then '0-5 min'
        when trip_minutes < 10 then '5-10 min'
        when trip_minutes < 20 then '10-20 min'
        when trip_minutes < 30 then '20-30 min'
        when trip_minutes < 60 then '30-60 min'
        else '60+ min'
    end as duration_bucket,

    -- trip distance buckets
    case when trip_miles < 1 then '0-1 mile'
        when trip_miles < 3 then '1-3 mile'
        when trip_miles < 5 then '3-5 mile'
        when trip_miles < 10 then '5-10 mile'
        when trip_miles < 20 then '10-20 mile'
        else '20+ mile'
    end as distance_bucket,

from cte
order by ride_id asc

