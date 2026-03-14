-- Noticed missing company values in some trips → treated as unreported rides.
-- Compare performance for vehicles that have both reported and unreported rides.
-- Analyze differences at vehicle level and across all such vehicles.

-- Query to answer question: Are there any vehicles with both reported and unreported rides
-- Result: yes, there are such cases

with unreported_rides as (
    select
        taxi_id,
        -- Check distinct companies for vehicle
        count(distinct case when company is not null then company else null end) as company_count,
        count(distinct case when company is null then "private" else null end) as private_count,
        -- Count trips with and without company
        sum(case when company is not null then 1 else 0 end) as company_trips,
        sum(case when company is null then 1 else 0 end) as private_trips,

        count(*) as trips_count,
    from {{ ref('source_data') }}
    group by 1
    order by private_count desc, company_count desc, trips_count desc
),

-- Identify vehicles with both reported and unreported rides
vehicles as (
    select
        taxi_id,
        count(distinct case when company is not null then "reported" else "unreported" end) as distinct_ride_types,
    from {{ ref('source_data') }}
    group by 1 
    having distinct_ride_types > 1
),

-- Vehicle Level: Compare stats for each vehicle between reported and unreported rides
stats_for_each_vehicle as (
    select
        taxi_id,
        case when company is not null then "reported" else "unreported" end as ride_type,

        -- Stats
        count(*) as trips_count,
        sum(trip_total) as revenue,
        round(sum(trip_total) / count(*),2) as avg_revenue_per_trip,
        round(avg(trip_minutes),2) as avg_trip_duration_minutes,
        round(avg(trip_miles),2) as avg_trip_distance_miles,
        round(sum(tips) / sum(fare), 2) as tip_rate,
        round(sum(case when tips > 0 then 1 else 0 end) / count(*),2) as tip_frequency,

    from {{ ref('source_data') }}
    -- Filter down vehicles (may use left join instead of subquery)
    where taxi_id in (select taxi_id from vehicles)
    group by 1, 2
    order by 1, 2 
)

-- Company Level: Compare stats for reported vs unreported rides across all vehicles
-- Only for vehicles with both types of rides to ensure fair comparison
select 
    case when company is not null then "reported" else "unreported" end as ride_type,

    -- Stats
    count(*) as trips_count,
    sum(trip_total) as revenue,
    round(sum(trip_total) / count(*),2) as avg_revenue_per_trip,
    round(avg(trip_minutes),2) as avg_trip_duration_minutes,
    round(avg(trip_miles),2) as avg_trip_distance_miles,
    round(sum(tips) / sum(fare), 2) as tip_rate,
    round(sum(case when tips > 0 then 1 else 0 end) / count(*),2) as tip_frequency,
from {{ ref('source_data') }}
-- Filter down vehicles (may use left join instead of subquery)
where taxi_id in (select taxi_id from vehicles)
group by 1
order by 1 