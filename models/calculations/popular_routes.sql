-- Popular Routes Analysis
-- Top 10 most popular routes 

select 
    -- create route name 
    cast(pickup_community_area as string) || ' - ' || cast(dropoff_community_area as string) as route_name,

    -- aggregations
    count(*) as trips_count,
    sum(trip_total) as revenue,
    round(sum(trip_total) / count(*),2) as avg_revenue_per_trip,
    round(avg(trip_minutes),2) as avg_trip_duration_minutes,
    round(avg(trip_miles),2) as avg_trip_distance_miles,
    round(sum(tips) / sum(fare), 2) as tip_rate,
    round(sum(case when tips > 0 then 1 else 0 end) / count(*),2) as tip_frequency,

from {{ ref('source_data') }}
where pickup_community_area is not null and dropoff_community_area is not null
group by 1
order by trips_count desc
limit 10