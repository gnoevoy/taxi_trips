-- Test vertions right now

select 
    taxi_id,
    count(distinct category) as category_count
from (
    select 
        taxi_id,
        case when company is null then "private"  else "company" end as category,
    from {{ ref('source_data') }}
)
group by 1
order by 2 desc

