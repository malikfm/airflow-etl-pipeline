-- Get latest state for each order by taking the most recent batch_id
with ranked as (
    select
        id,
        user_id,
        status,
        created_at,
        updated_at,
        row_number() over (partition by id order by batch_id desc) as rn
    from {{ source('raw_ingest', 'orders') }}
)

select
    id,
    user_id,
    status,
    created_at,
    updated_at
from ranked
where rn = 1
