-- Get latest state for each user by taking the most recent batch_id
with ranked as (
    select
        id,
        name,
        email,
        address,
        created_at,
        updated_at,
        deleted_at,
        row_number() over (partition by id order by batch_id desc) as rn
    from {{ source('raw_ingest', 'users') }}
)

select
    id,
    name,
    email,
    address,
    created_at,
    updated_at,
    deleted_at
from ranked
where rn = 1
