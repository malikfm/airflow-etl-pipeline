-- Process specific date
{% set snapshot_date = var('snapshot_date') %}
{% if is_incremental() %}

select
    id,
    name,
    category,
    price,
    created_at,
    updated_at,
    deleted_at
from {{ source('raw_ingest', 'products') }}
where batch_id::date = '{{ snapshot_date }}'

-- Full refresh, get latest state for each item by taking the most recent batch_id
{% else %}

with ranked as (
    select
        id,
        name,
        category,
        price,
        created_at,
        updated_at,
        deleted_at,
        row_number() over (partition by id order by batch_id desc) as rn
    from {{ source('raw_ingest', 'products') }}
)
select
    id,
    name,
    category,
    price,
    created_at,
    updated_at,
    deleted_at
from ranked
where rn = 1

{% endif %}