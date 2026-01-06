-- Process specific date
{% set snapshot_date = var('snapshot_date') %}
{% if is_incremental() %}

select
    id,
    order_id,
    product_id,
    quantity
from {{ source('raw_ingest', 'order_items') }}
where batch_id::date = '{{ snapshot_date }}'

-- Full refresh, get latest state for each item by taking the most recent batch_id
{% else %}

with ranked as (
    select
        id,
        order_id,
        product_id,
        quantity,
        row_number() over (partition by id order by batch_id desc) as rn
    from {{ source('raw_ingest', 'order_items') }}
)
select
    id,
    order_id,
    product_id,
    quantity
from ranked
where rn = 1

{% endif %}