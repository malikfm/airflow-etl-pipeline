-- Get latest state for each order item by taking the most recent batch_id
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
