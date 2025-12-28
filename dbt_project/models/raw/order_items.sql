select
    id,
    order_id,
    product_id,
    quantity
from {{ source('staging', 'order_items') }}
