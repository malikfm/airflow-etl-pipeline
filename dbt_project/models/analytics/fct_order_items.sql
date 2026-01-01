{{ config(unique_key='order_item_id') }}

with _orders as (
    select
        id,
        user_id,
        created_at,
        updated_at
    from {{ ref('raw_current', 'orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
)

select
    oi.id as order_item_id,
    oi.order_id,
    o.user_id,
    oi.product_id,
    oi.quantity as order_qty,
    p.price as product_price,
    (oi.quantity * p.price) as order_subtotal,
    o.created_at,
    o.updated_at
from {{ ref('raw_current', 'order_items') }} oi
join _orders o on oi.order_id = o.id
join {{ ref('raw_current', 'products') }} p on oi.product_id = p.id
