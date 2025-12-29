{{ config(unique_key='order_id') }}

with _orders as (
    select
        id,
        user_id,
        created_at,
        updated_at
    from {{ ref('raw', 'orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

_order_items as (
    select
        oi.order_id,
        oi.product_id,
        oi.quantity
    from {{ ref('raw', 'order_items') }} oi
    join _orders o on oi.order_id = o.id
),

order_agg as (
    select
        oi.order_id,
        sum(oi.quantity * p.price) as order_total,
        count(*) as order_item_count
    from _order_items oi
    join {{ ref('raw', 'products') }} p on oi.product_id = p.id
    group by oi.order_id
),

select
    oa.order_id,
    o.user_id,
    oa.order_total,
    oa.order_item_count,
    o.created_at,
    o.updated_at
from _orders o
join order_agg oa on o.id = oa.order_id
