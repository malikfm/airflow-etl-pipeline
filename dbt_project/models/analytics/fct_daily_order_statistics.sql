{{ config(unique_key='date') }}

with date_info as (
    select
        coalesce(max(date), current_date - 1) + 1 as date
    from {{ this }}
),

_orders as (
    select
        id,
        user_id,
        status,
        created_at,
        updated_at
    from {{ ref('raw_current', 'orders') }}, date_info
    {% if is_incremental() %}
    where updated_at > date_info.date
    {% endif %}
),

completed_orders as (
    select
        id
    from _orders
    where status = 'completed'
),

daily_new_orders as (
    select
        count(*) as daily_new_orders,
    from _orders, date_info
    where created_at > date_info.date
),

daily_order_status as (
    select
        count(if(status = 'pending', 1, null)) as daily_pending_orders,
        count(if(status = 'shipped', 1, null)) as daily_shipped_orders,
        count(if(status = 'completed', 1, null)) as daily_completed_orders,
        count(if(status = 'cancelled', 1, null)) as daily_cancelled_orders,
    from _orders
),

daily_gmv as (
    select
        sum(oi.quantity * p.price) as daily_gmv
    from {{ ref('raw_current', 'order_items') }} oi
    join completed_orders co on oi.order_id = co.id
    join {{ ref('raw_current', 'products') }} p on oi.product_id = p.id
),

prev_total as (
    select
        total_completed_orders,
        total_cancelled_orders,
        total_orders,
        total_gmv
    from {{ this }}
    where date = date_info.date
)

select
    date_info.date,
    -- Orders
    daily_new_orders.daily_new_orders,
    daily_order_status.daily_pending_orders,
    daily_order_status.daily_shipped_orders,
    daily_order_status.daily_completed_orders,
    daily_order_status.daily_cancelled_orders,
    if(
        prev_total.total_completed_orders is null,
        daily_order_status.daily_completed_orders,
        prev_total.total_completed_orders + daily_order_status.daily_completed_orders
    ) as total_completed_orders,
    if(
        prev_total.total_cancelled_orders is null,
        daily_order_status.daily_cancelled_orders,
        prev_total.total_cancelled_orders + daily_order_status.daily_cancelled_orders
    ) as total_cancelled_orders,
    if(
        prev_total.total_orders is null,
        daily_order_status.daily_new_orders,
        prev_total.total_completed_orders + daily_order_status.daily_new_orders
    ) as total_orders,
    -- GMV
    daily_gmv.daily_gmv,
    if(
        prev_total.total_gmv is null,
        daily_gmv.daily_gmv,
        prev_total.total_gmv + daily_gmv.daily_gmv
    ) as total_gmv
from date_info, daily_new_orders, daily_order_status, daily_gmv, prev_total
