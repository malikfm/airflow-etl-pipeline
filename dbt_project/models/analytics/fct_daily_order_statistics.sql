{{ config(unique_key='date') }}

{% set execution_date = var('execution_date') %}
with date_info as (
    select
        '{{ execution_date }}'::date as cur_date,
        '{{ execution_date }}'::date - interval '1 day' as prev_date
),

_orders as (
    select
        id,
        user_id,
        status,
        created_at,
        updated_at
    from {{ ref('orders') }}, date_info
    where updated_at >= date_info.prev_date  -- GTE yesterday 00:00:00
),

completed_orders as (
    select
        id
    from _orders
    where status = 'completed'
),

daily_new_orders as (
    select
        count(*) as daily_new_orders
    from _orders, date_info
    where created_at > date_info.prev_date  -- GTE yesterday 00:00:00
),

daily_order_status as (
    select
        count(
            case
                when status = 'pending' then 1
                else null
            end
        ) as daily_pending_orders,
        count(
            case
                when status = 'shipped' then 1
                else null
            end
        ) as daily_shipped_orders,
        count(
            case
                when status = 'completed' then 1
                else null
            end
        ) as daily_completed_orders,
        count(
            case
                when status = 'cancelled' then 1
                else null
            end
        ) as daily_cancelled_orders
    from _orders
),

daily_gmv as (
    select
        coalesce(sum(oi.quantity * p.price), 0) as daily_gmv
    from {{ ref('order_items') }} oi
    join completed_orders co on oi.order_id = co.id
    join {{ ref('products') }} p on oi.product_id = p.id
),

prev_total as (
    {% if is_incremental() %}

    select
        total_completed_orders,
        total_cancelled_orders,
        total_orders,
        total_gmv
    from {{ this }}, date_info
    where date = date_info.prev_date  -- GTE yesterday 00:00:00

    -- First run only (non-incremental)
    {% else %}
    
    with all_time_order_status as (
        
        select
            count(
                case
                    when status = 'completed' then 1
                    else null
                end
            ) as total_completed_orders,
            count(
                case
                    when status = 'cancelled' then 1
                    else null
                end
            ) as total_cancelled_orders,
            count(*) as total_orders
        from {{ ref('orders') }}

    ),

    all_time_gmv as (
        select
            sum(oi.quantity * p.price) as total_gmv
        from {{ ref('order_items') }} oi
        join {{ ref('orders') }} o on oi.order_id = o.id and o.status = 'completed'
        join {{ ref('products') }} p on oi.product_id = p.id
    )

    select
        total_completed_orders,
        total_cancelled_orders,
        total_orders,
        coalesce(total_gmv, 0) as total_gmv
    from all_time_order_status, all_time_gmv

    {% endif %}
)

select
    date_info.cur_date as date,
    -- Orders
    daily_new_orders.daily_new_orders,
    daily_order_status.daily_pending_orders,
    daily_order_status.daily_shipped_orders,
    daily_order_status.daily_completed_orders,
    daily_order_status.daily_cancelled_orders,
    prev_total.total_completed_orders + daily_order_status.daily_completed_orders as total_completed_orders,
    prev_total.total_cancelled_orders + daily_order_status.daily_cancelled_orders as total_cancelled_orders,
    prev_total.total_orders + daily_new_orders.daily_new_orders as total_orders,
    -- GMV
    daily_gmv.daily_gmv,
    prev_total.total_gmv + daily_gmv.daily_gmv as total_gmv
from date_info, daily_new_orders, daily_order_status, daily_gmv, prev_total
