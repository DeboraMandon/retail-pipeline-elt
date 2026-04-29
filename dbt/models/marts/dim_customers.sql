-- dim_customers.sql
-- Dimension clients — une ligne par client unique

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_id,
        count(*)                as order_count,
        sum(p.payment_value)    as total_spent,
        min(o.order_purchased_at) as first_order_at,
        max(o.order_purchased_at) as last_order_at
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_order_payments') }} p on o.order_id = p.order_id
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        c.customer_zip_code_prefix,
        coalesce(o.order_count, 0)   as order_count,
        coalesce(o.total_spent, 0)   as total_spent,
        o.first_order_at,
        o.last_order_at
    from customers c
    left join orders o on c.customer_id = o.customer_id
)

select * from final