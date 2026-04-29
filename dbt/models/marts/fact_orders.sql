-- fact_orders.sql
-- Table de faits centrale — une ligne par commande
-- Joins : orders + order_items + order_payments + order_reviews

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        count(*)                    as item_count,
        sum(price)                  as total_price,
        sum(freight_value)          as total_freight
    from {{ ref('stg_order_items') }}
    group by order_id
),

order_payments as (
    select
        order_id,
        sum(payment_value)          as total_payment,
        count(distinct payment_type) as payment_type_count
    from {{ ref('stg_order_payments') }}
    group by order_id
),

order_reviews as (
    select distinct on (order_id)
        order_id,
        review_score,
        review_created_at
    from {{ ref('stg_order_reviews') }}
    order by order_id, review_created_at desc
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchased_at,
        o.order_approved_at,
        o.order_delivered_carrier_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,
        -- metriques commande
        coalesce(i.item_count, 0)       as item_count,
        coalesce(i.total_price, 0)      as total_price,
        coalesce(i.total_freight, 0)    as total_freight,
        coalesce(p.total_payment, 0)    as total_payment,
        -- delais (en jours)
        extract(epoch from (o.order_delivered_customer_at - o.order_purchased_at))
            / 86400                     as delivery_days,
        extract(epoch from (o.order_estimated_delivery_at - o.order_delivered_customer_at))
            / 86400                     as delay_vs_estimate_days,
        -- satisfaction
        r.review_score
    from orders o
    left join order_items i     on o.order_id = i.order_id
    left join order_payments p  on o.order_id = p.order_id
    left join order_reviews r   on o.order_id = r.order_id
)

select * from final