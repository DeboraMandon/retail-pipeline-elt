-- dim_sellers.sql
-- Dimension vendeurs — une ligne par vendeur

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

order_items as (
    select
        seller_id,
        count(distinct order_id)    as order_count,
        sum(price)                  as total_revenue,
        avg(price)                  as avg_item_price
    from {{ ref('stg_order_items') }}
    group by seller_id
),

final as (
    select
        s.seller_id,
        s.seller_city,
        s.seller_state,
        s.seller_zip_code_prefix,
        coalesce(o.order_count, 0)      as order_count,
        coalesce(o.total_revenue, 0)    as total_revenue,
        coalesce(o.avg_item_price, 0)   as avg_item_price
    from sellers s
    left join order_items o on s.seller_id = o.seller_id
)

select * from final