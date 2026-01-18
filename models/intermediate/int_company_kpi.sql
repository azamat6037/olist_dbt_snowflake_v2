WITH -- 1. Get Revenue per Order (aggregating items first)
order_revenue AS (
    SELECT 
        order_id,
        SUM(price + freight_value) AS total_order_value
    FROM 
        {{ref('stg_olist__olist_items')}}
    GROUP BY order_id
),

-- 2. Get Review Score per Order (taking the latest or average if multiple exist)
order_reviews AS (
    SELECT 
        order_id,
        AVG(review_score) as avg_score
    FROM 
        {{ref('stg_olist__olist_reviews')}}
    GROUP BY order_id
),

-- 3. The Main Join Table (Combines Orders, Customers, Revenue, and Reviews)
joined_data AS (
    SELECT 
        o.order_id,
        c.customer_unique_id,
        o.order_status,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.order_purchase_timestamp,
        -- Bring in pre-calculated revenue (default to 0 if null)
        COALESCE(r.total_order_value, 0) AS final_order_value,
        -- Bring in pre-calculated reviews
        rev.avg_score
    FROM {{ref('stg_olist__olist_orders')}} o
    -- Join Customers to get unique ID
    LEFT JOIN {{ref('stg_olist__olist_customers')}} c 
        ON o.customer_id = c.customer_id
    -- Join Revenue CTE
    LEFT JOIN order_revenue r 
        ON o.order_id = r.order_id
    -- Join Reviews CTE
    LEFT JOIN order_reviews rev 
        ON o.order_id = rev.order_id
)

select *
from joined_data