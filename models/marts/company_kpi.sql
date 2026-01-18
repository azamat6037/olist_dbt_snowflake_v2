SELECT
    date_trunc('month', order_purchase_timestamp) as months,
    
    -- Main KPI: Total Revenue (GMV)
    SUM(final_order_value) AS total_gmv,
    
    -- Additional KPI 1: Total Order Volume
    COUNT(DISTINCT order_id) AS total_orders,
    
    -- Additional KPI 2: Average Order Value (AOV)
    -- Using DIV0 to handle potential division by zero errors safely
    DIV0(SUM(final_order_value), COUNT(DISTINCT order_id)) AS average_order_value,
    
    -- Additional KPI 3: Perfect Order Rate %
    -- Logic: Delivered + On Time + Score >= 4
    ROUND(
        (COUNT(CASE 
            WHEN order_delivered_customer_date <= order_estimated_delivery_date 
                 AND avg_score >= 4 
            THEN 1 
         END) 
         / COUNT(order_id)) * 100, 2
    ) AS perfect_order_rate_pct,
    
    -- Additional KPI 4: Total Active Customers
    COUNT(DISTINCT customer_unique_id) AS active_customers
FROM
    {{ref('int_company_kpi')}}
GROUP BY ALL