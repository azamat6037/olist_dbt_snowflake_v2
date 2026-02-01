-- EASY

-- Umumiy buyurtmalar soni qancha?
select count(order_id), count(distinct order_id)
from
    olist_orders;

-- Qaysi shtatdan eng ko'p xaridorlar ro'yxatdan o'tgan?
select 
    customer_state,
    count(customer_id),
    count(customer_unique_id)
from
    olist_customers
group by all
order by 3 desc;


-- Mahsulotlarning o'rtacha og'irligi qancha?
select
    avg(product_weight_g/1000) as weight_kg
from OLIST_RAW.PUBLIC.OLIST_PRODUCTS

-- ##################




-- MEDIUM

-- Eng ko'p sotilgan mahsulot kategoriyalari qaysilar?
select 
    eng.PRODUCT_CATEGORY_NAME_ENGLISH, 
    count(*) as n
from
    olist_orders as orders
inner join
    olist_items as items
    on orders.order_id = items.order_id
inner join
    olist_products as products
    on products.product_id = items.product_id
inner join
    product_category_name_translation as eng
    on eng.product_category_name = products.product_category_name
where
    order_status not in ('canceled', 'unavailable')
group by 1
order by 2 desc
limit 10
-- bed_bath_table	11097



-- Buyurtmalarning o'rtacha qiymati (AOV) qancha?
-- v1 - xato
select avg(payment_value) as aov
from
    olist_orders
inner join
    olist_payments
    on olist_orders.order_id = olist_payments.order_id
-- OAV = 154

-- v2
with order_value as (
select
    olist_orders.order_id,
    --count(*) as n
    sum(payment_value) as order_value
from
    olist_orders
inner join
    olist_payments
-- where order_id='fa65dad1b0e818e3ccc5cb0e39231352'
    on olist_orders.order_id = olist_payments.order_id
group by 1
)

select
    avg(order_value) as avo
from
    order_value
-- AVO = 161



-- Qaysi to'lov turi eng ommabop?
with payments as (
    select
        order_id,
        payment_type
    from
        olist_payments
    group by 1,2
)

select
    payment_type,
    count(*) as n
from
    payments
group by 1
order by 2 desc
-- credit_card	76505


-- Yetkazib berish o'rtacha necha kun?
select 
    avg(datediff('day', order_purchase_timestamp, order_delivered_customer_date)) as avg_delivery_days
from
    olist_orders
where
    order_status = 'delivered'
-- avg delivery days 12.5 days

-- bonus
select
    customer_state,
    avg(datediff('day', order_purchase_timestamp, order_delivered_customer_date)) as avg_delivery_days
from
    olist_orders
inner join
    olist_customers
    on olist_orders.customer_id = olist_customers.customer_id
where
    order_status = 'delivered'
group by 1
order by 2 desc

-- ###########################



-- HARD

-- Qaysi sotuvchilar (sellers) eng yuqori daromad keltirmoqda?
select
    s.seller_id,
    sum(PRICE) as sum_price,
    sum(freight_value) as sum_freight_value
from
    olist_orders as o
inner join
    olist_items as i
    on i.order_id = o.order_id
inner join
    olist_sellers as s
    on s.seller_id = i.seller_id
where
    order_status not in ('canceled', 'unavailable')
group by 1
order by 2 desc
limit 10


-- "Kechikkan" buyurtmalar foizi qancha?
with orders_w_late_delivery_col as (
select
    *,
    case when order_delivered_customer_date::date > order_estimated_delivery_date::date then 1 else 0 end as late_delivery
from
    olist_orders
where
    order_status = 'delivered'
    and order_delivered_customer_date IS NOT NULL
)

select
    round(sum(late_delivery*100)/count(*), 2) as late_share,
    count(*)
from
    orders_w_late_delivery_col
-- 6.77%


-- Xaridorlarning sodiqligi (Retention): Necha foiz odam qayta xarid qilgan?
with cust_base as (
select
    customer_unique_id,
    count(order_id) as n
from
    olist_orders as o
inner join
    olist_customers as c
    on o.customer_id = c.customer_id
group by all
)

select
    round(sum(case when n>1 then 1 else 0 end)*100/count(*), 2) as retention
from
    cust_base
-- 3.12%