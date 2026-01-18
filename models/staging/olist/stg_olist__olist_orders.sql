with 

source as (

    select * from {{ source('olist', 'olist_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date

    from source
    where
        order_status = 'delivered'

)

select * from renamed