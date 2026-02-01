CREATE DATABASE OLIST_RAW;
create schema OLIST_RAW.DATA;
use schema olist_raw.data;

-- Step 1: Create a File Format 
-- This tells Snowflake how to read the CSVs (skip header, handle quotes)
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '');

-- Step 2: Create the "Stage" (The Bridge to S3)
-- This creates a pointer to your specific bucket
CREATE OR REPLACE STAGE my_olist_stage
URL='s3://olist-data-da-course/'
  FILE_FORMAT = my_csv_format;

-- Step 3: Test the Connection
-- Run this line. If you see a list of files, IT WORKS.
LIST @my_olist_stage;

-- Step 4: Load the Data (COPY INTO)
-- Run these one by one or all together. 
-- Make sure the file names match EXACTLY what you see in the LIST command result.

CREATE OR REPLACE TABLE olist_orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- Orders
COPY INTO olist_orders 
FROM @my_olist_stage/olist_orders_dataset.csv;




-- ITEMS (Watch out for the FLOAT price)
CREATE OR REPLACE TABLE olist_items (
    order_id VARCHAR(32),
    order_item_id INT,
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT
);

-- Items
COPY INTO olist_items 
FROM @my_olist_stage/olist_order_items_dataset.csv;





-- CUSTOMERS (Zip code must be VARCHAR, not INT!)
CREATE OR REPLACE TABLE olist_customers (
    customer_id VARCHAR(32),
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

-- Customers
COPY INTO olist_customers 
FROM @my_olist_stage/olist_customers_dataset.csv;




-- PRODUCTS
CREATE OR REPLACE TABLE olist_products (
    product_id VARCHAR(32),
    product_category_name VARCHAR(100),
    product_name_lenght INT, -- Misspelled in source data, keep it or fix it? Keep for realism.
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- Products
COPY INTO olist_products 
FROM @my_olist_stage/olist_products_dataset.csv;




-- REVIEWS (The messy one)
CREATE OR REPLACE TABLE olist_reviews (
    review_id VARCHAR(32),
    order_id VARCHAR(32),
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message VARCHAR(4000), -- Needs to be long
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- Reviews (This is the hardest one, check for errors here)
COPY INTO olist_reviews 
FROM @my_olist_stage/olist_order_reviews_dataset.csv;



-- SELLERS
CREATE OR REPLACE TABLE olist_sellers (
    seller_id VARCHAR(32),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5)
);

-- Sellers
COPY INTO olist_sellers 
FROM @my_olist_stage/olist_sellers_dataset.csv;



-- PAYMENTS
CREATE OR REPLACE TABLE olist_payments (
    order_id VARCHAR(32),
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value FLOAT
);


-- Payments
COPY INTO olist_payments 
FROM @my_olist_stage/olist_order_payments_dataset.csv;




-- GEOLOCATION (Heavy table)
CREATE OR REPLACE TABLE olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(5)
);

-- Geolocation
COPY INTO olist_geolocation 
FROM @my_olist_stage/olist_geolocation_dataset.csv;





-- TRANSLATION
CREATE OR REPLACE TABLE product_category_name_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);


-- Translation
COPY INTO product_category_name_translation 
FROM @my_olist_stage/product_category_name_translation.csv;