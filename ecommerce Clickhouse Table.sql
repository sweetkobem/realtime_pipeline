-- Bronze --
-- bronze_hot_ecommerce_category
CREATE TABLE bronze_hot_ecommerce_category (
    category_id UInt64,
    name String,
    parent_id Nullable(UInt64),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_hot_ecommerce_product
CREATE TABLE bronze_hot_ecommerce_product (
    product_id UInt64,
    name String,
    description String,
    price Decimal64(2),
    stock_quantity UInt32,
    category_id UInt64,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_hot_ecommerce_user
CREATE TABLE bronze_hot_ecommerce_user
(
    user_id UInt64,
    name String,
    email String,
    password_hash String,
    phone String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_hot_ecommerce_order
CREATE TABLE bronze_hot_ecommerce_order (
    order_id UInt64,
    user_id UInt64,
    order_status String,
    total_amount Decimal64(2),
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_hot_ecommerce_order_item
CREATE TABLE bronze_hot_ecommerce_order_item (
    order_item_id UInt64,
    order_id UInt64,
    product_id UInt64,
    quantity UInt32,
    price Decimal64(2),
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_hot_ecommerce_payment
CREATE TABLE bronze_hot_ecommerce_payment (
    payment_id UInt64,
    order_id UInt64,
    amount Decimal64(2),
    payment_method String,
    payment_date DateTime64(3),
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_hot_ecommerce_shipping
CREATE TABLE bronze_hot_ecommerce_shipping (
    shipping_id UInt64,
    order_id UInt64,
    address String,
    city String,
    postal_code String,
    country String,
    shipping_date DateTime64(3),
    tracking_number String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_hot_ecommerce_product_review
CREATE TABLE bronze_hot_ecommerce_product_review (
    review_id UInt64,
    product_id UInt64,
    user_id UInt64,
    rating UInt8,
    comment String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at);

-- bronze_cold_ecommerce_category
CREATE TABLE bronze_cold_ecommerce_category (
    category_id UInt64,
    name String,
    parent_id Nullable(UInt64),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- bronze_cold_ecommerce_product
CREATE TABLE bronze_cold_ecommerce_product (
    product_id UInt64,
    name String,
    description String,
    price Decimal64(2),
    stock_quantity UInt32,
    category_id UInt64,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- bronze_cold_ecommerce_user
CREATE TABLE bronze_cold_ecommerce_user
(
    user_id UInt64,
    name String,
    email String,
    password_hash String,
    phone String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    deleted_at Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- bronze_cold_ecommerce_order
CREATE TABLE bronze_cold_ecommerce_order (
    order_id UInt64,
    user_id UInt64,
    order_status String,
    total_amount Decimal64(2),
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- bronze_cold_ecommerce_order_item
CREATE TABLE bronze_cold_ecommerce_order_item (
    order_item_id UInt64,
    order_id UInt64,
    product_id UInt64,
    quantity UInt32,
    price Decimal64(2),
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- bronze_cold_ecommerce_payment
CREATE TABLE bronze_cold_ecommerce_payment (
    payment_id UInt64,
    order_id UInt64,
    amount Decimal64(2),
    payment_method String,
    payment_date DateTime64(3),
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- bronze_cold_ecommerce_shipping
CREATE TABLE bronze_cold_ecommerce_shipping (
    shipping_id UInt64,
    order_id UInt64,
    address String,
    city String,
    postal_code String,
    country String,
    shipping_date DateTime64(3),
    tracking_number String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- bronze_cold_ecommerce_product_review
CREATE TABLE bronze_cold_ecommerce_product_review (
    review_id UInt64,
    product_id UInt64,
    user_id UInt64,
    rating UInt8,
    comment String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_at)
ORDER BY (updated_at)
SETTINGS storage_policy = 'remote';

-- Silver --
-- silver_cold_ecommerce_category
CREATE TABLE silver_cold_ecommerce_category (
    category_id UInt64,
    name String,
    parent_id Nullable(UInt64),
    created_time DateTime64(3),
    updated_time DateTime64(3),
    deleted_time Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- silver_cold_ecommerce_product
CREATE TABLE silver_cold_ecommerce_product (
    product_id UInt64,
    name String,
    description String,
    price_amount Decimal64(2),
    stock_quantity UInt32,
    category_id UInt64,
    created_time DateTime64(3),
    updated_time DateTime64(3),
    deleted_time Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- silver_cold_ecommerce_user
CREATE TABLE silver_cold_ecommerce_user
(
    user_id UInt64,
    name String,
    email String,
    phone String,
    created_time DateTime64(3),
    updated_time DateTime64(3),
    deleted_time Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- silver_cold_ecommerce_order
CREATE TABLE silver_cold_ecommerce_order (
    order_id UInt64,
    user_id UInt64,
    order_status String,
    total_amount Decimal64(2),
    created_time DateTime64(3),
    updated_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- silver_cold_ecommerce_order_item
CREATE TABLE silver_cold_ecommerce_order_item (
    order_item_id UInt64,
    order_id UInt64,
    product_id UInt64,
    quantity UInt32,
    price_amount Decimal64(2),
    created_time DateTime64(3),
    updated_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- silver_cold_ecommerce_payment
CREATE TABLE silver_cold_ecommerce_payment (
    payment_id UInt64,
    order_id UInt64,
    amount Decimal64(2),
    payment_method String,
    payment_time DateTime64(3),
    status String,
    created_time DateTime64(3),
    updated_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- silver_cold_ecommerce_shipping
CREATE TABLE silver_cold_ecommerce_shipping (
    shipping_id UInt64,
    order_id UInt64,
    address String,
    city String,
    postal_code String,
    country String,
    shipping_time DateTime64(3),
    tracking_number String,
    created_time DateTime64(3),
    updated_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- silver_cold_ecommerce_product_review
CREATE TABLE silver_cold_ecommerce_product_review (
    review_id UInt64,
    product_id UInt64,
    user_id UInt64,
    rating UInt8,
    comment String,
    created_time DateTime64(3),
    updated_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(updated_time)
ORDER BY (updated_time)
SETTINGS storage_policy = 'remote';

-- Gold Non Aggregated --
-- dim_date
CREATE TABLE dim_date (
    date_key Date,
    start_day_of_week Date,
    start_day_of_month Date,
    quarter UInt8,
    year UInt16,
    is_weekend UInt8,
    day_name String,
    month_name String
)
ENGINE MergeTree()
ORDER BY date_key;

-- Populate dim_date
INSERT INTO dim_date
SELECT
    date_key,
    toMonday(date_key) AS start_day_of_week,
    toStartOfMonth(date_key) AS start_day_of_month,
    toQuarter(date_key) AS quarter,
    toYear(date_key) AS year,
    IF(toDayOfWeek(date_key) IN (6, 7), 1, 0) AS is_weekend,

    -- Day name (manual mapping)
    CASE toDayOfWeek(date_key)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_name,

    -- Month name (manual mapping)
    CASE toMonth(date_key)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name

FROM (
    SELECT toDate('2025-01-01') + number AS date_key
    FROM numbers(27393)  -- 2025-01-01 to 2099-12-31
) AS populate_data;


-- dim_user
CREATE TABLE dim_user (
    user_key String,
    user_id UInt64,
    name String,
    email String,
    phone String,
    created_time DateTime64(3),
    start_date Date,
    end_date Nullable(Date),
    is_current UInt8
)
ENGINE = ReplacingMergeTree(start_date)
ORDER BY (user_id, start_date);

-- dim_category
CREATE TABLE dim_category (
    category_key String,
    category_id UInt64,
    name String,
    parent_id UInt64,
    start_date Date,
    end_date Nullable(Date),
    is_current UInt8
)
ENGINE = ReplacingMergeTree(start_date)
ORDER BY (category_id, start_date);

-- dim_product
CREATE TABLE dim_product (
    product_key String,
    product_id UInt64,
    name String,
    description String,
    price_amount Decimal64(2),
    category_id UInt64,
    stock_quantity UInt64,
    start_date Date,
    end_date Nullable(Date),
    is_current UInt8
)
ENGINE = ReplacingMergeTree(start_date)
ORDER BY (product_id, start_date);

-- fact_order
CREATE TABLE fact_order (
    order_id UInt64,
    user_key String,
    date_key Date,
    order_status LowCardinality(String),
    order_created_time DateTime64(3),
    order_completed_time Nullable(DateTime64(3)),
    payment_method LowCardinality(Nullable(String)),
    postal_code String,
    country String,
    city String,
    total_amount Decimal64(2)
)
ENGINE = MergeTree()
PARTITION BY date_key
ORDER BY (date_key, order_id, user_key);

-- fact_order_detail
CREATE TABLE fact_order_detail (
    order_id UInt64,
    date_key Date,
    product_key String,
    category_key String,
    quantity UInt32,
    price_amount Decimal64(2)
)
ENGINE = MergeTree()
PARTITION BY date_key
ORDER BY (date_key, order_id);

-- Gold Aggregated --
-- daily_transaction
CREATE TABLE daily_transaction (
    date_key Date,
    order_status LowCardinality(String),
    payment_method LowCardinality(Nullable(String)),
    category String,
    country String,
    city String,
    total_order UInt32,
    total_user UInt32,
    items_sold UInt32,
    total_revenue Decimal64(2)
)
ENGINE = MergeTree()
PARTITION BY date_key
ORDER BY (date_key, order_status, country);

-- daily_user
CREATE TABLE daily_user (
    date_key Date,
    active_user UInt32,
    new_user UInt32
)
ENGINE = MergeTree()
PARTITION BY date_key
ORDER BY (date_key);