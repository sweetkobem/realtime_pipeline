INSERT INTO dim_product
SELECT *
FROM (
    -- New/changed rows (new version, is_current = 1)
    SELECT
        toString(xxHash64(product_id, toDate('{execution_date}'))) AS product_key,
        product_id,
        name,
        description,
        price_amount,
        category_id,
        stock_quantity,
        toDate('{execution_date}') AS start_date,
        NULL AS end_date,
        1 AS is_current
    FROM silver_ecommerce_product
    WHERE toDate(updated_time) = toDate('{execution_date}')

    UNION ALL
    -- Expire old versions (set end_date, is_current = 0)
    SELECT
        o.product_key,
        o.product_id,
        o.name,
        o.description,
        o.price_amount,
        o.category_id,
        o.stock_quantity,
        o.start_date,
        toDate('{execution_date}') AS end_date,
        0 AS is_current
    FROM dim_product o
    RIGHT JOIN silver_ecommerce_product n
        ON n.product_id = o.product_id
    WHERE o.is_current = 1
        AND toDate(n.updated_time) = toDate('{execution_date}')
) AS merged_changes;