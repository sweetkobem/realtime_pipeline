INSERT INTO fact_order_detail
SELECT
    oi.order_id,
    toDate(oi.created_time) AS date_key,
    p.product_key,
    c.category_key,
    oi.quantity,
    oi.price_amount

FROM silver_ecommerce_order_item oi

LEFT JOIN dim_product p
    ON (oi.product_id=p.product_id
        AND p.start_date <= toDate('{execution_date}')
            AND (p.end_date > toDate('{execution_date}')
                OR p.is_current = 1))

LEFT JOIN dim_category c
    ON (p.category_id=c.category_id
        AND c.start_date <= toDate('{execution_date}')
            AND (c.end_date > toDate('{execution_date}')
                OR c.is_current = 1))

WHERE toDate(oi.created_time) = toDate('{execution_date}')