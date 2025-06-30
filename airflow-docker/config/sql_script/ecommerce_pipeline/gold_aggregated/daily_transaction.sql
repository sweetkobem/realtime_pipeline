INSERT INTO daily_transaction
SELECT
    fo.date_key,
    fo.order_status,
    fo.payment_method,
    dc.name AS category,
    fo.country,
    fo.city,
    COUNT(DISTINCT fo.order_id) AS total_order,
    COUNT(DISTINCT fo.user_key) AS total_user,
    sumIf(fd.quantity, fo.order_status = 'completed') AS items_sold,
    sumIf(fd.price_amount, fo.order_status = 'completed') AS total_revenue

FROM fact_order fo
LEFT JOIN fact_order_detail fd
    ON fo.order_id = fd.order_id
LEFT JOIN dim_category dc
    ON fd.category_key = dc.category_key
WHERE fo.date_key = '{execution_date}'
GROUP BY 1,2,3,4,5,6