INSERT INTO fact_order
-- Created Data --
SELECT
    o.order_id,
    u.user_key,
    toDate(o.created_time) AS date_key,
    'created' AS order_status,
    o.created_time AS order_created_time,
    NULL AS order_completed_time,
    p.payment_method,
    s.postal_code,
    s.country,
    s.city,
    o.total_amount
    
FROM silver_ecommerce_order o
LEFT JOIN dim_user u
    ON (o.user_id=u.user_id)
    
LEFT JOIN silver_ecommerce_shipping s
    ON (o.order_id=s.order_id)
    
LEFT JOIN silver_ecommerce_payment p
    ON (o.order_id=p.order_id)

WHERE toDate(o.created_time) = toDate('{execution_date}')
    AND (u.start_date <= toDate(o.created_time)
        AND (u.end_date > toDate(o.created_time) OR u.is_current = 1))

-- Completed Data --
UNION ALL
SELECT
    o.order_id,
    u.user_key,
    toDate(o.updated_time) AS date_key,
    o.order_status,
    o.created_time AS order_created_time,
    o.updated_time AS order_completed_time,
    p.payment_method,
    s.postal_code,
    s.country,
    s.city,
    o.total_amount
    
FROM silver_ecommerce_order o
LEFT JOIN dim_user u
    ON (o.user_id=u.user_id)
    
LEFT JOIN silver_ecommerce_shipping s
    ON (o.order_id=s.order_id)
    
LEFT JOIN silver_ecommerce_payment p
    ON (o.order_id=p.order_id)

WHERE toDate(o.updated_time) = toDate('{execution_date}')
    AND (u.start_date <= toDate(o.created_time)
        AND (u.end_date > toDate(o.created_time) OR u.is_current = 1))
	AND o.order_status = 'completed'
	
-- Cancel Data --
UNION ALL
SELECT
    o.order_id,
    u.user_key,
    toDate(o.updated_time) AS date_key,
    order_status,
    o.created_time AS order_created_time,
    NULL AS order_completed_time,
    p.payment_method,
    s.postal_code,
    s.country,
    s.city,
    o.total_amount
    
FROM silver_ecommerce_order o
LEFT JOIN dim_user u
    ON (o.user_id=u.user_id)
    
LEFT JOIN silver_ecommerce_shipping s
    ON (o.order_id=s.order_id)
    
LEFT JOIN silver_ecommerce_payment p
    ON (o.order_id=p.order_id)

WHERE toDate(o.updated_time) = toDate('{execution_date}')
    AND (u.start_date <= toDate(o.created_time)
        AND (u.end_date > toDate(o.created_time) OR u.is_current = 1))
    AND o.order_status = 'cancelled'
