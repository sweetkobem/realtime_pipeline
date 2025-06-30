-- gold_hot_order_revenue
CREATE VIEW gold_hot_order_revenue AS
SELECT
    toHour(o.created_at) AS hour,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(oi.quantity) AS items_sold,
    SUM(oi.price * oi.quantity) AS revenue
FROM (
	SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_order
	)
	WHERE rn = 1
) AS o
INNER JOIN (
	SELECT *
  FROM (
      SELECT *, row_number() OVER (PARTITION BY order_item_id ORDER BY updated_at DESC) AS rn
      FROM bronze_hot_ecommerce_order_item
  )
  WHERE rn = 1
) oi ON o.order_id = oi.order_id
WHERE DATE(o.updated_at) = DATE(NOW('Asia/Jakarta'))
  AND o.order_status = 'completed'
GROUP BY 1
ORDER BY 1;

-- gold_hot_unique_user
CREATE VIEW gold_hot_unique_user AS
SELECT
  COUNT(DISTINCT user_id) AS active_users,
  COUNT(*) AS total_orders
FROM (
  SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_order
	)
	WHERE rn = 1
)
WHERE DATE(updated_at) = DATE(NOW('Asia/Jakarta'));

-- gold_hot_top_selling_product
CREATE VIEW gold_hot_top_selling_product AS
SELECT
  p.name,
  SUM(oi.quantity) AS total_quantity,
  SUM(oi.price * oi.quantity) AS revenue
FROM (
  SELECT *
  FROM (
      SELECT *, row_number() OVER (PARTITION BY order_item_id ORDER BY updated_at DESC) AS rn
      FROM bronze_hot_ecommerce_order_item
  )
  WHERE rn = 1
) oi
JOIN bronze_hot_ecommerce_product p ON oi.product_id = p.product_id
JOIN (
  SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_order
	)
	WHERE rn = 1
) o ON oi.order_id = o.order_id
WHERE DATE(o.updated_at) = DATE(NOW('Asia/Jakarta'))
  AND o.order_status = 'completed'
GROUP BY 1
ORDER BY 3 DESC
LIMIT 10;

-- gold_hot_payment_failure_rate
CREATE VIEW gold_hot_payment_failure_rate AS
SELECT
    countIf(status = 'failed') AS failed,
    COUNT(*) AS total,
    ROUND(100.0 * countIf(status = 'failed') / count(), 2) AS failed_rate
FROM (
  SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_payment
	)
	WHERE rn = 1
)
WHERE DATE(payment_date) = DATE(NOW('Asia/Jakarta'));

-- gold_hot_abnormal_order
CREATE VIEW gold_hot_abnormal_order AS
SELECT
  user_id,
  SUM(oi.price * oi.quantity) AS total_order_value
FROM (
  SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_order
	)
	WHERE rn = 1
) o
JOIN (
  SELECT *
  FROM (
      SELECT *, row_number() OVER (PARTITION BY order_item_id ORDER BY updated_at DESC) AS rn
      FROM bronze_hot_ecommerce_order_item
  )
  WHERE rn = 1
) oi ON o.order_id = oi.order_id
WHERE o.created_at >= NOW('Asia/Jakarta') - INTERVAL '15 minutes'
GROUP BY 1
HAVING total_order_value > 400; -- Define threshold

-- gold_hot_abnormal_payment
CREATE VIEW gold_hot_abnormal_payment AS
SELECT
  o.user_id,
  count(*) AS failed_count,
  toStartOfFiveMinute(p.payment_date) AS window_start,
  toStartOfFiveMinute(p.payment_date) + INTERVAL 5 MINUTE AS window_end
FROM (
  SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_payment
	)
	WHERE rn = 1
) p
LEFT JOIN (
  SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_order
	)
	WHERE rn = 1
) o
  ON p.order_id = o.order_id
WHERE p.status = 'failed'
GROUP BY
  1,
  3
HAVING failed_count >= 3; -- Define threshold

-- gold_hot_order_created_abnormal
CREATE VIEW gold_hot_order_created_abnormal AS
SELECT
  order_id,
  created_at,
  now() AS current_time,
  dateDiff('hour', created_at, NOW('Asia/Jakarta')) AS hours_since_order
FROM (
  SELECT *
	FROM (
	    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
	    FROM bronze_hot_ecommerce_order
	)
	WHERE rn = 1
)
WHERE dateDiff('hour', created_at, NOW('Asia/Jakarta')) > 1 -- Define threshold
  AND order_status = 'created'
ORDER BY 4 DESC