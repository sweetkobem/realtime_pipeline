-- Insert 5 Categories
INSERT INTO categories (name) VALUES
('Electronics'),
('Books'),
('Clothing'),
('Home & Kitchen'),
('Toys & Games');

-- Insert 30 Products (6 per category)
INSERT INTO products (name, description, price, stock_quantity, category_id)
SELECT 
    CONCAT('Product ', g * 6 + i),
    CONCAT('Description for product ', g * 6 + i),
    ROUND((RANDOM() * 100 + 10)::NUMERIC, 2),
    FLOOR(RANDOM() * 100 + 10),
    g + 1
FROM generate_series(0, 4) AS g,
     generate_series(1, 6) AS i;

-- Insert 2000 User with random users
DO $$
BEGIN
  FOR i IN 2..2001 LOOP
    INSERT INTO users (name, email, password_hash, phone, created_at, updated_at, deleted_at)
    VALUES (
      'User ' || i,
      'user' || i || '@example.com',
      'hashed_password_' || i,
      '0812' || LPAD(i::text, 6, '0'),
      NOW(),
      NOW(),
      NULL
    );
  END LOOP;
END;
$$;

-- Insert 500 Orders from 2025-06-14 to 2025-06-16 with random users and total_amount (user_id from 2 to 2001)
INSERT INTO orders (user_id, order_status, total_amount, created_at, updated_at)
SELECT
    FLOOR(RANDOM() * 2000 + 2)::BIGINT,  -- Random user_id between 2 and 2001
    CASE 
        WHEN RANDOM() < 0.33 THEN 'created'
        WHEN RANDOM() < 0.66 THEN 'cancelled'
        ELSE 'completed'
    END,
    ROUND((RANDOM() * 300 + 50)::NUMERIC, 2),  -- total_amount between 50 and 350
    ts AS created_at,
    ts + (RANDOM() * INTERVAL '1 hours') AS updated_at  -- updated_at within 6 hours
FROM (
    SELECT 
        TIMESTAMP '2025-06-14 00:00:00' 
        + RANDOM() * (TIMESTAMP '2025-06-16 23:59:59' - TIMESTAMP '2025-06-14 00:00:00') AS ts
    FROM generate_series(1, 500)
) AS t;

-- Insert 1–3 order_items per order
INSERT INTO order_items (order_id, product_id, quantity, price, created_at, updated_at)
SELECT
    order_id,
    FLOOR(RANDOM() * 30 + 1)::BIGINT,
    FLOOR(RANDOM() * 3 + 1)::INT,
    ROUND((RANDOM() * 100 + 10)::NUMERIC, 2),
    created_at,
    updated_at
FROM orders
JOIN LATERAL generate_series(1, (FLOOR(RANDOM() * 3 + 1))::INT) AS i(n) ON TRUE
WHERE DATE(created_at) = DATE(NOW());

-- Insert Payments (one per order)
INSERT INTO payments (order_id, amount, payment_method, status, created_at, updated_at)
SELECT
    order_id,
    total_amount,
    CASE WHEN RANDOM() < 0.33 THEN 'credit_card'
         WHEN RANDOM() < 0.66 THEN 'paypal'
         ELSE 'bank_transfer'
    END,
    CASE WHEN order_status='completed'
        THEN 'completed'
    ELSE 'failed' END,
    created_at,
    updated_at
FROM orders
WHERE order_status IN ('completed', 'cancelled')
    AND DATE(created_at) = DATE(NOW());

-- Insert Shipping (one per order)
INSERT INTO shipping (order_id, address, city, postal_code, country, tracking_number, created_at, updated_at)
SELECT
    order_id,
    CONCAT('Address ', order_id),
    'CityName',
    LPAD(FLOOR(RANDOM()*100000)::TEXT, 5, '0'),
    'CountryName',
    CONCAT('TRK', order_id),
    created_at,
    updated_at
FROM orders
WHERE DATE(created_at) = DATE(NOW());

-- Insert Product Reviews (optional per order, only if they have at least one item)
INSERT INTO product_reviews (product_id, user_id, rating, comment, created_at, updated_at)
SELECT
    oi.product_id,
    o.user_id,
    FLOOR(RANDOM() * 5 + 1),
    CONCAT('Review for product ', oi.product_id),
    o.created_at,
    o.created_at
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'completed'
  AND DATE(o.created_at) = DATE(NOW())
  AND RANDOM() < 0.8;  -- 80% chance of review per item
