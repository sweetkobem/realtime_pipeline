INSERT INTO dim_user
SELECT *
FROM (
    -- New/changed rows (new version, is_current = 1)
    SELECT
        toString(xxHash64(user_id, toDate('{execution_date}'))) AS user_key,
        user_id,
        name,
        email,
        phone,
        created_time AS created_time,
        toDate('{execution_date}') AS start_date,
        NULL AS end_date,
        1 AS is_current
    FROM silver_ecommerce_user
    WHERE toDate(updated_time) = toDate('{execution_date}')

    UNION ALL
    -- Expire old versions (set end_date, is_current = 0)
    SELECT
        o.user_key,
        o.user_id,
        o.name,
        o.email,
        o.phone,
        created_time AS created_time,
        o.start_date,
        toDate('{execution_date}') AS end_date,
        0 AS is_current
    FROM dim_user o
    RIGHT JOIN silver_ecommerce_user n
        ON n.user_id = o.user_id
    WHERE o.is_current = 1
        AND toDate(n.updated_time) = toDate('{execution_date}')
) AS merged_changes;