INSERT INTO dim_category
SELECT *
FROM (
    -- New/changed rows (new version, is_current = 1)
    SELECT
        toString(xxHash64(category_id, toDate('{execution_date}'))) AS category_key,
        category_id,
        name,
        parent_id,
        toDate('{execution_date}') AS start_date,
        NULL AS end_date,
        1 AS is_current
    FROM silver_ecommerce_category
    WHERE toDate(updated_time) = toDate('{execution_date}')

    UNION ALL
    -- Expire old versions (set end_date, is_current = 0)
    SELECT
        o.category_key,
        o.category_id,
        o.name,
        o.parent_id,
        o.start_date,
        toDate('{execution_date}') AS end_date,
        0 AS is_current
    FROM dim_category o
    RIGHT JOIN silver_ecommerce_category n
        ON n.category_id = o.category_id
    WHERE o.is_current = 1
        AND toDate(n.updated_time) = toDate('{execution_date}')
) AS merged_changes;