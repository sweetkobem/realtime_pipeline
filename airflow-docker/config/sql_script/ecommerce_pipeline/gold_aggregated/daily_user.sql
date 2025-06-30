INSERT INTO daily_user
SELECT
  fo.date_key,
  COUNT(DISTINCT du.user_id) AS active_user,
  countDistinctIf(du.user_id, toDate(du.created_time) = fo.date_key) AS new_user
FROM fact_order fo
JOIN dim_user du
	ON fo.user_key = du.user_key

WHERE fo.date_key = '{execution_date}'
GROUP BY 1