-- ToDo: This table can also be partitioned by event_date.
-- As this base table can then be used for weekly and
-- monthly growth accounting.



WITH grouped_data AS (
  SELECT CAST('2022-01-01' AS DATE) as event_date,
         user_type,
         product_type,
         platform,
         count(user_id) as metric
FROM `sub_growth_daily_granular` 
WHERE accounting_event_date = '2022-01-01'
GROUP BY user_type, product_type, platform)

SELECT event_date,
       product_type,
       platform,
       new_user,retained,churned,resurrected
FROM 
(
  SELECT event_date, product_type,
         platform, user_type, metric 
  FROM grouped_data)
PIVOT
(
  SUM(metric) FOR 
  user_type IN ("new_user", 
                "retained", 
                "churned", 
                "resurrected"))