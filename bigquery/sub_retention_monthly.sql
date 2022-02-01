-- This query is run at the end of every month to generate the new 
-- entries for monthly subscriber retention. So this query should be run
-- only on the first of every month.
SELECT
    COUNT(user_id) AS active_users,
    platform,
    product_type,
    date_trunc(first_active_date, month) AS cohort_month,
    date_diff(
        date_trunc('2022-01-01', month),
        date_trunc(first_active_date, month),
        month
    ) AS _interval
FROM
    sub_growth_daily_granular
WHERE
    accounting_event_date < date_trunc('2022-01-01', month)
    AND accounting_event_date >= date_add(
        date_trunc('2022-01-01', month),
        INTERVAL -1 month
    ) -- We select only the active users during the last month period.
    AND user_type IN ("new_user", "resurrected", "churned", "retained")
GROUP BY
    platform,
    product_type,
    _interval,
    cohort_month