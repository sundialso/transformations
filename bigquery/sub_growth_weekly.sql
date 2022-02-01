WITH current_week_active_data AS (
    SELECT
        user_id,
        platform,
        product_type,
        logical_or(is_active) AS is_active,
        MAX (last_active_date) AS last_active_date,
    FROM
        `sub_growth_daily_granular`
    WHERE
        accounting_event_date <= '2022-01-01'
        AND accounting_event_date >= date_add('2022-01-01', INTERVAL -6 day)
        AND -- We fetch only the active users for current week. 
        user_type IN ("new_user", "churned", "retained", "resurrected")
    GROUP BY
        user_id,
        platform,
        product_type
),
last_week_data AS (
    SELECT
        user_id,
        platform,
        product_type,
        logical_or(is_active) AS is_active,
        MAX(last_active_date) AS last_active_date,
    FROM
        `sub_growth_daily_granular` -- We will fetch both active and inactive users for last week
    WHERE
        accounting_event_date <= CAST(
            date_add('2022-01-01', INTERVAL -7 day) AS DATE
        )
        AND accounting_event_date >= CAST(
            date_add('2022-01-01', INTERVAL -13 day) AS DATE
        )
    GROUP BY
        user_id,
        platform,
        product_type
),
weekly_growth_accounting AS (
    SELECT
        CAST('2022-01-01' AS DATE) AS event_date,
        COALESCE(
            current_week_active_data.user_id,
            last_week_data.user_id
        ) as user_id,
        COALESCE(
            current_week_active_data.platform,
            last_week_data.platform
        ) as platform,
        COALESCE(
            current_week_active_data.product_type,
            last_week_data.product_type
        ) as product_type,
        #   consider the case for segment transitions in between . That needs to be accounted differently
        CASE
            WHEN current_week_active_data.is_active IS TRUE
            AND last_week_data.is_active IS TRUE THEN "retained"
            WHEN (
                current_week_active_data.is_active IS FALSE
                OR current_week_active_data.is_active IS NULL
            )
            AND last_week_data.is_active IS TRUE THEN "churned"
            WHEN current_week_active_data.is_active IS TRUE
            AND last_week_data.is_active IS NULL THEN "new_user"
            WHEN current_week_active_data.is_active IS TRUE
            AND last_week_data.is_active IS NOT NULL
            AND last_week_data.is_active IS FALSE THEN "resurrected"
            WHEN (
                current_week_active_data.is_active IS FALSE
                OR current_week_active_data.is_active IS NULL
            )
            AND (
                last_week_data.is_active IS FALSE
                OR last_week_data.is_active IS NULL
            ) THEN 'inactive'
            ELSE "Unknown"
        END as user_type
    FROM
        current_week_active_data FULL
        OUTER JOIN last_week_data -- We join on user_id and all segments to consider users 
        -- changing their segments . Any change in segments would 
        -- be considered as the user churning in the old segment and
        -- added as a new_user user in the new_user segment.
        ON (
            current_week_active_data.user_id = last_week_data.user_id
            AND current_week_active_data.platform = last_week_data.platform
            AND current_week_active_data.product_type = last_week_data.product_type
        )
),
grouped_data AS (
    SELECT
        CAST('2022-01-01' AS DATE) as event_date,
        user_type,
        product_type,
        platform,
        count(user_id) as metric
    FROM
        weekly_growth_accounting
    WHERE
        event_date = '2022-01-01'
    GROUP BY
        user_type,
        product_type,
        platform
)
SELECT
    event_date,
    product_type,
    platform,
    new_user,
    retained,
    churned,
    resurrected
FROM
    (
        SELECT
            event_date,
            product_type,
            platform,
            user_type,
            metric
        FROM
            grouped_data
    ) as tab1 PIVOT (
        SUM(metric) FOR user_type IN (
            "new_user",
            "retained",
            "churned",
            "resurrected"
        )
    ) AS tab2