-- TODO: Need to optimise this query to reduce data processed
WITH yesterdays_user_state AS (
    SELECT
        user_id,
        first_active_date,
        last_active_date,
        CASE
            WHEN last_active_date >= date_add('2022-01-01', INTERVAL -1 day) THEN TRUE
            ELSE FALSE
        END AS is_active,
        plan_type,
        plan_duration
    FROM
        sub_dim_user_daily_snapshot
    WHERE
        event_date = date_add('2022-01-01', INTERVAL -1 day)
),
todays_user_state AS (
    SELECT
        user_id,
        product_type,
        platform,
        first_active_date,
        last_active_date,
        CASE
            WHEN last_active_date >= '2022-01-01' THEN TRUE
            ELSE FALSE
        END AS is_active
    FROM
        `sub_dim_user_daily_snapshot`
    WHERE
        event_date = '2022-01-01'
)
SELECT
    CAST ('2022-01-01' AS DATE) AS accounting_event_date,
    COALESCE(
        yesterdays_user_state.user_id,
        todays_user_state.user_id
    ) AS user_id,
    COALESCE(
        todays_user_state.product_type,
        yesterdays_user_state.product_type
    ) AS product_type,
    COALESCE(
        todays_user_state.platform,
        yesterdays_user_state.platform
    ) AS platform,
    COALESCE(
        yesterdays_user_state.first_active_date,
        todays_user_state.first_active_date
    ) AS first_active_date,
    COALESCE(
        todays_user_state.last_active_date,
        yesterdays_user_state.last_active_date
    ) AS last_active_date,
    COALESCE(
        todays_user_state.is_active,
        yesterdays_user_state.is_active
    ) AS is_active,
    CASE
        WHEN (
            todays_user_state.is_active IS TRUE
            AND yesterdays_user_state.is_active IS TRUE
        ) THEN "retained"
        WHEN (
            (
                todays_user_state.is_active IS NULL
                OR todays_user_state.is_active IS FALSE
            )
            AND yesterdays_user_state.is_active IS TRUE
        ) THEN "churned"
        WHEN (
            todays_user_state.is_active IS TRUE
            AND yesterdays_user_state.is_active IS NULL
        ) THEN "new_user"
        WHEN todays_user_state.is_active IS TRUE
        AND yesterdays_user_state.last_active_date IS NOT NULL
        AND yesterdays_user_state.is_active IS FALSE THEN "resurrected"
        WHEN (
            yesterdays_user_state.is_active IS FALSE
            AND todays_user_state.is_active IS FALSE
        ) THEN "inactive"
        ELSE "unknown"
    END AS user_type
FROM
    yesterdays_user_state FULL
    OUTER JOIN todays_user_state ON -- We join on all segments so that we can consider users 
    -- changing a segment.
    -- We will have to treat slow changing and fast changing
    -- dimensions separately to avoid lots of noise.
    (
        yesterdays_user_state.user_id = todays_user_state.user_id
        AND yesterdays_user_state.product_type = todays_user_state.product_type
        AND yesterdays_user_state.platform = todays_user_state.platform
    )