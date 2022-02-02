-- Update all segment changes in the user-dim so that we
-- can use them for segment changes by existing users.
-- 1. We use 'DISTINCT' to insert just 1 unique row when there are 
-- duplicates. Quick verification of duplicates showed that
-- all fields are same except transaction id , which is not 
-- relevant for us.
-- 2. We use MAX(expires_date) and groupy to remove duplicates. 
-- when we see multiple entries with different expires_date. 
-- This is mostly some internal employee user who is changing free plans for some testing. 
-- (Based on handful of manual verifications)
WITH temp_data AS (
    SELECT
        DISTINCT subscriber_id AS user_id,
        expires_date AS last_active_date,
        event_date AS first_active_date,
        product_id AS plan_type,
        plan AS plan_duration -- Collect segments here. Segments or properties can change
        -- over time like their plans etc.
    FROM
        `your-payment-events-table` -- Merge the previous days data into dim_user.
        -- We do this the next day to avoid cyclic dependency on dim_user.
        -- We want to query the dim_user based on its previous day's state
        -- to check for resurrected users.
    WHERE
        event_date = '2022-01-01'
        AND -- All free tier plans have a 'free' keyword in it.
        -- This is our assumption.
        product_id NOT LIKE '%free%'
        AND 
        -- We saw cases where a subscriber event was created with a 
        -- expiry date before the event date. 
        -- We will exclude these users.
        expires_date >= '2022-01-01'
),
-- Bigquery does not support max_by function. Sometimes on
-- the same day a user changes from one plan to another or
-- one platform to another.
-- We are selecting the row which has the latest timestamp
-- out of the rows and considering that as his final plan.
ordered_by_datetime AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY user_id
            ORDER BY
                event_datetime DESC,
                purchase_datetime DESC
        ) time_rank
    FROM
        temp_data
)
SELECT
    user_id,
    MAX(last_active_date) as last_active_date,
    first_active_date,
    product_type,
    platform,
FROM
    ordered_by_datetime
WHERE
    time_rank = 1
GROUP BY
    user_id,
    first_active_date,
    plan_type,
    plan_duration