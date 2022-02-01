SELECT CAST('2022-01-01'  AS DATE) AS
       event_date,
       user_id, last_active_date,
       first_active_date, 
       product_type, platform,
       is_trial, trial_start_date
FROM `sub_dim_user`