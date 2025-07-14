WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
        , DATE_PART('day', date)::INT AS date_day        -- number of the day of month
        , DATE_PART('month', date)::INT AS date_month    -- number of the month of year
        , DATE_PART('year', date)::INT AS date_year      -- number of year
        , DATE_PART('week', date)::INT AS cw             -- number of the week of year
        , TO_CHAR(date, 'Month') AS month_name           -- name of the month
        , TO_CHAR(date, 'Day') AS weekday                -- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
        , (CASE 
            WHEN DATE_PART('month', date) IN (12, 1, 2) THEN 'winter'
            WHEN DATE_PART('month', date) IN (3, 4, 5) THEN 'spring'
            WHEN DATE_PART('month', date) IN (6, 7, 8) THEN 'summer'
            WHEN DATE_PART('month', date) IN (9, 10, 11) THEN 'autumn'
        END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date;