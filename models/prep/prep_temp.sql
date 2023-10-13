WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_temp')}}
),
add_weekday AS (
    SELECT *,
        ... AS weekday,
        ... AS day_num
    FROM temp_daily
)
SELECT *
FROM add_weekday