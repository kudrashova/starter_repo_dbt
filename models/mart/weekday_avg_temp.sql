WITH total_avg AS (
    SELECT
        city, country, date, weekday,
        AVG(avgtemp_c) AS avg_temp_weekday,
        MAX(maxtemp_c) AS max_temp_weekday,
        MIN(mintemp_c) AS min_temp_weekday
    FROM {{ref('prep_temp')}}
    GROUP BY city, country, date, weekday
)
SELECT *
FROM total_avg