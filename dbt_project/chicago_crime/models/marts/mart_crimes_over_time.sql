-- Monthly crime counts (feeds time-series line chart tile)
SELECT
    year,
    month,
    DATE_TRUNC('month', incident_date)      AS month_start,
    crime_type,
    COUNT(*)                                AS total_crimes,
    SUM(CASE WHEN was_arrested THEN 1 ELSE 0 END) AS total_arrests
FROM {{ ref('stg_chicago_crime') }}
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2