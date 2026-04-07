-- Distribution of crime types (feeds categorical bar chart tile)
SELECT
    crime_type,
    COUNT(*)                                            AS total_crimes,
    SUM(CASE WHEN was_arrested THEN 1 ELSE 0 END)       AS total_arrests,
    ROUND(
        100.0 * SUM(CASE WHEN was_arrested THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                                   AS arrest_rate_pct
FROM {{ ref('stg_chicago_crime') }}
GROUP BY crime_type
ORDER BY total_crimes DESC