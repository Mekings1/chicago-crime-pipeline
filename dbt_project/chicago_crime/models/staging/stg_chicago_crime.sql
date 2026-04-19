WITH source AS (
    SELECT * FROM raw_chicago_crime
),

cleaned AS (
    SELECT
        id                                                  AS crime_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id)     AS crime_id_surrogate,
        case_number,
        incident_datetime,
        incident_date,
        year,
        month,

        TRIM(UPPER(primary_type))                           AS crime_type,
        TRIM(lower(description))                            AS crime_description,
        TRIM(lower(location_description))                   AS location_type,

        CAST(arrest   AS BOOLEAN)                           AS was_arrested,
        CAST(domestic AS BOOLEAN)                           AS is_domestic,

        COALESCE(CAST(district AS VARCHAR), 'Unknown')      AS district,
        COALESCE(CAST(ward AS VARCHAR),     'Unknown')      AS ward,

        CAST(latitude  AS DOUBLE)                           AS latitude,
        CAST(longitude AS DOUBLE)                           AS longitude
    FROM source
    WHERE crime_id IS NOT NULL
      AND incident_date IS NOT NULL
      AND crime_type    IS NOT NULL
)

SELECT * FROM cleaned WHERE crime_id_surrogate = 1