WITH airports_reorder AS (
    SELECT 
        country,
        region,     -- moved up to come right after country
        faa,
        name,
        lat,
        lon,
        alt,
        tz,
        dst,
        city
    FROM {{ref('staging_airports')}}
)
SELECT * 
FROM airports_reorder