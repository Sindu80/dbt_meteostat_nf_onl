dt 
sql
Copy
Edit
WITH all_flights AS (
    SELECT
        origin AS airport_code,
        tail_number,
        airline,
        cancelled,
        diverted
    FROM {{ ref('prep_flights') }}
    
    UNION ALL
    
    SELECT
        dest AS airport_code,
        tail_number,
        airline,
        cancelled,
        diverted
    FROM {{ ref('prep_flights') }}
),

stats_per_airport AS (
    SELECT
        airport_code,
        COUNT(*) AS total_flights,
        COUNT(*) FILTER (WHERE cancelled = 1) AS total_cancelled,
        COUNT(*) FILTER (WHERE diverted = 1) AS total_diverted,
        COUNT(*) FILTER (WHERE cancelled = 0 AND diverted = 0) AS total_completed,
        COUNT(DISTINCT CASE WHEN airport_code = origin THEN dest END) AS unique_departure_connections,
        COUNT(DISTINCT CASE WHEN airport_code = dest THEN origin END) AS unique_arrival_connections,
        COUNT(DISTINCT CASE WHEN tail_number IS NOT NULL THEN tail_number END) 
            / NULLIF(COUNT(DISTINCT flight_date), 0) AS avg_unique_airplanes, -- optional
        COUNT(DISTINCT CASE WHEN airline IS NOT NULL THEN airline END) 
            / NULLIF(COUNT(DISTINCT flight_date), 0) AS avg_unique_airlines -- optional
    FROM {{ ref('prep_flights') }}
    GROUP BY airport_code
),

airport_info AS (
    SELECT
        faa AS airport_code,
        city,
        country,
        name
    FROM {{ ref('prep_airports') }}
)

SELECT
    s.airport_code,
    a.name,
    a.city,
    a.country,
    s.unique_departure_connections,
    s.unique_arrival_connections,
    s.total_flights,
    s.total_cancelled,
    s.total_diverted,
    s.total_completed,
    s.avg_unique_airplanes,
    s.avg_unique_airlines
FROM stats_per_airport s
LEFT JOIN airport_info a ON s.airport_code = a.airport_code