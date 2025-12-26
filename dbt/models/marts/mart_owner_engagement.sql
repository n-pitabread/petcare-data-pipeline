{{ config(materialized='table') }}

WITH owner_pets AS (
    SELECT
        owner_id,
        COUNT(DISTINCT pet_id) AS num_pets
    FROM {{ ref('stg_pets') }}
    GROUP BY owner_id
),

owner_consultations AS (
    SELECT
        p.owner_id,
        COUNT(c.consultation_id) AS total_consultations,
        AVG(c.duration_minutes) AS avg_consultation_duration,
        SUM(c.duration_minutes) AS total_consultation_minutes,
        MIN(c.created_at) AS first_consultation_date,
        MAX(c.created_at) AS last_consultation_date,
        -- TIMESTAMP('2025-08-01') should be replace with CURRENT_TIMESTAMP when use in production
        DATE_PART('day', DATE('2025-08-01')::timestamp - MAX(c.created_at)) AS days_since_last_consultation
    FROM {{ ref('stg_pets') }} p
    LEFT JOIN {{ ref('stg_consultations') }} c ON CAST(p.pet_id AS INTEGER) = CAST(c.pet_id AS INTEGER)
    GROUP BY p.owner_id
)

SELECT
    o.owner_id,
    o.name as owner_name,
    o.email,
    o.country,
    o.signup_date,
    -- TIMESTAMP('2025-08-01') should be replace with CURRENT_TIMESTAMP when use in production
    DATE_PART('day', DATE('2025-08-01')::timestamp - o.signup_date) AS days_since_signup,
    COALESCE(op.num_pets, 0) AS num_pets,
    COALESCE(oc.total_consultations, 0) AS total_consultations,
    COALESCE(oc.avg_consultation_duration, 0) AS avg_consultation_duration,
    COALESCE(oc.total_consultation_minutes, 0) AS total_consultation_minutes,
    oc.first_consultation_date,
    oc.last_consultation_date,
    -- DATE('2025-08-01') should be replace with CURRENT_DATE when use in production
    COALESCE(oc.days_since_last_consultation, DATE_PART('day', DATE('2025-08-01')::timestamp - o.signup_date)) AS days_since_last_consultation,
    {{ churn_flag('COALESCE(oc.days_since_last_consultation, DATE_PART(\'day\', DATE(\'2025-08-01\')::timestamp - o.signup_date))', 'COALESCE(oc.total_consultations, 0)') }} AS churn_flag
    -- DATE('2025-08-01') should be replace with CURRENT_DATE when use in production
FROM {{ ref('stg_owners') }} o
LEFT JOIN owner_pets op ON o.owner_id = op.owner_id
LEFT JOIN owner_consultations oc ON o.owner_id = oc.owner_id

