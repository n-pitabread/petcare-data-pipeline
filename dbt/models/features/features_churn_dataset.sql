{{ config(materialized='table', schema='features') }}

WITH owner_engagement AS (
    SELECT 
        owner_id,
        churn_flag,
        total_consultations,
        days_since_last_consultation
    FROM {{ ref('mart_owner_engagement') }}
)

SELECT
    oa.owner_id,
    oa.total_consultations,
    oa.days_since_last_consultation,
    oa.avg_consultation_duration,
    oa.total_consultation_minutes,
    oa.consultations_per_month,
    oa.avg_minutes_per_consultation,
    COALESCE(pi.num_pets, 0) AS num_pets,
    COALESCE(pi.avg_pet_age, 0) AS avg_pet_age,
    COALESCE(pi.num_unique_breeds, 0) AS num_unique_breeds,
    oe.churn_flag AS churn_flag
FROM {{ ref('features_owner_activity') }} oa
LEFT JOIN {{ ref('features_pet_info') }} pi ON oa.owner_id = pi.owner_id
LEFT JOIN owner_engagement oe ON oa.owner_id = oe.owner_id
WHERE oa.total_consultations IS NOT NULL

