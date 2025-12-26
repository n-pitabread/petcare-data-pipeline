{{ config(materialized='table', schema='features') }}

SELECT
    owner_id,
    total_consultations,
    days_since_last_consultation,
    avg_consultation_duration,
    total_consultation_minutes,
    num_pets,
    days_since_signup,
    CASE
        WHEN days_since_signup > 0 THEN total_consultations::FLOAT / days_since_signup * 30
        ELSE 0
    END AS consultations_per_month,
    CASE
        WHEN total_consultations > 0 THEN total_consultation_minutes::FLOAT / total_consultations
        ELSE 0
    END AS avg_minutes_per_consultation
FROM {{ ref('mart_owner_engagement') }}

