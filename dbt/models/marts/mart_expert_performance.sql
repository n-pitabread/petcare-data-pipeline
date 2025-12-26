{{ config(materialized='table') }}

WITH expert_metrics AS (
    SELECT
        e.expert_id,
        e.name as expert_name,
        e.specialty,
        e.years_experience,
        COUNT(c.consultation_id) AS total_consultations,
        COUNT(DISTINCT c.pet_id) AS unique_pets_served,
        AVG(c.duration_minutes) AS avg_consultation_duration,
        SUM(c.duration_minutes) AS total_consultation_minutes,
        MIN(c.created_at) AS first_consultation_date,
        MAX(c.created_at) AS last_consultation_date
    FROM {{ ref('stg_experts') }} e
    LEFT JOIN {{ ref('stg_consultations') }} c ON CAST(e.expert_id AS INTEGER) = CAST(c.expert_id AS INTEGER)
    GROUP BY e.expert_id, e.name, e.specialty, e.years_experience
)

SELECT * FROM expert_metrics
ORDER BY total_consultations DESC

