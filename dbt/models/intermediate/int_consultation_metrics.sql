WITH consultation_metrics AS (
    SELECT
        pet_id,
        expert_id,
        COUNT(*) AS total_consultations,
        AVG(duration_minutes) AS avg_duration_minutes,
        SUM(duration_minutes) AS total_duration_minutes,
        MIN(created_at) AS first_consultation_date,
        MAX(created_at) AS last_consultation_date,
        -- DATE('2025-08-01') should be replace with CURRENT_DATE when use in production
        DATE_PART('day', DATE('2025-08-01') - MAX(created_at)) AS days_since_last_consultation
    FROM {{ ref('stg_consultations') }}
    GROUP BY pet_id, expert_id
)

SELECT * FROM consultation_metrics

