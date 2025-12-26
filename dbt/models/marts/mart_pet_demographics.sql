{{ config(materialized='table') }}

WITH pet_profiles AS (
    SELECT * FROM {{ ref('int_pet_profiles') }}
),

pet_demographics AS (
    SELECT
        breed,
        age_bucket,
        COUNT(*) AS pet_count,
        AVG(age_years) AS avg_age_years,
        COUNT(DISTINCT owner_id) AS unique_owners
    FROM pet_profiles
    GROUP BY breed, age_bucket
)

SELECT * FROM pet_demographics
ORDER BY pet_count DESC

