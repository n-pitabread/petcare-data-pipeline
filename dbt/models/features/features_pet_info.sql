{{ config(materialized='table', schema='features') }}

WITH pets_by_owner AS (
    SELECT
        owner_id,
        COUNT(*) AS num_pets,
        AVG(age_years) AS avg_pet_age,
        MIN(age_years) AS min_pet_age,
        MAX(age_years) AS max_pet_age,
        COUNT(DISTINCT breed) AS num_unique_breeds
    FROM {{ ref('int_pet_profiles') }}
    GROUP BY owner_id
)

SELECT * FROM pets_by_owner

