SELECT
    p.pet_id,
    p.name as pet_name,
    p.breed,
    p.dob as date_of_birth,
    DATE_PART('year', AGE(DATE('2025-08-01'), p.dob::date)) AS age_years,
    {{ age_bucket('p.dob') }} AS age_bucket,
    p.owner_id,
    o.name as owner_name,
    o.email AS owner_email,
    o.country AS owner_country,
    o.signup_date AS owner_signup_date
FROM {{ ref('stg_pets') }} p
LEFT JOIN {{ ref('stg_owners') }} o ON p.owner_id = o.owner_id

