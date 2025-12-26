{% macro age_bucket(date_of_birth) %}
    CASE
        -- DATE('2025-08-01') should be replace with CURRENT_DATE when use in production
        WHEN DATE_PART('year', AGE(DATE('2025-08-01'), {{ date_of_birth }})) < 3 THEN 'Young (1-3 years)'
        WHEN DATE_PART('year', AGE(DATE('2025-08-01'), {{ date_of_birth }})) < 7 THEN 'Adult (3-7 years)'
        WHEN DATE_PART('year', AGE(DATE('2025-08-01'), {{ date_of_birth }})) < 10 THEN 'Senior (7-10 years)'
        ELSE 'Elderly (10+ years)'
    END
{% endmacro %}
