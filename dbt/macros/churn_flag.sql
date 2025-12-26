{% macro churn_flag(days_since_last_consultation, total_consultations) %}
    CASE
        WHEN {{ days_since_last_consultation }} > 90 OR {{ total_consultations }} = 0 THEN 1
        WHEN {{ days_since_last_consultation }} > 60 AND {{ total_consultations }} < 3 THEN 1
        ELSE 0
    END
{% endmacro %}

