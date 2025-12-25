WITH raw_consultations AS (
    SELECT 
        timestamp as raw_timestamp,
        message::jsonb AS message_json
    FROM {{ source("kafka_raw", "raw_stream") }}
    WHERE topic = 'consultations'
    {% if is_incremental() %}
        AND timestamp > (SELECT COALESCE(MAX(ingested_at), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
),

cleaned_consultations AS (
    SELECT
        message_json->>'consultation_id' AS consultation_id,
        message_json->>'topic' AS consultation_topic,
        message_json->>'pet_id' AS pet_id,
        message_json->>'expert_id' AS expert_id,
        (message_json->>'created_at')::timestamp AS created_at,
        CASE 
            WHEN (message_json->>'duration_minutes')::int > 0 
            THEN (message_json->>'duration_minutes')::int 
            ELSE 0 
        END AS duration_minutes,
        raw_timestamp AS ingested_at
    FROM raw_consultations
    WHERE
        message_json->>'consultation_id' IS NOT NULL
        AND message_json->>'pet_id' IS NOT NULL
        AND message_json->>'expert_id' IS NOT NULL
        AND message_json->>'topic' IS NOT NULL
        AND message_json->>'created_at' IS NOT NULL
        AND message_json->>'duration_minutes' IS NOT NULL
)
SELECT * FROM cleaned_consultations

