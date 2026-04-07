{{
    config(
        materialized='incremental',
        unique_key='standings_id',
        schema='BRONZE'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'STANDINGS') }}
    {% if is_incremental() %}
    -- Only process new files loaded since the last run
    WHERE inserted_at > (SELECT COALESCE(MAX(loaded_at_utc), '1900-01-01') FROM {{ this }})
    {% endif %}
),

flattened AS (
    SELECT
        file_name,
        inserted_at,
        f.value AS standings_data
    FROM source_data,
    LATERAL FLATTEN(input => RAW_JSON) f
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['standings_data:team_id', 
    'standings_data:wins', 
    'standings_data:losses',
    'CAST(standings_data:fetched_at AS DATE)']) }} AS standings_id,

    standings_data:conference::string           AS conference,
    standings_data:conference_rank::string            AS conference_rank,
    standings_data:division::string            AS division,
    standings_data:home_record::string            AS home_record,
    standings_data:last_10::string            AS last_10,
    standings_data:losses::int            AS losses,
    standings_data:road_record::string            AS road_record,
    standings_data:streak::int            AS streak,
    standings_data:team_id::int            AS team_id,
    standings_data:team_name::string            AS team_name,
    standings_data:win_pct::float            AS win_pct,
    standings_data:wins::int            AS wins,
    file_name                             AS source_file,
    inserted_at                           AS loaded_at_utc,
    standings_data:fetched_at::timestamp_ntz AS api_fetched_at

FROM flattened