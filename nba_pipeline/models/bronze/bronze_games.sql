{{
    config(
        materialized='incremental',
        unique_key='game_id',
        schema='BRONZE'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'GAMES') }}
    {% if is_incremental() %}
    -- Only process new files loaded since the last run
    WHERE inserted_at > (SELECT COALESCE(MAX(loaded_at_utc), '1900-01-01') FROM {{ this }})
    {% endif %}
),

flattened AS (
    SELECT
        file_name,
        inserted_at,
        f.value AS games_data
    FROM source_data,
    LATERAL FLATTEN(input => RAW_JSON) f
)

SELECT
    games_data:game_date::timestamp_ntz           as game_date,
    games_data:game_id::string            as game_id,
    games_data:matchup::string              as matchup,
    games_data:season::string              as season,
    games_data:team_id::int            as team_id,
    file_name                       as source_file,
    inserted_at                     as loaded_at_utc,
    games_data:fetched_at::timestamp_ntz as api_fetched_at

FROM flattened