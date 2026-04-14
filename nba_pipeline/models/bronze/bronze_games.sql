{{
    config(
        materialized='incremental',
        unique_key='game_team_id',
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
    {{ dbt_utils.generate_surrogate_key([
        'games_data:game_id', 
        'games_data:team_id'
    ]) }} as game_team_id,
    games_data:game_date::timestamp_ntz           as game_date,
    games_data:game_id::string            as game_id,
    games_data:matchup::string              as matchup,
    games_data:season::string              as season,
    games_data:team_id::int            as team_id,
    games_data:plus_minus::int            as plus_minus,
    games_data:pts::int            as pts,
    games_data:wl::string              as wl,
    file_name                       as source_file,
    inserted_at                     as loaded_at_utc,
    games_data:fetched_at::timestamp_ntz as api_fetched_at

FROM flattened