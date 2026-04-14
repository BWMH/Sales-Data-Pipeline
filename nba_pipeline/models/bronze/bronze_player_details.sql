{{
    config(
        materialized='incremental',
        unique_key='player_record_id',
        schema='BRONZE'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'PLAYER_DETAILS') }}
    {% if is_incremental() %}
    -- Only process new files loaded since the last run
    WHERE inserted_at > (SELECT COALESCE(MAX(loaded_at_utc), '1900-01-01') FROM {{ this }})
    {% endif %}
),

flattened AS (
    SELECT
        file_name,
        inserted_at,
        f.value AS player_data
    FROM source_data,
    LATERAL FLATTEN(input => RAW_JSON) f
), transformed AS(

SELECT
    -- 1. Unique Record Identifier (Player + Version)
    {{ dbt_utils.generate_surrogate_key([
        'player_data:player_id', 
        'player_data:updated_at'
    ]) }} AS player_record_id,

    -- 2. Player Identity & Physicals
    player_data:player_id::int              AS player_id,
    player_data:full_name::string           AS full_name,
    player_data:birthdate::timestamp_ntz    AS birthdate,
    player_data:country::string             AS country,
    player_data:height::string              AS height,
    player_data:weight::string      AS weight_lbs,
    player_data:jersey_number::string       AS jersey_number,
    player_data:position::string            AS position,

    -- 3. Team Context
    player_data:team_id::int                AS team_id,
    player_data:team_name::string           AS team_name,
    player_data:team_abbreviation::string   AS team_abbreviation,

    -- 4. Draft & Career Info
    player_data:draft_year::string   AS draft_year,
    player_data:draft_round::string  AS draft_round,
    player_data:draft_number::string AS draft_number,
    player_data:from_year::string    AS career_from_year,
    player_data:to_year::string      AS career_to_year,

    -- 5. Metadata
    file_name                               AS source_file,
    inserted_at                             AS loaded_at_utc,
    player_data:fetched_at::timestamp_ntz   AS api_fetched_at,
    player_data:updated_at::timestamp_ntz   AS api_updated_at
FROM flattened)
SELECT *
FROM transformed
QUALIFY ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY loaded_at_utc DESC) =1