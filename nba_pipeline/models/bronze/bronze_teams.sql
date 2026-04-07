{{
    config(
        materialized='incremental',
        unique_key='team_version_id',
        schema='BRONZE'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'TEAMS') }}
    {% if is_incremental() %}
    -- Only process new files loaded since the last run
    WHERE inserted_at > (SELECT COALESCE(MAX(loaded_at_utc), '1900-01-01') FROM {{ this }})
    {% endif %}
),

flattened AS (
    SELECT
        file_name,
        inserted_at,
        f.value AS teams_data
    FROM source_data,
    LATERAL FLATTEN(input => RAW_JSON) f
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'teams_data:team_id', 
        'CAST(teams_data:fetched_at AS DATE)'
    ]) }} as team_version_id,
    teams_data:abbreviation::string           as abbreviation,
    teams_data:city::string            as city,
    teams_data:nickname::string              as nickname,
    teams_data:state::string              as state,
    teams_data:team_id::int            as team_id,
    teams_data:team_name::string              as team_name,
    teams_data:year_founded::int              as year_founded,
    file_name                       as source_file,
    inserted_at                     as loaded_at_utc,
    teams_data:fetched_at::timestamp_ntz as api_fetched_at

FROM flattened