{{
    config(
        materialized='incremental',
        unique_key='team_version_id',
        schema='BRONZE'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'TEAM_DETAILS') }}
    {% if is_incremental() %}
    -- Only process new files loaded since the last run
    WHERE inserted_at > (SELECT COALESCE(MAX(loaded_at_utc), '1900-01-01') FROM {{ this }})
    {% endif %}
),

flattened AS (
    SELECT
        file_name,
        inserted_at,
        f.value AS team_details_data
    FROM source_data,
    LATERAL FLATTEN(input => RAW_JSON) f
), transformed AS(
SELECT
    {{ dbt_utils.generate_surrogate_key([
        'team_details_data:team_id', 
        'CAST(team_details_data:fetched_at AS DATE)'
    ]) }} as team_version_id,
    team_details_data:team_id::int              as team_id,
    team_details_data:abbreviation::string      as abbreviation,
    team_details_data:nickname::string          as nickname,
    team_details_data:city::string              as city,
    team_details_data:state::string             as state,

    -- 2. Arena Information
    team_details_data:arena::string             as arena_name,
    team_details_data:arena_capacity::int       as arena_capacity,

    -- 3. Personnel & Ownership
    team_details_data:head_coach::string        as head_coach,
    team_details_data:general_manager::string   as general_manager,
    team_details_data:owner::string             as owner,
    team_details_data:dleague_affiliation::string as dleague_affiliation,

    -- 4. History
    team_details_data:year_founded::int         as year_founded,
    file_name                       as source_file,
    inserted_at                     as loaded_at_utc,
    team_details_data:fetched_at::timestamp_ntz as api_fetched_at

FROM flattened)
SELECT *
FROM transformed
QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY loaded_at_utc DESC) =1