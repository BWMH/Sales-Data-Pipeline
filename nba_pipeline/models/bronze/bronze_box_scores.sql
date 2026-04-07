{{
    config(
        materialized='incremental',
        unique_key='box_score_id',
        schema='BRONZE'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'BOX_SCORES') }}
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
)

SELECT
    -- 0. Unique Identifier
    {{ dbt_utils.generate_surrogate_key(['player_data:game_id', 'player_data:player_id']) }} AS box_score_id,

    -- 1. Metadata & Keys
    player_data:game_id::string           AS game_id,
    player_data:player_id::int            AS player_id,
    player_data:team_id::int              AS team_id,
    file_name                             AS source_file,
    inserted_at                           AS loaded_at_utc,

    -- 2. Player Info
    player_data:first_name::string        AS first_name,
    player_data:family_name::string       AS last_name,
    player_data:jersey_number::string     AS jersey_number, 
    player_data:position::string          AS position,
    player_data:status::string            AS status,

    -- 3. Team Info
    player_data:team_city::string         AS team_city,
    player_data:team_name::string         AS team_name,
    player_data:team_abbreviation::string AS team_abbreviation,

    -- 4. Game Stats
    player_data:minutes::string           AS minutes_played,
    player_data:points::int               AS points,
    player_data:assists::int              AS assists,
    player_data:steals::int               AS steals,
    player_data:blocks::int               AS blocks,
    player_data:turnovers::int            AS turnovers,
    player_data:fouls::int                AS personal_fouls,
    player_data:plus_minus::int           AS plus_minus,

    -- 5. Shooting & Rebounds (The full set)
    player_data:rebounds_offensive::int   AS o_reb,
    player_data:rebounds_defensive::int   AS d_reb,
    player_data:rebounds_total::int       AS total_reb,
    
    player_data:fg_made::int              AS fg_made,
    player_data:fg_attempted::int         AS fg_attempted,
    player_data:fg_pct::float             AS fg_pct,
    
    player_data:fg3_made::int             AS fg3_made,
    player_data:fg3_attempted::int        AS fg3_attempted,
    player_data:fg3_pct::float            AS fg3_pct,
    
    player_data:ft_made::int              AS ft_made,
    player_data:ft_attempted::int         AS ft_attempted,
    player_data:ft_pct::float             AS ft_pct,

    -- 6. Time Tracking
    player_data:fetched_at::timestamp_ntz AS api_fetched_at

FROM flattened