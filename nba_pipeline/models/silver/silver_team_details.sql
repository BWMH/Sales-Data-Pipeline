SELECT
    team_id,
    abbreviation,
    nickname,
    city,
    state,
    arena_name,
    arena_capacity,
    head_coach,
    general_manager,
    owner,
    dleague_affiliation,
    year_founded,
    dbt_valid_from AS record_effective_date,
    loaded_at_utc  AS last_updated_at,
    TRUE AS is_current_record
FROM {{ ref('team_details_snapshot') }}

WHERE dbt_valid_to IS NULL