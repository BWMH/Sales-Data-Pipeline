SELECT
    player_id,
    full_name,
    birthdate,
    country,
    height,
    weight_lbs,
    jersey_number,
    position,
    team_id,
    team_name,
    team_abbreviation,
    draft_year,
    draft_round,
    draft_number,
    career_from_year,
    career_to_year,
    dbt_valid_from AS record_effective_date,
    loaded_at_utc  AS last_updated_at,
    TRUE AS is_current_record
FROM {{ ref('player_details_snapshot') }}

WHERE dbt_valid_to IS NULL