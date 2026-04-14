SELECT
    CONFERENCE,
    CONFERENCE_RANK,
    DIVISION,
    HOME_RECORD,
    LAST_10,
    LOSSES,
    ROAD_RECORD,
    STREAK,
    TEAM_ID,
    team_name,
    WIN_PCT,
    WINS,
    dbt_valid_from AS record_effective_date,
    loaded_at_utc  AS last_updated_at,
    TRUE AS is_current_record
FROM {{ ref('standings_snapshot') }}

WHERE dbt_valid_to IS NULL