SELECT
    team_id,
    team_name,
    abbreviation,
    nickname,
    city,
    state,
    year_founded,
    fetched_at,
    updated_at
FROM {{ ref('bronze_teams') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY team_id
    ORDER BY fetched_at DESC
) = 1