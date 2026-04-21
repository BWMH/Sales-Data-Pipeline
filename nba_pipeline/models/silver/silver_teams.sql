SELECT
    team_version_id,
    team_id,
    team_name,
    abbreviation,
    nickname,
    city,
    state,
    year_founded,
    api_fetched_at
FROM {{ ref('bronze_teams') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY team_id
    ORDER BY api_fetched_at DESC
) = 1