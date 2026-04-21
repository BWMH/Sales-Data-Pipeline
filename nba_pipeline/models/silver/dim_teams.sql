SELECT
    {{ dbt_utils.generate_surrogate_key(['a.team_id']) }} as team_pk,
    a.team_id,
    a.team_name,
    a.abbreviation,
    a.nickname,
    a.city,
    a.state,
    a.year_founded,
    b.arena_name,
    b.arena_capacity,
    b.head_coach,
    b.general_manager,
    b.owner,
    b.dleague_affiliation,
    b.last_updated_at as data_freshness_at
    FROM {{ ref('silver_teams') }} a
    LEFT JOIN {{ ref('silver_team_details') }} b
    ON a.team_id = b.team_id
