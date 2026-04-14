{% snapshot standings_snapshot %}

{{
    config(
      target_database='NBA',
      target_schema='snapshots',
      unique_key='team_id',
      strategy='check',
      updated_at='api_fetched_at',
      check_cols=['conference_rank'],
    )
}}

select * from {{ ref('bronze_standings') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY loaded_at_utc DESC) = 1

{% endsnapshot %}