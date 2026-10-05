SELECT
    CAST(id AS string) AS player_id,
    CAST(name AS string) AS player_name,
    CAST(position AS string) AS player_position,
    CAST(season AS string),
    CAST(team AS string) AS season_team,
    CAST(
        ROUND(
            `usage.overall`,
            3
        )
        AS double
    ) AS total_season_usage
FROM 
     {{ source('_01_raw_bronze', 'player_usage') }}