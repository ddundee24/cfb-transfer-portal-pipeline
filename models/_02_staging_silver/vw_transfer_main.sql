WITH 
    player_usage AS (
        SELECT
            id AS player_id,
            name AS player_name,
            season,
            team AS season_team,
            `usage.overall` AS total_season_usage
        FROM 
        {{ source('_01_raw_bronze', 'player_usage') }}
    ),
    player_transfer AS (



        
    )

SELECT * FROM player_usage