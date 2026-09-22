SELECT 
    'player_usage' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT_WS('-', COALESCE(id, ''), COALESCE(name, ''), COALESCE(team, ''), season)) AS unique_rows,
    COUNT(*) - COUNT(DISTINCT CONCAT_WS('-', COALESCE(id, ''), COALESCE(name, ''), COALESCE(team, ''), season)) AS duplicate_count
FROM 
    cfb_transfer_portal_pipeline._01_raw_bronze.player_usage

UNION ALL

SELECT 
    'transfer_portal' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT_WS('-', COALESCE(firstName, ''), COALESCE(lastName, ''), COALESCE(origin, ''), season)) AS unique_rows,
    COUNT(*) - COUNT(DISTINCT CONCAT_WS('-', COALESCE(firstName, ''), COALESCE(lastName, ''), COALESCE(origin, ''), season)) AS duplicate_count
FROM 
    cfb_transfer_portal_pipeline._01_raw_bronze.transfer_portal

UNION ALL

SELECT 
    'recruiting_players' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT_WS('-', COALESCE(id, ''), COALESCE(name, ''), season)) AS unique_rows,
    COUNT(*) - COUNT(DISTINCT CONCAT_WS('-', COALESCE(id, ''), COALESCE(name, ''), season)) AS duplicate_count
FROM 
    cfb_transfer_portal_pipeline._01_raw_bronze.recruiting_players;