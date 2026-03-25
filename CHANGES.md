# Zweeler Scraper Updates - Data Structure Changes

## Summary of Changes

The scraper has been completely rewritten based on actual Zweeler website structure. Here are the key changes:

## New Data Structure

### game_list.json
```json
{
  "game_id": "cyclingTour_FantasyGirodItalia2026",  // NEW: composite ID
  "game_slug": "FantasyGirodItalia2026",             // NEW: used in URLs
  "game_type": "cyclingTour",                        // NEW: cycling or cyclingTour
  "game_name": "Giro d'Italia (15+5)",
  "game_url": "https://nl.zweeler.com/game/cyclingTour/FantasyGirodItalia2026/",
  "year": 2026,
  "scraped_at": "2026-03-25T12:00:00"
}
```

### game_details.json
```json
{
  "game_id": "cyclingTour_FantasyGirodItalia2026",
  "game_slug": "FantasyGirodItalia2026",
  "game_type": "cyclingTour",
  "game_name": "Giro d'Italia (15+5)",
  "base_url": "/game/cyclingTour/FantasyGirodItalia2026/",
  "start_date_time": "2026-05-08 13:30:0",          // NEW: actual field name
  "price_per_team": "€12.50",                        // NEW
  "teams_paid": "38",                                // NEW
  "prizes_total": "€7,000.00",                       // NEW
  "teams_limit": "2000",                             // NEW
  "has_subleagues": true,                            // NEW
  "has_schedule": false,                             // NEW
  "has_withdrawals": false,                          // NEW
  "has_ratings": true,                               // NEW
  "game_data_json": "{...full JSON...}",            // RENAMED from game_data
  "scraped_at": "2026-03-25T12:00:00"
}
```

### game_stats.json
```json
{
  "game_id": "cyclingTour_FantasyGirodItalia2026",
  "game_slug": "FantasyGirodItalia2026",
  "player_id": "17298",                              // NEW: extracted from URL
  "player_name": "Almeida, João",
  "player_url": "/main/startlists/sportsman/17298/almeidajoao", // NEW
  "team": "UAE Team Emirates XRG",
  "rank": 1,                                         // NEW: ranking position
  "selection_stats": "101 x (93%)",                  // NEW: replaces popularity
  "on_startlist": true,                              // NEW
  "stats_data_json": "[...]",                        // RENAMED from stats_data
  "scraped_at": "2026-03-25T12:00:00"
}
```

## Databricks Table Schema Updates Needed

### ingestion.zweeler.game_list
```sql
CREATE TABLE IF NOT EXISTS ingestion.zweeler.game_list (
    game_id STRING,           -- CHANGED from BIGINT
    game_slug STRING,         -- NEW
    game_type STRING,         -- NEW
    game_name STRING,
    game_url STRING,          -- NEW
    year INT,
    scraped_at TIMESTAMP
) USING DELTA
```

### ingestion.zweeler.game_details
```sql
CREATE TABLE IF NOT EXISTS ingestion.zweeler.game_details (
    game_id STRING,           -- CHANGED from BIGINT
    game_slug STRING,         -- NEW
    game_type STRING,         -- NEW
    game_name STRING,
    base_url STRING,          -- NEW
    start_date_time STRING,   -- NEW (renamed from start_date)
    price_per_team STRING,    -- NEW
    teams_paid STRING,        -- NEW
    prizes_total STRING,      -- NEW
    teams_limit STRING,       -- NEW
    has_subleagues BOOLEAN,   -- NEW
    has_schedule BOOLEAN,     -- NEW
    has_withdrawals BOOLEAN,  -- NEW
    has_ratings BOOLEAN,      -- NEW
    game_data_json STRING,    -- RENAMED from game_data
    scraped_at TIMESTAMP
) USING DELTA
```

### ingestion.zweeler.game_stats
```sql
CREATE TABLE IF NOT EXISTS ingestion.zweeler.game_stats (
    game_id STRING,           -- CHANGED from BIGINT
    game_slug STRING,         -- NEW
    player_id STRING,
    player_name STRING,
    player_url STRING,        -- NEW
    team STRING,
    rank INT,                 -- NEW
    selection_stats STRING,   -- NEW (replaces popularity/price)
    on_startlist BOOLEAN,     -- NEW
    stats_data_json STRING,   -- RENAMED from stats_data
    scraped_at TIMESTAMP
) USING DELTA
```

## What Was Removed

From game_details:
- `deadline` (not in API response)
- `budget` (not in API response)
- `sport` (not in API response)

From game_stats:
- `price` (not available, use selection_stats instead)
- `popularity` (not available, use selection_stats instead)
- `position` (not available)

## Logging & Debugging

The new scraper includes extensive logging:
- Timestamps for all operations
- HTTP response sizes
- JSON parsing confirmations
- Error details with stack traces
- Progress indicators for multi-game scraping

Check GitHub Actions logs for detailed output!

## Next Steps for Databricks Notebook

1. Update `GITHUB_USER = "KvRooijen"` in the Configuration cell
2. Update table schemas in the load functions to match new structure above
3. Run the notebook after GitHub Actions completes successfully
