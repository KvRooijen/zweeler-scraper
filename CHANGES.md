# Zweeler Scraper Updates - Data Structure Changes

## Summary of Changes

The scraper extracts numeric game codes from calendar links (`/results/20459`) and uses them in API calls.

## New Data Structure

### game_list.json
```json
{
  "game_code": "20459",                              // Numeric ID from /results/ links
  "game_name": "Etoile de Bessèges",
  "year": 2026,
  "scraped_at": "2026-03-25T12:00:00"
}
```

### game_details.json
```json
{
  "game_code": "20459",                              // Numeric ID
  "game_name": "Giro d'Italia (15+5)",
  "base_url": "/game/cyclingTour/FantasyGirodItalia2026/",
  "start_date_time": "2026-05-08 13:30:0",
  "price_per_team": "€12.50",
  "teams_paid": "38",
  "prizes_total": "€7,000.00",
  "teams_limit": "2000",
  "has_subleagues": true,
  "has_schedule": false,
  "has_withdrawals": false,
  "has_ratings": true,
  "game_data_json": "{...full JSON...}",
  "scraped_at": "2026-03-25T12:00:00"
}
```

### game_stats.json
```json
{
  "game_code": "20459",                              // Numeric ID
  "player_id": "17298",                              // Extracted from URL
  "player_name": "Almeida, João",
  "player_url": "/main/startlists/sportsman/17298/almeidajoao",
  "team": "UAE Team Emirates XRG",
  "rank": 1,                                         // Ranking position
  "selection_stats": "101 x (93%)",                  // Selection info
  "on_startlist": true,
  "stats_data_json": "[...]",                        // Full JSON array entry
  "scraped_at": "2026-03-25T12:00:00"
}
```

## Databricks Table Schema

### ingestion.zweeler.game_list
```sql
CREATE TABLE IF NOT EXISTS ingestion.zweeler.game_list (
    game_code STRING,         -- Numeric ID as string
    game_name STRING,
    year INT,
    scraped_at TIMESTAMP
) USING DELTA
```

### ingestion.zweeler.game_details
```sql
CREATE TABLE IF NOT EXISTS ingestion.zweeler.game_details (
    game_code STRING,         -- Numeric ID as string
    game_name STRING,
    base_url STRING,
    start_date_time STRING,
    price_per_team STRING,
    teams_paid STRING,
    prizes_total STRING,
    teams_limit STRING,
    has_subleagues BOOLEAN,
    has_schedule BOOLEAN,
    has_withdrawals BOOLEAN,
    has_ratings BOOLEAN,
    game_data_json STRING,    -- Full API response
    scraped_at TIMESTAMP
) USING DELTA
```

### ingestion.zweeler.game_stats
```sql
CREATE TABLE IF NOT EXISTS ingestion.zweeler.game_stats (
    game_code STRING,         -- Numeric ID as string
    player_id STRING,
    player_name STRING,
    player_url STRING,
    team STRING,
    rank INT,
    selection_stats STRING,
    on_startlist BOOLEAN,
    stats_data_json STRING,   -- Full JSON array entry
    scraped_at TIMESTAMP
) USING DELTA
```

## How It Works

1. **Calendar scraping**: Looks for `<a href="/results/20459">` links
2. **Extract game_code**: Extracts numeric ID (20459)
3. **API calls**:
   - Details: `https://nl.zweeler.com/api/v1/gameDetails/20459/gameDetails/EUR`
   - Stats: `https://nl.zweeler.com/api/v1/gameDetails/20459/mostPopular`

## Logging Example

```
[2026-03-25T19:00:00] [INFO] Scraping game list for 2026...
[2026-03-25T19:00:02] [INFO] ✓ Received response: 200, size: 2510509 bytes
[2026-03-25T19:00:02] [INFO] Found 127 raw game link elements
[2026-03-25T19:00:02] [INFO]   + Added game: 20459 - Etoile de Bessèges
[2026-03-25T19:00:02] [INFO]   + Added game: 20460 - Tour Down Under
[2026-03-25T19:00:02] [INFO] ✓ Successfully scraped 45 unique games for 2026
```

## Next Steps for Databricks

1. Update `GITHUB_USER = "KvRooijen"` in Configuration cell
2. Update table schemas to use `game_code` instead of `game_id`
3. Run after GitHub Actions completes successfully
