# Zweeler Cycling Games Scraper

Automated scraper for Zweeler cycling fantasy game data, running on GitHub Actions and integrated with Databricks.

## 🎯 What This Does

This project automatically scrapes cycling game data from Zweeler.com daily and stores it in a format that can be loaded into Databricks Delta tables.

### Data Collected

1. **Game List** - All cycling games for the current year
2. **Game Details** - Static information (name, deadline, budget, etc.)
3. **Game Stats** - Dynamic player pricing and popularity data

## 🏗️ Architecture

```
Zweeler.com → GitHub Actions (scraper) → GitHub Repo (JSON files)
                                              ↓
                                   raw.githubusercontent.com
                                              ↓
                                         Databricks
                                              ↓
                                    ingestion.zweeler.* tables
```

## 📅 Schedule

The scraper runs automatically every day at 6:00 AM UTC via GitHub Actions.

You can also trigger it manually:
1. Go to the **Actions** tab
2. Select **Scrape Zweeler Cycling Data**
3. Click **Run workflow**

## 📊 Data Structure

### data/game_list.json
```json
[
  {
    "game_id": 20645,
    "game_name": "Paris-Nice",
    "year": 2026,
    "scraped_at": "2026-03-25T12:00:00"
  }
]
```

### data/game_details.json
```json
[
  {
    "game_id": 20645,
    "game_name": "Paris-Nice",
    "deadline": "2026-03-08T12:00:00",
    "start_date": "2026-03-08",
    "budget": 100.0,
    "sport": "cycling",
    "game_data": "{...full json...}",
    "scraped_at": "2026-03-25T12:00:00"
  }
]
```

### data/game_stats.json
```json
[
  {
    "game_id": 20645,
    "player_id": "12345",
    "player_name": "Tadej Pogačar",
    "price": 25.0,
    "popularity": 45.2,
    "position": "Alround",
    "team": "UAE Team Emirates",
    "stats_data": "{...full json...}",
    "scraped_at": "2026-03-25T12:00:00"
  }
]
```

## 🚀 Loading Data in Databricks

Use the companion Databricks notebook **Load Zweeler Data from GitHub** to:
1. Fetch JSON files from this repository
2. Load into Delta tables at `ingestion.zweeler.*`
3. Track historical changes

## ⚙️ Configuration

### Rate Limiting
- 2 seconds between requests to be respectful to Zweeler servers

### Years Scraped
- Currently: Current year only
- To add historical data, modify `scraper.py`:
  ```python
  for year in range(2023, datetime.now().year + 1):
      games.extend(scrape_game_list(year))
  ```

## 📝 Files

- `.github/workflows/scrape-zweeler.yml` - GitHub Actions workflow
- `scraper.py` - Python scraper script
- `data/` - Output directory for JSON files
- `README.md` - This file

## 🔧 Local Testing

```bash
# Install dependencies
pip install requests beautifulsoup4

# Run scraper
python scraper.py

# Check output
ls -l data/
```

## 📜 License

This is a personal data collection project. Please be respectful of Zweeler's terms of service and rate limits.

## 🙏 Credits

Built with:
- GitHub Actions for automation
- Python + BeautifulSoup for scraping
- Databricks for data storage and analysis
