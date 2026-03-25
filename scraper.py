import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
import re
import os
import sys

# Configuration
BASE_URL = "https://nl.zweeler.com"
RATE_LIMIT_SECONDS = 2
OUTPUT_DIR = "data"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log(message, level="INFO"):
    """Verbose logging function"""
    timestamp = datetime.now().isoformat()
    print(f"[{timestamp}] [{level}] {message}", flush=True)

def scrape_game_list(year):
    """
    Scrape game list from Zweeler calendar for a given year.
    Returns a list of dictionaries with game_code (numeric ID) and game_name.
    """
    url = f"{BASE_URL}/main/calendar/sport/cycling/year/{year}"
    log(f"Scraping game list for {year} from {url}")
    
    try:
        log("Sending HTTP GET request...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        log(f"✓ Received response: {response.status_code}, size: {len(response.content)} bytes")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all game links - they follow pattern /results/{game_code}
        game_links = soup.find_all('a', href=re.compile(r'/results/\d+'))
        log(f"Found {len(game_links)} raw game link elements")
        
        games = []
        seen_codes = set()
        
        for link in game_links:
            href = link.get('href', '')
            
            # Extract numeric game code from URL
            match = re.search(r'/results/(\d+)', href)
            if match:
                game_code = match.group(1)
                
                if game_code not in seen_codes:
                    seen_codes.add(game_code)
                    
                    # Get game name from link text
                    game_name = link.get_text(strip=True)
                    if not game_name:
                        game_name = f"Game {game_code}"
                    
                    games.append({
                        'game_code': game_code,
                        'game_name': game_name,
                        'year': year,
                        'scraped_at': datetime.now().isoformat()
                    })
                    log(f"  + Added game: {game_code} - {game_name}")
        
        log(f"✓ Successfully scraped {len(games)} unique games for {year}")
        return games
        
    except requests.exceptions.Timeout:
        log(f"✗ Request timeout after 30 seconds", "ERROR")
        return []
    except requests.exceptions.HTTPError as e:
        log(f"✗ HTTP error: {e.response.status_code} - {e}", "ERROR")
        return []
    except Exception as e:
        log(f"✗ Unexpected error scraping game list: {type(e).__name__}: {str(e)}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        return []
    finally:
        log(f"Waiting {RATE_LIMIT_SECONDS} seconds (rate limit)...")
        time.sleep(RATE_LIMIT_SECONDS)

def scrape_game_details(game_code, game_name):
    """
    Scrape static game details from the API.
    Uses the numeric game code.
    """
    # The API endpoint format: /api/v1/gameDetails/{game_code}/gameDetails/EUR
    url = f"{BASE_URL}/api/v1/gameDetails/{game_code}/gameDetails/EUR"
    log(f"Scraping details for {game_code} ({game_name})")
    log(f"  URL: {url}")
    
    try:
        log("  Sending HTTP GET request...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        log(f"  ✓ Received response: {response.status_code}, size: {len(response.content)} bytes")
        
        data = response.json()
        log(f"  ✓ Parsed JSON successfully")
        
        # Extract relevant fields from the API response
        game_details = {
            'game_code': game_code,
            'game_name': data.get('gameName', game_name),
            'base_url': data.get('baseUrl', ''),
            'start_date_time': data.get('startDateAndTime', ''),
            'price_per_team': data.get('pricePerTeam', ''),
            'teams_paid': data.get('numberOfTeamsPaid', ''),
            'prizes_total': data.get('prizesTotal', ''),
            'teams_limit': data.get('teamsLimit', ''),
            'has_subleagues': data.get('hasSubleagues', False),
            'has_schedule': data.get('hasSchedule', False),
            'has_withdrawals': data.get('hasWithdrawals', False),
            'has_ratings': data.get('hasRatings', False),
            'game_data_json': json.dumps(data),  # Store full JSON
            'scraped_at': datetime.now().isoformat()
        }
        
        log(f"  ✓ Extracted details for: {game_details['game_name']}")
        return game_details
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            log(f"  ✗ Game details not found (404) - this game may not be available yet", "WARN")
        else:
            log(f"  ✗ HTTP error: {e.response.status_code} - {e}", "ERROR")
        return None
    except json.JSONDecodeError as e:
        log(f"  ✗ Failed to parse JSON response: {e}", "ERROR")
        log(f"  Response content: {response.text[:500]}", "ERROR")
        return None
    except Exception as e:
        log(f"  ✗ Unexpected error: {type(e).__name__}: {str(e)}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        return None
    finally:
        log(f"  Waiting {RATE_LIMIT_SECONDS} seconds (rate limit)...")
        time.sleep(RATE_LIMIT_SECONDS)

def scrape_game_stats(game_code, game_name):
    """
    Scrape dynamic game stats (player popularity, selections) from the API.
    Uses the numeric game code.
    """
    url = f"{BASE_URL}/api/v1/gameDetails/{game_code}/mostPopular"
    log(f"Scraping stats for {game_code} ({game_name})")
    log(f"  URL: {url}")
    
    try:
        log("  Sending HTTP GET request...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        log(f"  ✓ Received response: {response.status_code}, size: {len(response.content)} bytes")
        
        data = response.json()
        log(f"  ✓ Parsed JSON successfully")
        
        stats_records = []
        scraped_at = datetime.now().isoformat()
        
        # Navigate to the mostPopularTable entries
        if 'tables' in data and 'mostPopularTable' in data['tables']:
            entries = data['tables']['mostPopularTable'].get('entries', [])
            log(f"  Found {len(entries)} player entries")
            
            for entry in entries:
                # Entry format: [rank, flag_info, player_info, team, selection_stats]
                if len(entry) >= 5:
                    rank = entry[0]
                    player_info = entry[2] if isinstance(entry[2], dict) else {}
                    team = entry[3] if len(entry) > 3 else ''
                    selection_stats = entry[4] if len(entry) > 4 else ''
                    
                    player_name = player_info.get('sportsmen_name', '').strip()
                    player_url = player_info.get('sportsment_url', '')
                    
                    # Try to extract player ID from URL
                    player_id = ''
                    if player_url:
                        id_match = re.search(r'/sportsman/(\d+)/', player_url)
                        if id_match:
                            player_id = id_match.group(1)
                    
                    stats_records.append({
                        'game_code': game_code,
                        'player_id': player_id,
                        'player_name': player_name,
                        'player_url': player_url,
                        'team': team,
                        'rank': rank,
                        'selection_stats': selection_stats,
                        'on_startlist': player_info.get('onStartlist', False),
                        'stats_data_json': json.dumps(entry),
                        'scraped_at': scraped_at
                    })
            
            log(f"  ✓ Extracted {len(stats_records)} player stat records")
        else:
            log(f"  ⚠ No mostPopularTable found in response", "WARN")
            log(f"  Response keys: {list(data.keys())}", "WARN")
        
        return stats_records
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            log(f"  ✗ Stats not available (404) - this game may not have started yet", "WARN")
        else:
            log(f"  ✗ HTTP error: {e.response.status_code} - {e}", "ERROR")
        return []
    except json.JSONDecodeError as e:
        log(f"  ✗ Failed to parse JSON response: {e}", "ERROR")
        log(f"  Response content: {response.text[:500]}", "ERROR")
        return []
    except Exception as e:
        log(f"  ✗ Unexpected error: {type(e).__name__}: {str(e)}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        return []
    finally:
        log(f"  Waiting {RATE_LIMIT_SECONDS} seconds (rate limit)...")
        time.sleep(RATE_LIMIT_SECONDS)

def main():
    log("="*60)
    log("Zweeler Scraper - Starting")
    log("="*60)
    
    current_year = datetime.now().year
    log(f"Current year: {current_year}")
    
    # Scrape game list
    log("\n--- Phase 1: Scraping Game List ---")
    games = scrape_game_list(current_year)
    
    if not games:
        log("✗ No games found, exiting.", "ERROR")
        sys.exit(1)
    
    # Save game list
    game_list_file = f"{OUTPUT_DIR}/game_list.json"
    with open(game_list_file, 'w', encoding='utf-8') as f:
        json.dump(games, f, indent=2, ensure_ascii=False)
    log(f"✓ Saved {len(games)} games to {game_list_file}")
    
    # Scrape details and stats
    log("\n--- Phase 2: Scraping Game Details and Stats ---")
    all_details = []
    all_stats = []
    
    for i, game in enumerate(games, 1):
        log(f"\n[{i}/{len(games)}] Processing: {game['game_code']} - {game['game_name']}")
        game_code = game['game_code']
        game_name = game['game_name']
        
        # Get details
        details = scrape_game_details(game_code, game_name)
        if details:
            all_details.append(details)
            log(f"  ✓ Details scraped successfully")
        else:
            log(f"  ⚠ Details not available", "WARN")
        
        # Get stats
        stats = scrape_game_stats(game_code, game_name)
        if stats:
            all_stats.extend(stats)
            log(f"  ✓ Stats scraped successfully ({len(stats)} players)")
        else:
            log(f"  ⚠ Stats not available", "WARN")
    
    # Save details
    details_file = f"{OUTPUT_DIR}/game_details.json"
    with open(details_file, 'w', encoding='utf-8') as f:
        json.dump(all_details, f, indent=2, ensure_ascii=False)
    log(f"\n✓ Saved {len(all_details)} game details to {details_file}")
    
    # Save stats
    stats_file = f"{OUTPUT_DIR}/game_stats.json"
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(all_stats, f, indent=2, ensure_ascii=False)
    log(f"✓ Saved {len(all_stats)} stat records to {stats_file}")
    
    log("\n" + "="*60)
    log("Scraping completed successfully!")
    log(f"Summary:")
    log(f"  - Games found: {len(games)}")
    log(f"  - Game details scraped: {len(all_details)}")
    log(f"  - Player stat records: {len(all_stats)}")
    log("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n✗ Scraper interrupted by user", "ERROR")
        sys.exit(1)
    except Exception as e:
        log(f"\n✗ Fatal error: {type(e).__name__}: {str(e)}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        sys.exit(1)
