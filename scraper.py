import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
import re
import os

# Configuration
BASE_URL = "https://nl.zweeler.com"
RATE_LIMIT_SECONDS = 2
OUTPUT_DIR = "data"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def scrape_game_list(year):
    """Scrape game list from Zweeler calendar."""
    url = f"{BASE_URL}/main/calendar/sport/cycling/year/{year}"
    print(f"Scraping game list for {year}...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        games = []
        game_links = soup.find_all('a', href=re.compile(r'/game/\d+/'))
        
        seen_ids = set()
        for link in game_links:
            href = link.get('href')
            match = re.search(r'/game/(\d+)/', href)
            if match:
                game_id = int(match.group(1))
                if game_id not in seen_ids:
                    seen_ids.add(game_id)
                    games.append({
                        'game_id': game_id,
                        'game_name': link.get_text(strip=True),
                        'year': year,
                        'scraped_at': datetime.now().isoformat()
                    })
        
        print(f"Found {len(games)} games")
        return games
    except Exception as e:
        print(f"Error scraping game list: {e}")
        return []
    finally:
        time.sleep(RATE_LIMIT_SECONDS)

def scrape_game_details(game_id):
    """Scrape static game details."""
    url = f"{BASE_URL}/api/v1/gameDetails/{game_id}/gameDetails/EUR"
    print(f"Scraping details for game {game_id}...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        game_details = {
            'game_id': game_id,
            'game_data': json.dumps(data),
            'scraped_at': datetime.now().isoformat()
        }
        
        if isinstance(data, dict):
            game_details['game_name'] = data.get('name', data.get('title', ''))
            game_details['deadline'] = data.get('deadline', '')
            game_details['start_date'] = data.get('startDate', data.get('start', ''))
            game_details['budget'] = data.get('budget', None)
            game_details['sport'] = data.get('sport', 'cycling')
        
        return game_details
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 404:
            print(f"HTTP error for game {game_id}: {e}")
        return None
    except Exception as e:
        print(f"Error scraping game {game_id}: {e}")
        return None
    finally:
        time.sleep(RATE_LIMIT_SECONDS)

def scrape_game_stats(game_id):
    """Scrape dynamic game stats."""
    url = f"{BASE_URL}/api/v1/gameDetails/{game_id}/mostPopular"
    print(f"Scraping stats for game {game_id}...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        stats_records = []
        scraped_at = datetime.now().isoformat()
        
        if isinstance(data, list):
            for item in data:
                stats_records.append({
                    'game_id': game_id,
                    'player_id': str(item.get('id', item.get('playerId', ''))),
                    'player_name': item.get('name', ''),
                    'price': item.get('price', None),
                    'popularity': item.get('popularity', item.get('picks', None)),
                    'position': item.get('position', ''),
                    'team': item.get('team', ''),
                    'stats_data': json.dumps(item),
                    'scraped_at': scraped_at
                })
        elif isinstance(data, dict):
            players = data.get('players', data.get('riders', []))
            for item in players:
                stats_records.append({
                    'game_id': game_id,
                    'player_id': str(item.get('id', item.get('playerId', ''))),
                    'player_name': item.get('name', ''),
                    'price': item.get('price', None),
                    'popularity': item.get('popularity', item.get('picks', None)),
                    'position': item.get('position', ''),
                    'team': item.get('team', ''),
                    'stats_data': json.dumps(item),
                    'scraped_at': scraped_at
                })
        
        return stats_records
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 404:
            print(f"HTTP error for stats {game_id}: {e}")
        return []
    except Exception as e:
        print(f"Error scraping stats {game_id}: {e}")
        return []
    finally:
        time.sleep(RATE_LIMIT_SECONDS)

def main():
    print("="*60)
    print("Zweeler Scraper - Starting")
    print("="*60)
    
    current_year = datetime.now().year
    
    # Scrape game list
    games = scrape_game_list(current_year)
    
    if not games:
        print("No games found, exiting.")
        return
    
    # Save game list
    with open(f"{OUTPUT_DIR}/game_list.json", 'w') as f:
        json.dump(games, f, indent=2)
    print(f"Saved {len(games)} games to game_list.json")
    
    # Scrape details and stats
    all_details = []
    all_stats = []
    
    for game in games:
        game_id = game['game_id']
        
        # Get details
        details = scrape_game_details(game_id)
        if details:
            all_details.append(details)
        
        # Get stats
        stats = scrape_game_stats(game_id)
        if stats:
            all_stats.extend(stats)
    
    # Save details
    with open(f"{OUTPUT_DIR}/game_details.json", 'w') as f:
        json.dump(all_details, f, indent=2)
    print(f"Saved {len(all_details)} game details")
    
    # Save stats
    with open(f"{OUTPUT_DIR}/game_stats.json", 'w') as f:
        json.dump(all_stats, f, indent=2)
    print(f"Saved {len(all_stats)} stat records")
    
    print("="*60)
    print("Scraping completed successfully!")
    print("="*60)

if __name__ == "__main__":
    main()
