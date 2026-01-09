import requests
import re

# HEADERS (Make sure we look like a real browser)
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def diagnose_espn():
    print("--- DIAGNOSTIC TEST START ---")
    
    # STEP 1: Check if we can reach ESPN at all
    print("\n1. Testing Team List API...")
    try:
        url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=5"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        print(f"   Status Code: {resp.status_code}")
        
        if resp.status_code == 403:
            print("   🔴 CRITICAL: You are receiving a 403 FORBIDDEN error.")
            print("   This means ESPN has soft-banned your IP address for scraping too fast.")
            return
        elif resp.status_code != 200:
            print(f"   🔴 Error: API returned code {resp.status_code}")
            return
            
        # Get a real Team ID to test with
        team_id = resp.json()['sports'][0]['leagues'][0]['teams'][0]['team']['id']
        print(f"   ✅ Success. Using Team ID: {team_id} for next step.")
        
    except Exception as e:
        print(f"   🔴 Connection Failed: {e}")
        return

    # STEP 2: Fetch a Schedule to find a Game ID
    print(f"\n2. Fetching Schedule for Team {team_id}...")
    try:
        sched_url = f"https://www.espn.com/mens-college-basketball/team/schedule/_/id/{team_id}/season/2026"
        resp = requests.get(sched_url, headers=HEADERS, timeout=10)
        
        # Extract Game IDs using the same logic as your main script
        ids = re.findall(r'/gameId/(\d+)', resp.text)
        valid_ids = [gid for gid in ids if len(gid) >= 7]
        
        if not valid_ids:
            print("   🔴 Found 0 Game IDs. The regex parser might be broken.")
            return
        
        print(f"   ✅ Success. Found {len(valid_ids)} games. Testing the first one: {valid_ids[0]}")
        test_gid = valid_ids[0]
        
    except Exception as e:
        print(f"   🔴 Schedule Fetch Failed: {e}")
        return

    # STEP 3: The Moment of Truth - Fetch the Stats
    print(f"\n3. Fetching Summary for Game ID {test_gid}...")
    summary_url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={test_gid}"
    
    try:
        resp = requests.get(summary_url, headers=HEADERS, timeout=10)
        print(f"   API Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            if 'boxscore' in data and 'teams' in data['boxscore']:
                print("   🟢 RESULT: SUCCESS! We got stats.")
                print("   (If your main script is failing, it might be hitting 'Future' games.)")
            else:
                print("   🟡 RESULT: 200 OK, but no 'boxscore' found.")
                print("   This likely means the game is in the FUTURE or Cancelled.")
                date = data.get('header', {}).get('competitions', [{}])[0].get('date')
                print(f"   Game Date: {date}")
        else:
            print(f"   🔴 RESULT: Failed with code {resp.status_code}")
            
    except Exception as e:
        print(f"   🔴 Summary Fetch Failed: {e}")

diagnose_espn()
