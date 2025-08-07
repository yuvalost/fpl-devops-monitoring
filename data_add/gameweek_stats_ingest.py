import requests
import psycopg2

# === DB CONFIG ===
DB_NAME = "premier_league"
DB_USER = "postgres"
DB_PASSWORD = "1q2w3e4r!"
DB_HOST = "localhost"
DB_PORT = "5432"

# === CONNECT TO DB ===
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    print("✅ Connected to PostgreSQL.")
except Exception as e:
    print("❌ Database connection error:", e)
    exit()

# === FETCH BOOTSTRAP DATA ===
try:
    bootstrap = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()
    print("✅ FPL bootstrap data fetched.")
except Exception as e:
    print("❌ Failed to fetch bootstrap data:", e)
    exit()

# Determine latest gameweek
finished_events = [e for e in bootstrap['events'] if e.get('finished')]
if finished_events:
    latest_gw = max(e['id'] for e in finished_events)
    print(f"✅ Latest finished gameweek: {latest_gw}")
else:
    fallback = next((e for e in bootstrap['events'] if e.get('is_current')), None)
    latest_gw = fallback['id'] if fallback else max(e['id'] for e in bootstrap['events'])
    print(f"⚠️ No finished gameweeks. Using fallback GW: {latest_gw}")

# Clear old stats
cur.execute("DELETE FROM fpl_player_gameweek_stats")
print("🧹 Cleared old stats.")

# Loop through each gameweek
for gw in range(1, latest_gw + 1):
    url = f"https://fantasy.premierleague.com/api/event/{gw}/live/"
    print(f"\n🌐 Fetching GW {gw} from {url}")
    try:
        res = requests.get(url)
        res.raise_for_status()
        gw_data = res.json()
        stats_list = gw_data.get("elements", [])
        print(f"📦 GW {gw} contains {len(stats_list)} player records.")
    except Exception as e:
        print(f"❌ Failed to fetch GW {gw}: {e}")
        continue

    if not stats_list:
        print(f"⚠️ No stats found for GW {gw}. Skipping.")
        continue

    for entry in stats_list:
        try:
            player_id = entry.get("id")
            stats = entry.get("stats", {})

            # Lookup extra player info from bootstrap data
            player_info = next((p for p in bootstrap['elements'] if p['id'] == player_id), {})
            now_cost = player_info.get("now_cost", 0) / 10.0
            team_id = player_info.get("team", None)

            cur.execute("""
                INSERT INTO fpl_player_gameweek_stats (
                    player_id, gameweek, minutes, goals_scored, assists,
                    yellow_cards, red_cards, bonus, total_points,
                    price, team_id, season
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                player_id,
                gw,
                stats.get("minutes", 0),
                stats.get("goals_scored", 0),
                stats.get("assists", 0),
                stats.get("yellow_cards", 0),
                stats.get("red_cards", 0),
                stats.get("bonus", 0),
                stats.get("total_points", 0),
                now_cost,
                team_id,
                "2024/25"
            ))
        except Exception as e:
            print(f"❌ Insert error (GW {gw}, Player {player_id}):", e)

    print(f"✅ Inserted stats for GW {gw}")

# Finalize
conn.commit()
cur.close()
conn.close()
print("\n✅ All gameweek stats loaded.")
