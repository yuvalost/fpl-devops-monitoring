import requests
import psycopg2
import pandas as pd

# === DB CONFIG ===
DB_NAME = "premier_league"
DB_USER = "postgres"
DB_PASSWORD = "1q2w3e4r!"
DB_HOST = "localhost"
DB_PORT = "5432"

# === CONNECT TO DB ===
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()
print("✅ Connected to PostgreSQL")

# === CLEAR EXISTING DATA ===
cur.execute("DELETE FROM fpl_player_gameweek_stats")
conn.commit()
print("🧹 Cleared old data from fpl_player_gameweek_stats")

# === 1. Load Historical Stats (2020-24) ===
seasons = ["2020-21", "2021-22", "2022-23", "2023-24"]
for season in seasons:
    print(f"📦 Processing historical season: {season}")
    for gw in range(1, 39):
        url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/gws/gw{gw}.csv"
        try:
            df = pd.read_csv(url)
        except Exception:
            print(f"⚠️ GW {gw} data missing for {season}")
            continue

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO fpl_player_gameweek_stats (
                    player_id, gameweek, minutes, goals_scored, assists,
                    bonus, yellow_cards, red_cards, total_points,
                    price, team_id, season
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['element'],
                gw,
                row['minutes'],
                row['goals_scored'],
                row['assists'],
                row['bonus'],
                row['yellow_cards'],
                row['red_cards'],
                row['total_points'],
                row['value'] / 10.0,
                row['team'],
                season
            ))
        print(f"   ✅ GW {gw} inserted for {season}")

# === 2. Load Live Stats from FPL API ===
bootstrap = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()
events = bootstrap['events']
finished_gws = [e['id'] for e in events if e.get("finished")]

print(f"\n🌐 Loading current season GWs: {finished_gws}")

for gw in finished_gws:
    url = f"https://fantasy.premierleague.com/api/event/{gw}/live/"
    try:
        res = requests.get(url)
        res.raise_for_status()
        gw_data = res.json()
        elements = gw_data.get("elements", [])
    except Exception as e:
        print(f"❌ Failed to fetch GW {gw}:", e)
        continue

    for entry in elements:
        stats = entry["stats"]
        cur.execute("""
            INSERT INTO fpl_player_gameweek_stats (
                player_id, gameweek, minutes, goals_scored, assists,
                bonus, yellow_cards, red_cards, total_points,
                price, team_id, season
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            entry["id"],
            gw,
            stats["minutes"],
            stats["goals_scored"],
            stats["assists"],
            stats["bonus"],
            stats["yellow_cards"],
            stats["red_cards"],
            stats["total_points"],
            stats["value"] / 10.0,
            stats["team"],
            "2024-25"
        ))

    print(f"✅ Inserted live stats for GW {gw}")

# === Finalize ===
conn.commit()
cur.close()
conn.close()
print("\n✅ All data loaded successfully into fpl_player_gameweek_stats")
