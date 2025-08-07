import requests
import psycopg2
import pandas as pd
import time

DB_NAME = "premier_league"
DB_USER = "postgres"
DB_PASSWORD = "1q2w3e4r!"
DB_HOST = "postgres"
DB_PORT = "5432"

while True:
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Connected to PostgreSQL.")
        break
    except Exception as e:
        print("⏳ Waiting for DB...", e)
        time.sleep(5)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    fpl_id INTEGER UNIQUE,
    name TEXT,
    team_name TEXT,
    goals_scored INTEGER
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS fpl_player_gameweek_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    gameweek INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    bonus INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    total_points INTEGER,
    price NUMERIC,
    team_id INTEGER,
    season VARCHAR(20)
);
""")
conn.commit()

seasons = ["2020-21", "2021-22", "2022-23", "2023-24"]
for season in seasons:
    print(f"📦 Loading historical data for {season}")
    for gw in range(1, 39):
        url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/gws/gw{gw}.csv"
        try:
            df = pd.read_csv(url)
        except Exception as e:
            print(f"⚠️ Could not load GW{gw} for {season}: {e}")
            continue

        for _, row in df.iterrows():
            try:
                conn.cursor().execute("""
                    INSERT INTO fpl_player_gameweek_stats (
                        player_id, gameweek, minutes, goals_scored, assists,
                        bonus, yellow_cards, red_cards, total_points,
                        price, team_id, season
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    int(row['element']),
                    int(row['round']),
                    int(row['minutes']),
                    int(row['goals_scored']),
                    int(row['assists']),
                    int(row['bonus']),
                    int(row['yellow_cards']),
                    int(row['red_cards']),
                    int(row['total_points']),
                    float(row['value']) / 10.0,
                    int(row['team']),
                    season
                ))
                conn.commit()
            except Exception as e:
                print(f"❌ Insert error (GW{gw}, Player {row.get('element')}):", e)
                conn.rollback()

try:
    bootstrap = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()
    print("✅ FPL bootstrap data fetched.")
except Exception as e:
    print("❌ Failed to fetch bootstrap data:", e)
    exit()

teams = {t['id']: t['name'] for t in bootstrap['teams']}

for p in bootstrap['elements']:
    fpl_id = p['id']
    name = f"{p['first_name']} {p['second_name']}"
    goals = p.get('goals_scored', 0)
    team_name = teams.get(p['team'], 'Unknown')

    try:
        conn.cursor().execute("""
            INSERT INTO players (fpl_id, name, team_name, goals_scored)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fpl_id) DO UPDATE SET
                name = EXCLUDED.name,
                team_name = EXCLUDED.team_name,
                goals_scored = EXCLUDED.goals_scored;
        """, (fpl_id, name, team_name, goals))
        conn.commit()
    except Exception as e:
        print(f"❌ Player insert error (FPL ID {fpl_id}):", e)
        conn.rollback()

print("✅ Players table populated.")

finished_events = [e for e in bootstrap['events'] if e.get('finished')]
if finished_events:
    latest_gw = max(e['id'] for e in finished_events)
    current_season = bootstrap['events'][0].get('season', '2024-25')
    print(f"✅ Latest finished GW: {latest_gw} ({current_season})")
else:
    latest_gw = 1
    current_season = '2024-25'
    print("⚠️ Defaulting to GW1")

for gw in range(1, latest_gw + 1):
    url = f"https://fantasy.premierleague.com/api/event/{gw}/live/"
    print(f"🌍 Fetching GW{gw} data...")
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        for player in data['elements']:
            stats = player.get('stats', {})
            try:
                conn.cursor().execute("""
                    INSERT INTO fpl_player_gameweek_stats (
                        player_id, gameweek, minutes, goals_scored, assists,
                        bonus, yellow_cards, red_cards, total_points,
                        price, team_id, season
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    player.get('id'),
                    gw,
                    stats.get('minutes', 0),
                    stats.get('goals_scored', 0),
                    stats.get('assists', 0),
                    stats.get('bonus', 0),
                    stats.get('yellow_cards', 0),
                    stats.get('red_cards', 0),
                    stats.get('total_points', 0),
                    current_season
                ))
                conn.commit()
            except Exception as e:
                print(f"❌ Insert error (live GW{gw}, Player {player.get('id')}):", e)
                conn.rollback()
        print(f"✅ GW{gw} inserted.")
    except Exception as e:
        print(f"❌ Failed GW{gw}:", e)

conn.close()
print("✅ All historical and live data loaded.")
