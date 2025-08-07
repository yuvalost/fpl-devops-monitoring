import requests
import psycopg2
import pandas as pd

# === DB CONFIG ===
DB_NAME = "premier_league"
DB_USER = "postgres"
DB_PASSWORD = "1q2w3e4r!"
DB_HOST = "localhost"
DB_PORT = "5432"

# === Connect to PostgreSQL ===
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# === Seasons to Load ===
seasons = ["2020-21", "2021-22", "2022-23", "2023-24"]

# === Prepare player ID mapping ===
player_id_map = {}

# === Create table if not exists ===
cur.execute("""
CREATE TABLE IF NOT EXISTS fpl_player_gameweek_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    gameweek INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    bonus INTEGER,
    total_points INTEGER,
    price NUMERIC(5,2),
    team_id INTEGER,
    season VARCHAR(10)
);
""")
conn.commit()

# === Process each season ===
for season in seasons:
    print(f"🔄 Processing season: {season}")
    cur.execute("DELETE FROM fpl_player_gameweek_stats WHERE season = %s", (season,))
    conn.commit()

    # --- Load players_raw.csv to map player info ---
    players_url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/players_raw.csv"
    players_df = pd.read_csv(players_url)

    # Build mapping of player name -> id/team/price
    for _, row in players_df.iterrows():
        full_name = f"{row['first_name']} {row['second_name']}"
        player_id_map[full_name] = {
            'id': row['id'],
            'team_id': row['team'],
            'now_cost': row['now_cost'] / 10.0
        }

    # --- Load gameweek stats ---
    for gw in range(1, 39):
        gw_url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/gws/gw{gw}.csv"
        try:
            gw_df = pd.read_csv(gw_url)
        except Exception:
            continue

        for _, row in gw_df.iterrows():
            full_name = row['name']
            mapped = player_id_map.get(full_name, {})

            cur.execute("""
                INSERT INTO fpl_player_gameweek_stats (
                    player_id, gameweek, minutes, goals_scored, assists,
                    yellow_cards, red_cards, bonus, total_points,
                    price, team_id, season
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                mapped.get('id'),
                gw,
                row['minutes'],
                row['goals_scored'],
                row['assists'],
                row['yellow_cards'],
                row['red_cards'],
                row['bonus'],
                row['total_points'],
                mapped.get('now_cost'),
                mapped.get('team_id'),
                season
            ))

        print(f"   ✅ GW{gw}: {len(gw_df)} records")

# Finalize
conn.commit()
cur.close()
conn.close()
print("✅ All historical data (2020–2024) loaded into fpl_player_gameweek_stats.")
