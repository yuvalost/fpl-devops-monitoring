import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values

DB_NAME = os.getenv("DB_NAME", "premier_league")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1q2w3e4r!")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")

SEASON = "2024-25"  # update as needed

def connect():
    while True:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
            )
            conn.autocommit = False
            print("✅ Connected to PostgreSQL.")
            return conn
        except Exception as e:
            print("⏳ Waiting for DB...", e)
            time.sleep(4)

def safe_int(v):
    try:
        return int(v)
    except:
        return None

def safe_float(v):
    try:
        return float(v)
    except:
        return None

def update_current():
    conn = connect()
    cur = conn.cursor()

    # Bootstrap to get teams + players
    bootstrap = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()

    # Teams
    team_rows = []
    for t in bootstrap["teams"]:
        team_rows.append((
            t["id"], t["name"], t["short_name"], SEASON
        ))
    execute_values(cur, """
        INSERT INTO fpl_teams (team_id, name, short_name, season)
        VALUES %s
        ON CONFLICT (team_id, season) DO UPDATE
        SET name = EXCLUDED.name, short_name = EXCLUDED.short_name;
    """, team_rows)

    # Players
    pos_map = {1:"GK", 2:"DEF", 3:"MID", 4:"FWD"}
    player_rows = []
    for p in bootstrap["elements"]:
        player_rows.append((
            p["id"], p["web_name"], p["first_name"], p["second_name"],
            pos_map.get(p["element_type"], None), p["team"], SEASON
        ))
    execute_values(cur, """
        INSERT INTO fpl_players (fpl_id, web_name, first_name, second_name, position, team_id, season)
        VALUES %s
        ON CONFLICT (fpl_id, season) DO UPDATE
        SET web_name = EXCLUDED.web_name,
            first_name = EXCLUDED.first_name,
            second_name = EXCLUDED.second_name,
            position = EXCLUDED.position,
            team_id = EXCLUDED.team_id;
    """, player_rows)

    # Latest finished GW
    finished = [e for e in bootstrap["events"] if e.get("finished")]
    if not finished:
        print("⚠ No finished gameweeks yet.")
        conn.commit()
        conn.close()
        return
    latest_gw = max(e["id"] for e in finished)
    print(f"✅ Latest finished GW: {latest_gw}")

    # Insert GW stats
    for gw in range(1, latest_gw+1):
        print(f"🌍 Fetching GW{gw}...")
        gw_data = requests.get(f"https://fantasy.premierleague.com/api/event/{gw}/live/").json()
        rows = []
        for el in gw_data["elements"]:
            s = el["stats"]
            rows.append((
                el["id"], gw,
                safe_int(s.get("minutes")), safe_int(s.get("goals_scored")),
                safe_int(s.get("assists")), safe_int(s.get("yellow_cards")),
                safe_int(s.get("red_cards")), safe_int(s.get("bonus")),
                safe_int(s.get("bps")), safe_int(s.get("total_points")),
                safe_float(s.get("influence")), safe_float(s.get("creativity")),
                safe_float(s.get("threat")), safe_float(s.get("ict_index")),
                None,  # value not in live API
                None,  # team_id already in players
                SEASON
            ))
        execute_values(cur, """
            INSERT INTO fpl_player_gameweek_stats (
                fpl_id, round, minutes, goals_scored, assists, yellow_cards, red_cards,
                bonus, bps, total_points, influence, creativity, threat, ict_index, value, team_id, season
            ) VALUES %s
            ON CONFLICT (fpl_id, season, round) DO UPDATE
            SET minutes = EXCLUDED.minutes,
                goals_scored = EXCLUDED.goals_scored,
                assists = EXCLUDED.assists,
                yellow_cards = EXCLUDED.yellow_cards,
                red_cards = EXCLUDED.red_cards,
                bonus = EXCLUDED.bonus,
                bps = EXCLUDED.bps,
                total_points = EXCLUDED.total_points,
                influence = EXCLUDED.influence,
                creativity = EXCLUDED.creativity,
                threat = EXCLUDED.threat,
                ict_index = EXCLUDED.ict_index;
        """, rows)
        print(f"✅ GW{gw} inserted/updated.")

    conn.commit()
    conn.close()
    print("🎉 Current season updated.")

if __name__ == "__main__":
    update_current()
