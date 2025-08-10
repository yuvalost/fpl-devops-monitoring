import os
import sys
import time
import json
import shutil
import zipfile
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from urllib.request import urlretrieve

DB_NAME = os.getenv("DB_NAME", "premier_league")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1q2w3e4r!")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")

SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24"]
DATA_BASE = "/tmp/FPL"
REPO_BASE_URL = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"  # :contentReference[oaicite:0]{index=0}

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

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

def fetch_csv(url, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    urlretrieve(url, dest)
    return dest

def load_teams(conn, season):
    # teams.csv columns: id,name,short_name,...
    teams_csv = fetch_csv(f"{REPO_BASE_URL}/{season}/teams.csv", f"{DATA_BASE}/{season}/teams.csv")
    tdf = pd.read_csv(teams_csv)
    tdf = tdf.rename(columns={"id":"team_id"})
    tdf["season"] = season
    rows = tdf[["team_id", "name", "short_name", "season"]].values.tolist()
    with conn.cursor() as cur:
        execute_values(cur,
            """
            INSERT INTO fpl_teams (team_id, name, short_name, season)
            VALUES %s
            ON CONFLICT (team_id, season) DO UPDATE
            SET name = EXCLUDED.name, short_name = EXCLUDED.short_name;
            """,
            rows
        )

def load_players(conn, season):
    # players_raw.csv has element id, names, team, element_type, web_name,...
    players_csv = fetch_csv(f"{REPO_BASE_URL}/{season}/players_raw.csv", f"{DATA_BASE}/{season}/players_raw.csv")
    pdf = pd.read_csv(players_csv)
    pos_map = {1:"GK", 2:"DEF", 3:"MID", 4:"FWD"}
    pdf["position"] = pdf["element_type"].map(pos_map)
    pdf["season"] = season
    rows = pdf[[
        "id", "web_name", "first_name", "second_name", "position", "team", "season"
    ]].values.tolist()
    with conn.cursor() as cur:
        execute_values(cur,
            """
            INSERT INTO fpl_players (fpl_id, web_name, first_name, second_name, position, team_id, season)
            VALUES %s
            ON CONFLICT (fpl_id, season) DO UPDATE
            SET web_name = EXCLUDED.web_name,
                first_name = EXCLUDED.first_name,
                second_name = EXCLUDED.second_name,
                position = EXCLUDED.position,
                team_id = EXCLUDED.team_id;
            """,
            rows
        )

def load_gw_stats(conn, season):
    # merged_gw.csv: one row per player x GW with rich stats
    merged_csv = fetch_csv(f"{REPO_BASE_URL}/{season}/gws/merged_gw.csv", f"{DATA_BASE}/{season}/merged_gw.csv")
    gdf = pd.read_csv(merged_csv)

    # Column normalization for robustness across seasons
    rename_map = {
        "element": "fpl_id",
        "round": "round",
        "minutes": "minutes",
        "goals_scored": "goals_scored",
        "assists": "assists",
        "yellow_cards": "yellow_cards",
        "red_cards": "red_cards",
        "bonus": "bonus",
        "bps": "bps",
        "total_points": "total_points",
        "influence": "influence",
        "creativity": "creativity",
        "threat": "threat",
        "ict_index": "ict_index",
        "value": "value",
        "team": "team_id"
    }
    # Some seasons call team "team" or "team_id"; ensure present
    if "team_id" not in gdf.columns and "team" in gdf.columns:
        gdf["team_id"] = gdf["team"]

    # Ensure all needed columns exist
    for k in list(rename_map.keys()):
        if k not in gdf.columns:
            gdf[k] = None

    gdf = gdf.rename(columns=rename_map)
    gdf["season"] = season

    use_cols = ["fpl_id","round","minutes","goals_scored","assists","yellow_cards","red_cards",
                "bonus","bps","total_points","influence","creativity","threat","ict_index","value","team_id","season"]

    gdf = gdf[use_cols].copy()
    # Casts
    int_cols = ["fpl_id","round","minutes","goals_scored","assists","yellow_cards","red_cards","bonus","bps","total_points","team_id"]
    for c in int_cols:
        gdf[c] = gdf[c].apply(safe_int)
    num_cols = ["influence","creativity","threat","ict_index","value"]
    for c in num_cols:
        gdf[c] = gdf[c].apply(safe_float)

    rows_iter = gdf.values.tolist()
    with conn.cursor() as cur:
        execute_values(cur,
            """
            INSERT INTO fpl_player_gameweek_stats (
              fpl_id, round, minutes, goals_scored, assists, yellow_cards, red_cards,
              bonus, bps, total_points, influence, creativity, threat, ict_index, value, team_id, season
            )
            VALUES %s
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
                ict_index = EXCLUDED.ict_index,
                value = EXCLUDED.value,
                team_id = EXCLUDED.team_id;
            """,
            rows_iter, page_size=5000
        )

def main():
    conn = connect()
    try:
        for season in SEASONS:
            print(f"\n=== Ingesting {season} ===")
            load_teams(conn, season)
            load_players(conn, season)
            load_gw_stats(conn, season)
            conn.commit()
            print(f"✅ {season} done.")
        print("\n🎉 All seasons 2020–2024 ingested successfully.")
    except Exception as e:
        conn.rollback()
        print("❌ Fatal error:", e)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
