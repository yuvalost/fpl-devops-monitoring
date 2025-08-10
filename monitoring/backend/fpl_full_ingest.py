import os
import sys
import time
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from urllib.request import urlretrieve
import requests

DB_NAME = os.getenv("DB_NAME", "premier_league")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1q2w3e4r!")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")

SEASONS_HIST = ["2020-21", "2021-22", "2022-23", "2023-24"]
DATA_BASE = "/tmp/FPL"
REPO_BASE_URL = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"


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
            time.sleep(3)


def safe_int(v):
    try:
        return int(v)
    except Exception:
        return None


def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


def fetch_csv(url, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    urlretrieve(url, dest)
    return dest


def load_teams(conn, season):
    print(f"  • Loading teams {season}…")
    path = fetch_csv(f"{REPO_BASE_URL}/{season}/teams.csv", f"{DATA_BASE}/{season}/teams.csv")
    df = pd.read_csv(path).rename(columns={"id": "team_id"})
    before = len(df)
    df = df.drop_duplicates(subset=["team_id"])
    after = len(df)
    if after < before:
        print(f"    - Dedup teams: {before} → {after}")
    df["season"] = season
    rows = df[["team_id", "name", "short_name", "season"]].values.tolist()
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fpl_teams (team_id, name, short_name, season)
            VALUES %s
            ON CONFLICT (team_id, season) DO UPDATE
            SET name = EXCLUDED.name, short_name = EXCLUDED.short_name;
            """,
            rows,
            page_size=1000,
        )


def load_players(conn, season):
    print(f"  • Loading players {season}…")
    path = fetch_csv(f"{REPO_BASE_URL}/{season}/players_raw.csv", f"{DATA_BASE}/{season}/players_raw.csv")
    df = pd.read_csv(path)
    pos_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    df["position"] = df["element_type"].map(pos_map)
    before = len(df)
    df = df.drop_duplicates(subset=["id"])  # dedup by FPL id
    after = len(df)
    if after < before:
        print(f"    - Dedup players: {before} → {after}")
    df["season"] = season
    rows = df[["id", "web_name", "first_name", "second_name", "position", "team", "season"]].values.tolist()
    with conn.cursor() as cur:
        execute_values(
            cur,
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
            rows,
            page_size=2000,
        )


def get_team_map(conn, season: str) -> dict:
    """Return mapping fpl_id -> team_id for a given season from DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT fpl_id, team_id FROM fpl_players WHERE season = %s", (season,))
        rows = cur.fetchall()
    return {fpl_id: team_id for fpl_id, team_id in rows if team_id is not None}


def load_gw_stats(conn, season):
    print(f"  • Loading gameweeks {season}…")
    path = fetch_csv(f"{REPO_BASE_URL}/{season}/gws/merged_gw.csv", f"{DATA_BASE}/{season}/merged_gw.csv")
    gdf = pd.read_csv(path)

    # Normalize/ensure expected columns
    if "team_id" not in gdf.columns and "team" in gdf.columns:
        gdf["team_id"] = gdf["team"]  # may be short_name or id

    need = {
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
        "team_id": "team_id",
    }
    for k in need:
        if k not in gdf.columns:
            gdf[k] = None

    gdf = gdf.rename(columns=need)
    gdf["season"] = season

    # Cast numeric columns (except team_id for now)
    for c in ["fpl_id", "round", "minutes", "goals_scored", "assists", "yellow_cards", "red_cards", "bonus", "bps", "total_points"]:
        gdf[c] = gdf[c].apply(safe_int)
    for c in ["influence", "creativity", "threat", "ict_index", "value"]:
        gdf[c] = gdf[c].apply(safe_float)

    # Fix team_id:
    # 1) numeric-cast whatever the CSV has
    gdf["team_id_numeric"] = pd.to_numeric(gdf.get("team_id"), errors="coerce")

    # 2) authoritative map from players (already loaded for this season)
    team_map = get_team_map(conn, season)
    gdf["team_id_from_players"] = gdf["fpl_id"].map(team_map)

    # 3) prefer players map, else numeric cast
    gdf["team_id"] = gdf["team_id_from_players"].combine_first(gdf["team_id_numeric"]).apply(safe_int)
    gdf.drop(columns=["team_id_from_players", "team_id_numeric"], inplace=True)

    # Dedup by PK (fpl_id, season, round)
    before = len(gdf)
    gdf = gdf.drop_duplicates(subset=["fpl_id", "season", "round"], keep="last")
    after = len(gdf)
    if after < before:
        print(f"    - Dedup gw rows: {before} → {after}")

    use_cols = [
        "fpl_id",
        "round",
        "minutes",
        "goals_scored",
        "assists",
        "yellow_cards",
        "red_cards",
        "bonus",
        "bps",
        "total_points",
        "influence",
        "creativity",
        "threat",
        "ict_index",
        "value",
        "team_id",
        "season",
    ]
    rows = gdf[use_cols].values.tolist()

    with conn.cursor() as cur:
        # Note: PK order (fpl_id, season, round)
        execute_values(
            cur,
            """
            INSERT INTO fpl_player_gameweek_stats (
                fpl_id, season, round, minutes, goals_scored, assists,
                yellow_cards, red_cards, bonus, bps, total_points,
                influence, creativity, threat, ict_index, value, team_id
            )
            SELECT x.fpl_id, x.season, x.round, x.minutes, x.goals_scored, x.assists,
                   x.yellow_cards, x.red_cards, x.bonus, x.bps, x.total_points,
                   x.influence, x.creativity, x.threat, x.ict_index, x.value, x.team_id
            FROM (VALUES %s) AS x(
                fpl_id, round, minutes, goals_scored, assists, yellow_cards, red_cards,
                bonus, bps, total_points, influence, creativity, threat, ict_index, value, team_id, season
            )
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
            rows,
            page_size=5000,
        )


def ingest_historical(conn):
    for season in SEASONS_HIST:
        print(f"\n=== Ingesting {season} ===")
        load_teams(conn, season)
        conn.commit()
        load_players(conn, season)
        conn.commit()
        load_gw_stats(conn, season)
        conn.commit()
        print(f"✅ {season} done.")


def guess_current_season():
    from datetime import datetime
    now = datetime.utcnow()
    yr = now.year
    return f"{yr}-{str(yr + 1)[-2:]}" if now.month >= 7 else f"{yr - 1}-{str(yr)[-2:]}"


def update_current(conn):
    SEASON = guess_current_season()
    print(f"\n=== Updating current season {SEASON} ===")
    bs = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()

    # Teams
    t_rows = [(t["id"], t["name"], t["short_name"], SEASON) for t in bs["teams"]]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fpl_teams (team_id, name, short_name, season)
            VALUES %s
            ON CONFLICT (team_id, season) DO UPDATE
            SET name = EXCLUDED.name, short_name = EXCLUDED.short_name;
            """,
            t_rows,
            page_size=1000,
        )
    conn.commit()

    # Players
    pos_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    p_rows = [
        (p["id"], p["web_name"], p["first_name"], p["second_name"], pos_map.get(p["element_type"]), p["team"], SEASON)
        for p in bs["elements"]
    ]
    # dedup within batch by (fpl_id, season)
    seen = set()
    p_rows = [r for r in p_rows if (r[0], SEASON) not in seen and not seen.add((r[0], SEASON))]
    with conn.cursor() as cur:
        execute_values(
            cur,
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
            p_rows,
            page_size=2000,
        )
    conn.commit()

    # Finished GWs
    finished = [e for e in bs.get("events", []) if e.get("finished")]
    if not finished:
        print("⚠ No finished gameweeks yet.")
        return
    latest_gw = max(e["id"] for e in finished)
    print(f"  • Latest finished GW: {latest_gw}")

    for gw in range(1, latest_gw + 1):
        print(f"  • GW{gw}…")
        data = requests.get(f"https://fantasy.premierleague.com/api/event/{gw}/live/").json()
        rows = []
        for el in data["elements"]:
            s = el["stats"]
            rows.append(
                (
                    el["id"],
                    SEASON,
                    gw,
                    safe_int(s.get("minutes")),
                    safe_int(s.get("goals_scored")),
                    safe_int(s.get("assists")),
                    safe_int(s.get("yellow_cards")),
                    safe_int(s.get("red_cards")),
                    safe_int(s.get("bonus")),
                    safe_int(s.get("bps")),
                    safe_int(s.get("total_points")),
                    safe_float(s.get("influence")),
                    safe_float(s.get("creativity")),
                    safe_float(s.get("threat")),
                    safe_float(s.get("ict_index")),
                    None,  # value not provided by live endpoint
                    None,  # team_id not needed here; players/teams already carry it
                )
            )

        # dedup within-batch by (fpl_id, season, round)
        seen = set()
        rows = [r for r in rows if (r[0], r[1], r[2]) not in seen and not seen.add((r[0], r[1], r[2]))]

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO fpl_player_gameweek_stats (
                    fpl_id, season, round, minutes, goals_scored, assists, yellow_cards, red_cards,
                    bonus, bps, total_points, influence, creativity, threat, ict_index, value, team_id
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
                    ict_index = EXCLUDED.ict_index;
                """,
                rows,
                page_size=5000,
            )
        conn.commit()


def ingest_historical(conn):
    for season in SEASONS_HIST:
        print(f"\n=== Ingesting {season} ===")
        load_teams(conn, season)
        conn.commit()
        load_players(conn, season)
        conn.commit()
        load_gw_stats(conn, season)
        conn.commit()
        print(f"✅ {season} done.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-current", action="store_true")
    args = parser.parse_args()

    conn = connect()
    try:
        ingest_historical(conn)
        if args.include_current:
            update_current(conn)
        print("\n🎉 Ingestion complete.")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("❌ Fatal:", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
