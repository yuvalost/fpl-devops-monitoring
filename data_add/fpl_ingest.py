import requests
import psycopg2

# === CONFIGURATION ===
DB_NAME = "premier_league"
DB_USER = "postgres"
DB_PASSWORD = "1q2w3e4r!"   
DB_HOST = "localhost"
DB_PORT = "5432"

# === CONNECT TO DATABASE ===
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
    print("❌ Failed to connect to database:", e)
    exit()

# === FETCH FPL DATA FROM API ===
url = "https://fantasy.premierleague.com/api/bootstrap-static/"
try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print("✅ FPL data fetched.")
except Exception as e:
    print("❌ Failed to fetch data from FPL API:", e)
    exit()

# === INSERT TEAMS ===
try:
    cur.execute("DELETE FROM teams")
    for team in data['teams']:
        cur.execute("""
            INSERT INTO teams (team_id, name, short_name, stadium, founded_year)
            VALUES (%s, %s, %s, %s, NULL)
        """, (team['id'], team['name'], team['short_name'], team['name'] + " Stadium"))
    print("✅ Teams inserted.")
except Exception as e:
    print("❌ Error inserting teams:", e)

# === INSERT PLAYERS ===
try:
    cur.execute("DELETE FROM players")
    for player in data['elements']:
        cur.execute("""
            INSERT INTO players (name, position, team_id, nationality, date_of_birth, fpl_price)
            VALUES (%s, %s, %s, %s, NULL, %s)
        """, (
            player['first_name'] + " " + player['second_name'],
            player['element_type'],
            player['team'],
            "Unknown",
            float(player['now_cost']) / 10
        ))
    print("✅ Players inserted.")
except Exception as e:
    print("❌ Error inserting players:", e)

# === CLEANUP ===
conn.commit()
cur.close()
conn.close()
print("✅ All done.")
