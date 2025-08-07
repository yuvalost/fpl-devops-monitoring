import requests
import psycopg2

# DB config
DB_NAME = "premier_league"
DB_USER = "postgres"
DB_PASSWORD = "1q2w3e4r!"  
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to DB
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# Fetch fixtures
url = "https://fantasy.premierleague.com/api/fixtures/"
res = requests.get(url)
fixtures = res.json()

# Insert fixtures
cur.execute("DELETE FROM fixtures")
for match in fixtures:
    cur.execute("""
        INSERT INTO fixtures (
            fixture_id, gameweek, date,
            home_team_id, away_team_id,
            home_score, away_score, venue
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        match['id'],
        match['event'],
        match['kickoff_time'],
        match['team_h'],
        match['team_a'],
        match['team_h_score'],
        match['team_a_score'],
        "Home Team Stadium"  # Can replace with actual if needed
    ))

conn.commit()
cur.close()
conn.close()
print("✅ Fixtures inserted successfully!")
