-- Drop old tables if they exist
DROP TABLE IF EXISTS fpl_player_gameweek_stats CASCADE;
DROP TABLE IF EXISTS fpl_players CASCADE;
DROP TABLE IF EXISTS fpl_teams CASCADE;

-- Teams table (per season)
CREATE TABLE fpl_teams (
    team_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    short_name TEXT,
    season TEXT NOT NULL,
    PRIMARY KEY (team_id, season)
);

-- Players table (per season)
CREATE TABLE fpl_players (
    fpl_id INTEGER NOT NULL,
    web_name TEXT,
    first_name TEXT,
    second_name TEXT,
    position TEXT,
    team_id INTEGER,
    season TEXT NOT NULL,
    PRIMARY KEY (fpl_id, season),
    FOREIGN KEY (team_id, season) REFERENCES fpl_teams(team_id, season)
);

-- Player Gameweek Stats (per player, per season, per GW)
CREATE TABLE fpl_player_gameweek_stats (
    fpl_id INTEGER NOT NULL,
    round INTEGER NOT NULL,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    bonus INTEGER,
    bps INTEGER,
    total_points INTEGER,
    influence NUMERIC,
    creativity NUMERIC,
    threat NUMERIC,
    ict_index NUMERIC,
    value NUMERIC,
    team_id INTEGER,
    season TEXT NOT NULL,
    PRIMARY KEY (fpl_id, season, round),
    FOREIGN KEY (fpl_id, season) REFERENCES fpl_players(fpl_id, season),
    FOREIGN KEY (team_id, season) REFERENCES fpl_teams(team_id, season)
);

-- Indexes for performance
CREATE INDEX idx_stats_season ON fpl_player_gameweek_stats (season);
CREATE INDEX idx_stats_team   ON fpl_player_gameweek_stats (team_id, season);
CREATE INDEX idx_players_team ON fpl_players (team_id, season);
