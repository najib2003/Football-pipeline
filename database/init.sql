CREATE TABLE IF NOT EXISTS competitions (
    id          INTEGER PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    code        VARCHAR(20),
    country     VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW()
);
 
CREATE TABLE IF NOT EXISTS teams (
    id          INTEGER PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    short_name  VARCHAR(50),
    tla         VARCHAR(10),
    crest_url   TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);
 
CREATE TABLE IF NOT EXISTS matches (
    id              INTEGER PRIMARY KEY,
    competition_id  INTEGER REFERENCES competitions(id),
    home_team_id    INTEGER REFERENCES teams(id),
    away_team_id    INTEGER REFERENCES teams(id),
    matchday        INTEGER,
    status          VARCHAR(30),
    match_date      TIMESTAMP,
    home_goals      INTEGER,
    away_goals      INTEGER,
    winner          VARCHAR(30),
    created_at      TIMESTAMP DEFAULT NOW()
);
 
CREATE TABLE IF NOT EXISTS standings (
    id              SERIAL PRIMARY KEY,
    competition_id  INTEGER REFERENCES competitions(id),
    team_id         INTEGER REFERENCES teams(id),
    season_year     INTEGER,
    position        INTEGER,
    played_games    INTEGER,
    won             INTEGER,
    draw            INTEGER,
    lost            INTEGER,
    goals_for       INTEGER,
    goals_against   INTEGER,
    goal_difference INTEGER,
    points          INTEGER,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_competition ON matches(competition_id);
CREATE INDEX IF NOT EXISTS idx_standings_competition ON standings(competition_id);
