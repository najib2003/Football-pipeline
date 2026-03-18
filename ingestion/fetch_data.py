import os, time, logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
 
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
 
# ─── Configuration ────────────────────────────
API_KEY  = os.getenv('FOOTBALL_API_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS  = {'X-Auth-Token': API_KEY}
 
DB_CONFIG = {
    'dbname':   os.getenv('POSTGRES_DB'),
    'user':     os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host':     os.getenv('POSTGRES_HOST', 'postgres'),
    'port':     os.getenv('POSTGRES_PORT', 5432)
}
 
# ─── Competitions to track ─────────────────────
COMPETITIONS = {
    'CL':  'Champions League',
    'PL':  'Premier League',
    'PD':  'La Liga',
    'BL1': 'Bundesliga',
    'SA':  'Serie A',
}
 
def get_connection():
    """Create a database connection with retry logic."""
    for attempt in range(10):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError:
            log.warning(f'DB not ready, attempt {attempt+1}/10 ...')
            time.sleep(5)
    raise RuntimeError('Cannot connect to PostgreSQL after 10 attempts')
 
def fetch(endpoint, params=None):
    """Generic API fetch with rate-limit handling."""
    url = f'{BASE_URL}/{endpoint}'
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 429:
        log.warning('Rate limited — sleeping 65s')
        time.sleep(65)
        return fetch(endpoint, params)
    resp.raise_for_status()
    return resp.json()
 
def upsert_competition(cur, comp_id, name, code, country):
    cur.execute('''
        INSERT INTO competitions (id, name, code, country)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
            SET name=EXCLUDED.name, country=EXCLUDED.country
    ''', (comp_id, name, code, country))
 
def upsert_team(cur, team):
    cur.execute('''
        INSERT INTO teams (id, name, short_name, tla, crest_url)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
            SET name=EXCLUDED.name, tla=EXCLUDED.tla
    ''', (team['id'], team['name'], team.get('shortName'),
          team.get('tla'), team.get('crest')))
 
def insert_matches(cur, matches, comp_id):
    rows = []
    for m in matches:
        home = m['homeTeam']
        away = m['awayTeam']
        score = m.get('score', {}).get('fullTime', {})
        rows.append((
            m['id'], comp_id,
            home['id'], away['id'],
            m.get('matchday'), m.get('status'),
            m.get('utcDate'),
            score.get('home'), score.get('away'),
            m.get('score', {}).get('winner')
        ))
    execute_values(cur, '''
        INSERT INTO matches
            (id, competition_id, home_team_id, away_team_id,
             matchday, status, match_date, home_goals, away_goals, winner)
        VALUES %s
        ON CONFLICT (id) DO UPDATE
            SET status=EXCLUDED.status,
                home_goals=EXCLUDED.home_goals,
                away_goals=EXCLUDED.away_goals,
                winner=EXCLUDED.winner
    ''', rows)
 
def sync_competition(conn, code, name):
    log.info(f'Syncing {name} ({code})...')
    data = fetch(f'competitions/{code}')
    comp_id = data['id']
 
    with conn.cursor() as cur:
        upsert_competition(cur, comp_id, name, code, data.get('area', {}).get('name'))
        conn.commit()
 
    # Fetch teams
    teams_data = fetch(f'competitions/{code}/teams')
    with conn.cursor() as cur:
        for team in teams_data.get('teams', []):
            upsert_team(cur, team)
        conn.commit()
    log.info(f'  → {len(teams_data["teams"])} teams stored')
 
    # Fetch matches
    matches_data = fetch(f'competitions/{code}/matches')
    with conn.cursor() as cur:
        insert_matches(cur, matches_data.get('matches', []), comp_id)
        conn.commit()
    log.info(f'  → {len(matches_data["matches"])} matches stored')
 
    # Fetch standings
    try:
        standings_data = fetch(f'competitions/{code}/standings')
        _store_standings(conn, standings_data, comp_id)
    except Exception as e:
        log.warning(f'Standings not available: {e}')
 
    time.sleep(10)  # Respect rate limits between competitions
 
def _store_standings(conn, data, comp_id):
    for stage in data.get('standings', []):
        if stage.get('type') == 'TOTAL':
            season_year = data.get('season', {}).get('startDate', '2024')[:4]
            rows = []
            for entry in stage.get('table', []):
                rows.append((
                    comp_id, entry['team']['id'], int(season_year),
                    entry['position'], entry['playedGames'],
                    entry['won'], entry['draw'], entry['lost'],
                    entry['goalsFor'], entry['goalsAgainst'],
                    entry['goalDifference'], entry['points']
                ))
            with conn.cursor() as cur:
                execute_values(cur, '''
                    INSERT INTO standings
                        (competition_id, team_id, season_year, position,
                         played_games, won, draw, lost,
                         goals_for, goals_against, goal_difference, points)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                ''', rows)
                conn.commit()
            log.info(f'  → {len(rows)} standings rows stored')
 
def main():
    log.info('Football Data Pipeline starting...')
    conn = get_connection()
    log.info('Connected to PostgreSQL')
 
    for code, name in COMPETITIONS.items():
        sync_competition(conn, code, name)
 
    conn.close()
    log.info('Sync complete!')
 
if __name__ == '__main__':
    main()
