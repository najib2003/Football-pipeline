# dashboard/app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2, os, time
from datetime import datetime

st.set_page_config(
    page_title='Football Analytics Pipeline',
    page_icon='⚽',
    layout='wide',
    initial_sidebar_state='expanded'
)

# ─── Database Connection ───────────────────────
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'football'),
        user=os.getenv('POSTGRES_USER', 'admin'),
        password=os.getenv('POSTGRES_PASSWORD', 'football123'),
        host=os.getenv('POSTGRES_HOST', 'postgres')
    )

def query(sql, ttl=300):
    @st.cache_data(ttl=ttl)
    def _query(sql):
        try:
            conn = get_connection()
            df = pd.read_sql(sql, conn)
            conn.close()
            return df
        except Exception:
            return pd.DataFrame()
    return _query(sql)

# ─── Sidebar Navigation ────────────────────────
st.sidebar.image('https://upload.wikimedia.org/wikipedia/en/thumb/b/be/UEFA_Champions_League_logo_2.svg/400px-UEFA_Champions_League_logo_2.svg.png', width=120)
st.sidebar.title('Football Analytics')
page = st.sidebar.radio('', ['📊 Overview', '🏆 Standings', '⚡ Live Events', '📈 Team Analysis'])

# ─── Page: Overview ────────────────────────────
if page == '📊 Overview':
    st.title('⚽ Football Analytics Dashboard')
    st.caption(f'Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M")}')

    col1, col2, col3, col4 = st.columns(4)
    matches_df = query('SELECT COUNT(*) as total FROM matches')
    teams_df   = query('SELECT COUNT(*) as total FROM teams')
    goals_df   = query("SELECT COALESCE(SUM(home_goals + away_goals), 0) as total FROM matches WHERE status = 'FINISHED'")
    comps_df   = query('SELECT COUNT(*) as total FROM competitions')

    col1.metric('Total Matches',   int(matches_df['total'][0]) if not matches_df.empty else 0)
    col2.metric('Teams Tracked',   int(teams_df['total'][0])   if not teams_df.empty else 0)
    col3.metric('Goals Recorded',  int(goals_df['total'][0])   if not goals_df.empty else 0)
    col4.metric('Competitions',    int(comps_df['total'][0])   if not comps_df.empty else 0)

    st.divider()

    goals_by_day = query('''
        SELECT matchday, SUM(home_goals + away_goals) as total_goals, COUNT(*) as num_matches
        FROM matches WHERE status = 'FINISHED' AND matchday IS NOT NULL
        GROUP BY matchday ORDER BY matchday
    ''')
    if not goals_by_day.empty:
        fig = px.bar(goals_by_day, x='matchday', y='total_goals',
                     title='Goals per Matchday', color='total_goals',
                     color_continuous_scale='Blues')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('No match data yet. Run the ingestion to populate data.')

# ─── Page: Standings ───────────────────────────
elif page == '🏆 Standings':
    st.title('🏆 League Standings')

    comps = query('SELECT id, name FROM competitions ORDER BY name')
    if comps.empty:
        st.info('No competitions yet. Run the ingestion first.')
    else:
        comp_choice = st.selectbox('Select Competition', comps['name'].tolist())
        comp_id = int(comps[comps['name'] == comp_choice]['id'].values[0])

        standings = query(f'''
            SELECT s.position as pos, t.name as team, s.played_games as p,
                   s.won as w, s.draw as d, s.lost as l,
                   s.goals_for as gf, s.goals_against as ga,
                   s.goal_difference as gd, s.points as pts
            FROM standings s JOIN teams t ON s.team_id = t.id
            WHERE s.competition_id = {comp_id}
            ORDER BY s.position
        ''')
        if not standings.empty:
            st.dataframe(standings.set_index('pos'), use_container_width=True, height=600)
        else:
            st.info('No standings data for this competition yet.')

# ─── Page: Live Events ─────────────────────────
elif page == '⚡ Live Events':
    st.title('⚡ Live Match Events (Kafka Stream)')

    auto_refresh = st.toggle('Auto-refresh every 5s', value=False)

    events = query('''
        SELECT event_type, team, home_team, away_team, minute, event_time
        FROM live_events ORDER BY id DESC LIMIT 50
    ''', ttl=0)  # No cache for live data

    if not events.empty:
        fig = px.pie(events, names='event_type', title='Event Types Distribution')
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(events, use_container_width=True)
    else:
        st.info('No live events yet. Start the Kafka producer to see events here.')

    if auto_refresh:
        time.sleep(5)
        st.rerun()

# ─── Page: Team Analysis ───────────────────────
elif page == '📈 Team Analysis':
    st.title('📈 Team Performance Analysis')

    stats = query('''
        SELECT t.name as team, ts.games_played, ts.wins, ts.draws,
               ts.goals_scored, ts.goals_conceded, ts.clean_sheets,
               ts.win_rate, ts.goals_per_game
        FROM team_stats_agg ts JOIN teams t ON ts.team_id = t.id
        ORDER BY ts.win_rate DESC
    ''')

    if not stats.empty:
        col1, col2 = st.columns(2)

        fig1 = px.scatter(stats, x='goals_per_game', y='win_rate',
                          size='games_played', hover_name='team',
                          title='Win Rate vs Goals per Game',
                          color='clean_sheets', color_continuous_scale='Greens')
        col1.plotly_chart(fig1, use_container_width=True)

        top10 = stats.head(10)
        fig2 = px.bar(top10, x='team', y='win_rate',
                      title='Top 10 Teams by Win Rate', color='win_rate',
                      color_continuous_scale='Blues')
        fig2.update_xaxes(tickangle=45)
        col2.plotly_chart(fig2, use_container_width=True)

        st.dataframe(stats, use_container_width=True)
    else:
        st.info('No team stats yet. Run the Spark transformation first.')
