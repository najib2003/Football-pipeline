import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'football')}"
JDBC_PROPS = {
    'user':     os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'football123'),
    'driver':   'org.postgresql.Driver'
}
 
def create_spark_session():
    return SparkSession.builder \
        .appName('FootballAnalytics') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0') \
        .getOrCreate()
 
def read_table(spark, table):
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)
 
def write_table(df, table):
    df.write.jdbc(url=JDBC_URL, table=table, mode='overwrite', properties=JDBC_PROPS)
 
def compute_team_stats(matches):
    """Win rate, goals per game, clean sheets for each team."""
    finished = matches.filter(F.col('status') == 'FINISHED')
 
    home = finished.select(
        F.col('home_team_id').alias('team_id'),
        F.col('home_goals').alias('scored'),
        F.col('away_goals').alias('conceded'),
        (F.col('winner') == 'HOME_TEAM').cast('int').alias('win'),
        (F.col('winner') == 'DRAW').cast('int').alias('draw')
    )
 
    away = finished.select(
        F.col('away_team_id').alias('team_id'),
        F.col('away_goals').alias('scored'),
        F.col('home_goals').alias('conceded'),
        (F.col('winner') == 'AWAY_TEAM').cast('int').alias('win'),
        (F.col('winner') == 'DRAW').cast('int').alias('draw')
    )
    all_games = home.union(away)
 
    stats = all_games.groupBy('team_id').agg(
        F.count('*').alias('games_played'),
        F.sum('win').alias('wins'),
        F.sum('draw').alias('draws'),
        F.sum('scored').alias('goals_scored'),
        F.sum('conceded').alias('goals_conceded'),
        F.count(F.when(F.col('conceded') == 0, 1)).alias('clean_sheets')
    )
 
    return stats.withColumn(
        'win_rate', F.round(F.col('wins') / F.col('games_played'), 3)
    ).withColumn(
        'goals_per_game', F.round(F.col('goals_scored') / F.col('games_played'), 2)
    )
 
def compute_form_table(matches, last_n=5):
    """Last N match results as a form string (W/D/L)."""
    finished = matches.filter(F.col('status') == 'FINISHED')
 
    home = finished.select(
        F.col('home_team_id').alias('team_id'),
        F.col('match_date'),
        F.when(F.col('winner') == 'HOME_TEAM', 'W')
         .when(F.col('winner') == 'DRAW', 'D')
         .otherwise('L').alias('result')
    )
    away = finished.select(
        F.col('away_team_id').alias('team_id'),
        F.col('match_date'),
        F.when(F.col('winner') == 'AWAY_TEAM', 'W')
         .when(F.col('winner') == 'DRAW', 'D')
         .otherwise('L').alias('result')
    )
    all_results = home.union(away)
 
    window = Window.partitionBy('team_id').orderBy(F.desc('match_date')).rowsBetween(0, last_n - 1)
    return all_results.withColumn(
        'form', F.collect_list('result').over(window)
    ).groupBy('team_id').agg(
        F.first('form').alias('last_5_form')
    )
 
def main():
    spark = create_spark_session()
    print('Reading data from PostgreSQL...')
 
    matches = read_table(spark, 'matches')
    standings = read_table(spark, 'standings')
    teams = read_table(spark, 'teams')
 
    print('Computing team statistics...')
    team_stats = compute_team_stats(matches)
    write_table(team_stats, 'team_stats_agg')
 
    print('Computing form tables...')
    form = compute_form_table(matches)
    write_table(form, 'team_form')
 
    print('All transformations complete!')
    spark.stop()
 
if __name__ == '__main__':
    main()
