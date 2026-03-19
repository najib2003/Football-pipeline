# processing/kafka_consumer.py
import json, os
from kafka import KafkaConsumer
import psycopg2

KAFKA_BROKER = 'kafka:9092'
TOPIC        = 'match-events'

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'football'),
        user=os.getenv('POSTGRES_USER', 'admin'),
        password=os.getenv('POSTGRES_PASSWORD', 'football123'),
        host=os.getenv('POSTGRES_HOST', 'postgres')
    )

def ensure_events_table(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS live_events (
                id          SERIAL PRIMARY KEY,
                match_id    INTEGER,
                home_team   VARCHAR(100),
                away_team   VARCHAR(100),
                event_type  VARCHAR(50),
                team        VARCHAR(100),
                minute      INTEGER,
                event_time  TIMESTAMP
            )
        ''')
        conn.commit()

def store_event(conn, event):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO live_events
                (match_id, home_team, away_team, event_type, team, minute, event_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (
            event['match_id'], event['home_team'], event['away_team'],
            event['event_type'], event['team'], event['minute'],
            event['timestamp']
        ))
        conn.commit()

def main():
    conn = get_connection()
    ensure_events_table(conn)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='football-consumers',
        auto_offset_reset='latest',
    )

    print(f'Listening on topic: {TOPIC}')
    for message in consumer:
        event = message.value
        store_event(conn, event)
        print(f"  Stored: {event['event_type']} @ {event['minute']}'")

if __name__ == '__main__':
    main()
