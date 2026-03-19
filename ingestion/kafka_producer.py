# ingestion/kafka_producer.py
# Simulates real-time match events (goals, cards, etc.)
import json, time, random
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'kafka:9092'
TOPIC        = 'match-events'

TEAMS = ['Arsenal', 'Barcelona', 'Bayern Munich', 'PSG', 'Real Madrid',
         'Manchester City', 'Inter Milan', 'Borussia Dortmund']

EVENT_TYPES = ['GOAL', 'YELLOW_CARD', 'RED_CARD', 'SUBSTITUTION', 'VAR_REVIEW']

def generate_event(match_id):
    """Simulate a football match event."""
    home = random.choice(TEAMS)
    away = random.choice([t for t in TEAMS if t != home])
    return {
        'match_id':   match_id,
        'home_team':  home,
        'away_team':  away,
        'event_type': random.choice(EVENT_TYPES),
        'team':       random.choice([home, away]),
        'minute':     random.randint(1, 90),
        'timestamp':  datetime.utcnow().isoformat(),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',           # Wait for all replicas
        retries=3,
    )

    print(f'Producing events to topic: {TOPIC}')
    match_id = 1001

    while True:
        event = generate_event(match_id)
        producer.send(TOPIC, value=event)
        print(f"  Event: {event['event_type']} by {event['team']} at {event['minute']}'")
        time.sleep(random.uniform(2, 8))  # Events every 2-8 seconds

if __name__ == '__main__':
    main()
