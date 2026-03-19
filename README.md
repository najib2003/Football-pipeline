# Football Pipeline

An industry-grade end-to-end data pipeline for football data, built with modern data engineering tools and fully Dockerized.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python (fetch_data.py) |
| Storage | PostgreSQL 15 |
| Processing | Apache Spark 3.5 |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| Containerization | Docker / Docker Compose |

## Project Structure

```
football-pipeline/
├── ingestion/          # Fetches raw football data and loads into Postgres
├── processing/         # Spark transformation jobs
├── orchestration/      # Airflow DAGs
├── dashboard/          # Streamlit dashboard
├── database/           # SQL init scripts
└── docker-compose.yml  # All services wired together
```

## Getting Started

### Prerequisites
- Docker & Docker Compose

### Run

1. Copy the environment file and fill in your values:
   ```bash
   cp .env.example .env
   ```

2. Start all services:
   ```bash
   docker-compose up -d
   ```

3. Run the Spark transformation:
   ```bash
   docker exec spark_master /opt/spark/bin/spark-submit \
     --packages org.postgresql:postgresql:42.6.0 \
     /opt/processing/spark_transform.py
   ```

## Services

| Service | URL |
|---|---|
| Spark UI | http://localhost:8080 |
| Airflow | http://localhost:8081 |
| Streamlit | http://localhost:8501 |
| PostgreSQL | localhost:5432 |
