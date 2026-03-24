# Docker Simulation Layer

This layer simulates enterprise data sources.

## Components
- API service (FastAPI/Flask)
- Event generator (Python)
- docker-compose orchestration

## Responsibilities
- Generate batch datasets (CSV)
- Simulate APIs (orders, marketing)
- Produce real-time-like JSON event streams

## Goal
To mimic real-world ingestion sources without using external systems like Kafka.