# RAG-Ingestion-Pipeline

High-throughput, asynchronous data factory designed to stream, chunk, embed, and store knowledge bases at enterprise scale.

## Architecture Prerequisites
* Docker & Docker Compose
* Python 3.11+ (Local development)
* Poetry (Dependency Management)

## Quick Start (Docker Orchestration)
To spin up the entire application mesh (FastAPI App, Redis Task Broker, Qdrant Vector Engine) with a single infrastructure command:
```bash
docker compose up --build -d


## Ingestion Invocations (Sprint 2 Testing)
To stream payload files directly into the asynchronous log mesh:

```bash
curl -X POST "http://localhost:8000/ingest" \
     -H "Content-Type: application/json" \
     -d '{
       "doc_id": "manual-doc-101",
       "content": "Enterprise microservices require streaming queue decoupling for optimal reliability.",
       "metadata": {"source": "terminal-curl", "priority": "high"}
     }'