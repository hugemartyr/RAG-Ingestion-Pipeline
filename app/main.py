import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, Optional
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
import redis.asyncio as aioredis

# 1. Telemetry Definitions
INGEST_COUNTER = Counter(
    "ingest_messages_total", 
    "Total number of raw documents successfully validated and queued."
)

# 2. Schema Definitions
class IngestRequest(BaseModel):
    doc_id: str = Field(..., description="Unique enterprise identifier for the document")
    content: str = Field(..., description="Raw text payload to extract and chunk")
    metadata: Optional[Dict[str, str]] = Field(default=None, description="Optional payload markers")

# 3. Microservice State Lifecycle Management
class AppState:
    redis_client: Optional[aioredis.Redis] = None

state = AppState()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    state.redis_client = aioredis.from_url(redis_url, decode_responses=True)
    yield
    if state.redis_client:
        await state.redis_client.close()

app = FastAPI(
    title="RAG Ingestion Pipeline API",
    description="Asynchronous processing factory for vector chunking and ingestion.",
    version="0.1.0",
    lifespan=lifespan
)

# 4. Infrastructure Probes & Observability Exporters
@app.get("/health", tags=["Infrastructure"])
async def health_check() -> Dict[str, str]:
    if not state.redis_client:
        raise HTTPException(status_code=503, detail="Redis client not initialized")
    try:
        await state.redis_client.ping()
        return {"status": "ok", "broker": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Broker unavailable: {str(e)}")

@app.get("/metrics", tags=["Infrastructure"], response_class=PlainTextResponse)
def metrics() -> PlainTextResponse:
    """Standard Prometheus scraping target endpoint."""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# 5. Business Logic Core Gateway Endpoint
@app.post("/ingest", tags=["Ingestion Framework"], status_code=status.HTTP_202_ACCEPTED)
async def ingest_document(payload: IngestRequest) -> Dict[str, any]:
    """Validates raw text payload metadata and appends tasks directly onto a Redis Stream log."""
    if not state.redis_client:
        raise HTTPException(status_code=500, detail="Storage engine connection down")
    
    try:
        # Convert dictionary metadata to flat string format for Redis Stream layout if available
        meta_str = payload.metadata.copy() if payload.metadata else {}
        
        # Construct Stream Package Payload
        stream_payload = {
            "doc_id": payload.doc_id,
            "content": payload.content,
        }
        # Flat append metadata variables into transaction stream dict
        for k, v in meta_str.items():
            stream_payload[f"meta_{k}"] = v

        # Append to high-throughput Append-Only Log via XADD
        # Stream Key Name: "raw_documents" | '*' means Redis autogenerates unique millisecond ID
        stream_id = await state.redis_client.xadd(
            name="raw_documents",
            fields=stream_payload,
            id="*"
        )
        
        # Increment Telemetry Counters
        INGEST_COUNTER.inc()
        
        return {
            "queued": True,
            "stream_id": stream_id,
            "doc_id": payload.doc_id
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to place message onto data broker log: {str(e)}"
        )