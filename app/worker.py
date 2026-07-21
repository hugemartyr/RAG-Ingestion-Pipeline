import os
import asyncio
import logging
from prometheus_client import Counter, start_http_server
import redis.asyncio as aioredis
from app.core.chunker import chunk_document

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("worker")

# Define Prometheus Telemetry Metrics
DOCUMENTS_PROCESSED = Counter(
    "worker_processed_documents_total", 
    "Total raw documents successfully processed and chunked by the worker."
)
LOCK_CONTENTIONS = Counter(
    "worker_lock_contentions_total", 
    "Total document lock contentions encountered by the worker."
)
CHUNKS_PUBLISHED = Counter(
    "worker_chunks_published_total", 
    "Total document chunks successfully published to the output stream."
)

async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    logger.info(f"Connecting to Redis at: {redis_url}")
    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    
    # Define stream and group names
    stream_name = "raw_documents"
    output_stream_name = "chunked_documents"
    group_name = "worker_group"
    consumer_name = os.getenv("WORKER_NAME", "worker_1")
    
    # 1. Create Stream Consumer Group if it doesn't already exist
    try:
        await redis_client.xgroup_create(
            name=stream_name, 
            groupname=group_name, 
            id="0", 
            mkstream=True
        )
        logger.info(f"Created consumer group: {group_name} on stream: {stream_name}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group {group_name} already exists.")
        else:
            logger.error(f"Error creating consumer group: {str(e)}")
            await redis_client.close()
            return

    logger.info("Worker started. Listening for raw documents...")
    
    # 2. Main Event loop - Poll Stream Group
    try:
        while True:
            try:
                # Read 1 new message blocking for 1000ms
                streams = await redis_client.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_name: ">"},
                    count=1,
                    block=1000
                )
                
                if not streams:
                    continue
                
                for stream, messages in streams:
                    for message_id, fields in messages:
                        doc_id = fields.get("doc_id")
                        content = fields.get("content")
                        
                        # Reconstruct flat metadata mapping from fields
                        metadata = {}
                        for k, v in fields.items():
                            if k.startswith("meta_"):
                                metadata[k[5:]] = v
                                
                        if not doc_id or not content:
                            logger.warning(f"Invalid message format for message ID {message_id}. Skipping.")
                            await redis_client.xack(stream_name, group_name, message_id)
                            continue
                        
                        # 3. Protect processing with a distributed document lock (TTL 60s)
                        lock_key = f"lock:doc:{doc_id}"
                        acquired = await redis_client.set(lock_key, "locked", ex=60, nx=True)
                        
                        if not acquired:
                            # Lock contention encountered: Increment Counter and log
                            LOCK_CONTENTIONS.inc()
                            logger.warning(
                                f"Lock contention detected! Document ID: {doc_id} is currently being processed by another worker. Skipping."
                            )
                            # We acknowledge the message to remove it from consumer routing
                            await redis_client.xack(stream_name, group_name, message_id)
                            continue
                        
                        logger.info(f"Processing document: {doc_id}")
                        
                        try:
                            # 4. Perform sentence-boundary chunking (max size 500 chars, no overlap)
                            chunks = chunk_document(content, max_chunk_size=500, overlap=0)
                            
                            # 5. Publish generated chunks back to output Redis stream
                            for i, chunk in enumerate(chunks):
                                chunk_payload = {
                                    "doc_id": doc_id,
                                    "chunk_index": str(i),
                                    "total_chunks": str(len(chunks)),
                                    "chunk_content": chunk
                                }
                                for k, v in metadata.items():
                                    chunk_payload[f"meta_{k}"] = v
                                    
                                await redis_client.xadd(
                                    name=output_stream_name,
                                    fields=chunk_payload,
                                    id="*"
                                )
                                CHUNKS_PUBLISHED.inc()
                            
                            logger.info(f"Successfully processed {doc_id} into {len(chunks)} chunks.")
                            DOCUMENTS_PROCESSED.inc()
                            
                        except Exception as chunk_err:
                            logger.error(f"Error during chunking/publication of {doc_id}: {str(chunk_err)}")
                        finally:
                            # 6. Housekeeping: Acknowledge execution and release lock
                            await redis_client.xack(stream_name, group_name, message_id)
                            await redis_client.delete(lock_key)
                            
            except asyncio.CancelledError:
                break
            except Exception as loop_err:
                logger.error(f"Error in consumer polling loop: {str(loop_err)}")
                await asyncio.sleep(2)
    finally:
        logger.info("Awaiting connection teardown...")
        await redis_client.close()
        logger.info("Worker shut down.")

if __name__ == "__main__":
    # Start exporter server on port 8001 for metrics scraping
    logger.info("Starting Prometheus metrics server on port 8001...")
    start_http_server(8001)
    
    # Bootstrap asyncio application loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker keyboard interrupted.")
