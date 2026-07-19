import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from app.core.chunker import chunk_document
from app.worker import main

# ==========================================
# 1. Chunker Unit Tests
# ==========================================

def test_chunker_basic_splitting():
    text = "This is a sentence. And here is another sentence! Finally, a third one?"
    chunks = chunk_document(text, max_chunk_size=50)
    # Check that sentences are grouped but not exceeding max chunk size
    # "This is a sentence. And here is another sentence!" is len 50, which fits perfectly.
    assert len(chunks) == 2
    assert chunks[0] == "This is a sentence. And here is another sentence!"
    assert chunks[1] == "Finally, a third one?"

def test_chunker_sentence_exceeding_max_size():
    text = "This is an extremely long sentence that will exceed the maximum chunk size on its own."
    chunks = chunk_document(text, max_chunk_size=20)
    assert len(chunks) == 1
    assert chunks[0] == text.strip()

def test_chunker_empty_input():
    assert chunk_document("") == []
    assert chunk_document(None) == []

def test_chunker_overlap():
    text = "Sentence one. Sentence two. Sentence three."
    # Let's chunk with max size 30 and overlap 15.
    # "Sentence one. Sentence two." (length 27)
    # Overlap allows "Sentence two." (length 13 <= 15) to overlap into the second chunk
    # Chunk 2: "Sentence two. Sentence three." (length 29)
    chunks = chunk_document(text, max_chunk_size=30, overlap=15)
    assert len(chunks) == 2
    assert chunks[0] == "Sentence one. Sentence two."
    assert chunks[1] == "Sentence two. Sentence three."


# ==========================================
# 2. Worker Operation & Lock Contention Tests
# ==========================================

@pytest.mark.asyncio
@patch("redis.asyncio.from_url")
@patch("app.worker.start_http_server")
async def test_worker_run_and_process(mock_start_http, mock_from_url):
    # Setup mock Redis client
    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis
    
    # Mock xreadgroup to return one valid message, then cancel the loop to stop it
    mock_redis.xreadgroup.side_effect = [
        [
            ("raw_documents", [
                ("12345-0", {
                    "doc_id": "test-doc-99",
                    "content": "First sentence. Second sentence.",
                    "meta_source": "unit-test"
                })
            ])
        ],
        asyncio.CancelledError()
    ]
    
    # Mock Lock acquisition success
    mock_redis.set.return_value = True
    
    # Run the worker main loop with custom mock environment variables
    with patch("os.getenv", side_effect=lambda key, default=None: "mock_worker_1" if key == "WORKER_NAME" else default):
        await main()
        
    # Assert consumer group setup
    mock_redis.xgroup_create.assert_called_once_with(
        name="raw_documents",
        groupname="worker_group",
        id="0",
        mkstream=True
    )
    
    # Assert distributed lock was verified and set
    mock_redis.set.assert_called_once_with("lock:doc:test-doc-99", "locked", ex=60, nx=True)
    
    # Assert the chunks are published correctly inside the output stream
    mock_redis.xadd.assert_called_once()
    called_args = mock_redis.xadd.call_args[1]
    assert called_args["name"] == "chunked_documents"
    assert called_args["fields"]["doc_id"] == "test-doc-99"
    assert called_args["fields"]["chunk_content"] == "First sentence. Second sentence."
    assert called_args["fields"]["chunk_index"] == "0"
    assert called_args["fields"]["meta_source"] == "unit-test"
    
    # Assert worker acknowledged stream item and cleaned lock
    mock_redis.xack.assert_called_once_with("raw_documents", "worker_group", "12345-0")
    mock_redis.delete.assert_called_once_with("lock:doc:test-doc-99")


@pytest.mark.asyncio
@patch("redis.asyncio.from_url")
@patch("app.worker.start_http_server")
async def test_worker_lock_contention(mock_start_http, mock_from_url):
    # Setup mock Redis client
    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis
    
    # Mock xreadgroup to return one valid message, then cancel the loop
    mock_redis.xreadgroup.side_effect = [
        [
            ("raw_documents", [
                ("12345-0", {
                    "doc_id": "test-doc-99",
                    "content": "First sentence. Second sentence."
                })
            ])
        ],
        asyncio.CancelledError()
    ]
    
    # Mock Lock acquisition FAILURE (lock already processing by another container)
    mock_redis.set.return_value = False
    
    # Run the worker main loop
    await main()
    
    # Verify set was called
    mock_redis.set.assert_called_once_with("lock:doc:test-doc-99", "locked", ex=60, nx=True)
    
    # Verify stream publish step skipped
    mock_redis.xadd.assert_not_called()
    
    # Verify message acknowledged to move off the raw pipeline queue
    mock_redis.xack.assert_called_once_with("raw_documents", "worker_group", "12345-0")
    # Verify lock was NOT deleted by this contention runner
    mock_redis.delete.assert_not_called()
