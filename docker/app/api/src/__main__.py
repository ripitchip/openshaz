import os

import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from modules.repository import (
    OPENSOURCE_BUCKET,
    QUERY_BUCKET,
    send_extraction_task,
    send_extraction_task_async,
    send_similarity_task,
    upload_to_object_storage,
)

HOST = os.getenv("FASTAPI_HOST", "0.0.0.0")
PORT = int(os.getenv("FASTAPI_PORT", "8000"))

app = FastAPI(
    title="OpenShaz API",
    description="API for OpenShaz audio similarity tool",
    version="1.0.0",
)


@app.get("/health")
async def health_check():
    """Liveness probe."""
    return {"status": "healthy"}


@app.get("/ready")
async def readiness_check():
    """Readiness probe."""
    return {"status": "ready"}


@app.post("/add-song")
async def add_song(file: UploadFile = File(...), wait: bool = False):
    """Upload a song to opensource bucket, extract features, and store in DB.

    Args:
        file: Audio file to upload
        wait: If True, wait for extraction to complete (slow). If False, return immediately.
    """
    try:
        bucket_url = upload_to_object_storage(
            file_obj=file.file, file_name=file.filename, bucket_name=OPENSOURCE_BUCKET
        )

        if wait:
            # Synchronous: wait for worker to complete (slow)
            result = send_extraction_task(
                music_name=file.filename, bucket_url=bucket_url
            )
        else:
            # Async: fire-and-forget, return immediately (fast for batch)
            result = send_extraction_task_async(
                music_name=file.filename, bucket_url=bucket_url
            )

        return {
            "status": "success" if wait else "queued",
            "music_name": file.filename,
            "bucket_url": bucket_url,
            "result": result,
        }
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get-similar")
async def get_similar_songs(file: UploadFile = File(...), top_k: int = 5):
    """Upload query song, extract features, and find similar songs."""
    try:
        bucket_url = upload_to_object_storage(
            file_obj=file.file, file_name=file.filename, bucket_name=QUERY_BUCKET
        )

        result = send_similarity_task(
            music_name=file.filename, bucket_url=bucket_url, top_k=top_k
        )

        return {
            "status": "completed",
            "query_song": file.filename,
            "bucket_url": bucket_url,
            "top_k": top_k,
            "similar_songs": result.get("similar", []),
            "result": result,
        }
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
