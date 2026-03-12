import json
import os
import time
import boto3
import urllib.parse
import urllib.request
import urllib.error
import base64


def log(level: str, message: str, **kwargs):
    """Structured JSON logger for CloudWatch"""
    entry = {"level": level, "message": message, **kwargs}
    print(json.dumps(entry))


CANVA_ACCESS_TOKEN = os.environ.get("CANVA_ACCESS_TOKEN", "")
CANVA_API_BASE = os.environ.get("CANVA_API_BASE", "https://api.canva.com/rest/v1")

s3_client = boto3.client("s3")

MAX_SIZE_BYTES = 25 * 1024 * 1024  # 25MB
MIME_MAP = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg"}


def lambda_handler(event, context):
    log("INFO", "Lambda triggered")

    detail = event.get("detail", {})
    bucket = detail.get("bucket", {}).get("name")
    key = urllib.parse.unquote_plus(detail.get("object", {}).get("key", ""))
    object_size = detail.get("object", {}).get("size", 0)

    log("INFO", "S3 event received", bucket=bucket, key=key, size=object_size)

    # Validate required fields
    if not bucket or not key:
        log("ERROR", "Missing bucket or key")
        return {"statusCode": 400, "body": "Missing bucket or key"}

    # Skip empty files
    if object_size == 0:
        log("WARN", "Skipping empty file", key=key)
        return {"statusCode": 200, "body": "Skipped — empty file"}

    # Skip files exceeding Canva's size limit
    if object_size > MAX_SIZE_BYTES:
        log("WARN", "Skipping file too large", key=key, size=object_size)
        return {"statusCode": 200, "body": "Skipped — file too large"}

    # Validate file extension
    ext = key.split(".")[-1].lower()
    mime_type = MIME_MAP.get(ext)
    if not mime_type:
        log("WARN", "Skipping unsupported file type", key=key, ext=ext)
        return {"statusCode": 200, "body": "Skipped — unsupported file type"}

    # Download file from S3
    log("INFO", "Downloading from S3", bucket=bucket, key=key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_data = response["Body"].read()
    file_name = key.split("/")[-1]
    log("INFO", "S3 download complete", file_name=file_name, bytes=len(file_data))

    # Upload to Canva with retry (max 3 attempts)
    asset_id = None
    for attempt in range(3):
        asset_id = upload_to_canva(file_data, file_name, mime_type)
        if asset_id:
            break
        if attempt < 2:
            log("WARN", "Upload failed, retrying", attempt=attempt + 2, max=3)
            time.sleep(5)

    if asset_id:
        log("INFO", "Upload successful", asset_id=asset_id, file_name=file_name)
        return {"statusCode": 200, "body": json.dumps({"asset_id": asset_id})}
    else:
        log("ERROR", "Upload failed after 3 attempts", file_name=file_name)
        return {"statusCode": 500, "body": "Canva upload failed"}


def upload_to_canva(file_data: bytes, file_name: str, mime_type: str) -> str | None:
    # Encode file name in Base64 as required by Canva API spec
    name_b64 = base64.b64encode(file_name.encode()).decode()
    metadata = json.dumps({"name_base64": name_b64})

    upload_url = f"{CANVA_API_BASE}/asset-uploads"
    req = urllib.request.Request(
        upload_url,
        data=file_data,
        headers={
            "Authorization": f"Bearer {CANVA_ACCESS_TOKEN}",
            "Content-Type": "application/octet-stream",
            "Asset-Upload-Metadata": metadata,
        },
        method="POST",
    )

    # Step 1: Create upload job
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            job_id = result["job"]["id"]
            log("INFO", "Upload job created", job_id=job_id)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        log("ERROR", "Upload job request failed", http_code=e.code, response=body)
        if e.code == 401:
            log("ERROR", "Token expired — update CANVA_ACCESS_TOKEN env var")
        elif e.code == 403:
            log("ERROR", "Insufficient scope — check asset:write permission")
        elif e.code == 429:
            log("ERROR", "Rate limited — 30 req/min limit reached")
        return None
    except Exception as e:
        log("ERROR", "Unexpected error creating upload job", error=str(e))
        return None

    # Step 2: Poll job status until success or failure
    poll_url = f"{CANVA_API_BASE}/asset-uploads/{job_id}"
    for attempt in range(10):
        time.sleep(3)
        poll_req = urllib.request.Request(
            poll_url,
            headers={"Authorization": f"Bearer {CANVA_ACCESS_TOKEN}"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(poll_req) as resp:
                poll_result = json.loads(resp.read())
                status = poll_result["job"]["status"]
                log("INFO", "Polling job status", attempt=attempt + 1, status=status)
                if status == "success":
                    return poll_result["job"]["asset"]["id"]
                elif status == "failed":
                    error = poll_result.get("job", {}).get("error", {})
                    log("ERROR", "Canva upload job failed", error=error)
                    return None
        except Exception as e:
            log("ERROR", "Polling error", attempt=attempt + 1, error=str(e))
            return None

    log("ERROR", "Polling timed out after 10 attempts", job_id=job_id)
    return None