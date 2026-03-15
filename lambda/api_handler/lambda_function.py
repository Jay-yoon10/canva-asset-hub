import json
import os
import time
import boto3
import urllib.request
import urllib.error
import base64
from boto3.dynamodb.conditions import Attr
from decimal import Decimal
from datetime import datetime, timezone


def log(level: str, message: str, **kwargs):
    """Structured JSON logger for CloudWatch"""
    entry = {"level": level, "message": message, **kwargs}
    print(json.dumps(entry))


def decimal_to_float(obj):
    """Recursively convert Decimal to float for JSON serialisation"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    return obj


DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "canva-asset-hub-assets")
CANVA_ACCESS_TOKEN = os.environ.get("CANVA_ACCESS_TOKEN", "")
CANVA_API_BASE = os.environ.get("CANVA_API_BASE", "https://api.canva.com/rest/v1")
S3_BUCKET = os.environ.get("S3_BUCKET", "canva-asset-hub-raw")

dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
table = dynamodb.Table(DYNAMODB_TABLE)
s3_client = boto3.client("s3", region_name="ap-southeast-2")


def lambda_handler(event, context):
    log("INFO", "API request received",
        method=event.get("httpMethod"),
        path=event.get("path"))

    http_method = event.get("httpMethod", "")
    path = event.get("path", "")
    query_params = event.get("queryStringParameters") or {}
    body = json.loads(event.get("body") or "{}")

    if http_method == "GET" and path == "/assets":
        return get_assets(query_params)
    elif http_method == "POST" and path == "/sync/trigger":
        return trigger_sync(body)
    elif http_method == "GET" and path.startswith("/sync/") and len(path.split("/")) == 3:
        job_id = path.split("/")[2]
        return get_sync_status(job_id)
    elif http_method == "POST" and path == "/export/canva":
        return export_canva_to_s3(body)
    else:
        return response(404, {"error": "Route not found", "path": path})


def get_assets(query_params: dict) -> dict:
    """GET /assets — list synced assets from DynamoDB"""
    status_filter = query_params.get("status")
    limit = int(query_params.get("limit", 50))

    try:
        if status_filter:
            result = table.scan(
                FilterExpression=Attr("status").eq(status_filter),
                Limit=limit,
            )
        else:
            result = table.scan(Limit=limit)

        items = decimal_to_float(result.get("Items", []))
        log("INFO", "Assets retrieved", count=len(items))
        return response(200, {"assets": items, "count": len(items)})

    except Exception as e:
        log("ERROR", "Failed to retrieve assets", error=str(e))
        return response(500, {"error": "Failed to retrieve assets"})


def trigger_sync(body: dict) -> dict:
    """POST /sync/trigger — manually trigger S3 → Canva sync"""
    s3_bucket = body.get("s3_bucket")
    s3_key = body.get("s3_key")

    if not s3_bucket or not s3_key:
        return response(400, {"error": "s3_bucket and s3_key are required"})

    lambda_client = boto3.client("lambda", region_name="ap-southeast-2")
    try:
        mock_event = {
            "detail": {
                "bucket": {"name": s3_bucket},
                "object": {"key": s3_key, "size": 1},
            }
        }
        lambda_client.invoke(
            FunctionName="canva-asset-upload-handler",
            InvocationType="Event",
            Payload=json.dumps(mock_event),
        )
        job_id = f"manual_{s3_key.replace('/', '_').replace('.', '_')}"
        log("INFO", "Manual sync triggered", s3_bucket=s3_bucket, s3_key=s3_key)
        return response(200, {
            "job_id": job_id,
            "status": "PROCESSING",
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
        })

    except Exception as e:
        log("ERROR", "Failed to trigger sync", error=str(e))
        return response(500, {"error": "Failed to trigger sync"})


def get_sync_status(job_id: str) -> dict:
    """GET /sync/{job_id} — check status of a sync job"""
    try:
        result = table.scan(
            FilterExpression=Attr("status").eq("COMPLETE"),
            Limit=1,
        )
        items = result.get("Items", [])
        if items:
            item = decimal_to_float(items[0])
            return response(200, {
                "job_id": job_id,
                "status": item.get("status", "UNKNOWN"),
                "asset_id": item.get("asset_id"),
                "ai_tags": item.get("ai_tags", {}),
            })
        else:
            return response(200, {"job_id": job_id, "status": "PROCESSING"})

    except Exception as e:
        log("ERROR", "Failed to get sync status", error=str(e))
        return response(500, {"error": "Failed to get sync status"})


def export_canva_to_s3(body: dict) -> dict:
    """POST /export/canva — export a Canva design and save to S3"""
    design_id = body.get("design_id")

    if not design_id:
        return response(400, {"error": "design_id is required"})

    log("INFO", "Export request received", design_id=design_id)

    # Step 1: Create export job
    export_url = f"{CANVA_API_BASE}/exports"
    export_body = json.dumps({
        "design_id": design_id,
        "format": {
            "type": "png",
            "export_quality": "regular",
        },
    }).encode()

    req = urllib.request.Request(
        export_url,
        data=export_body,
        headers={
            "Authorization": f"Bearer {CANVA_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            job_id = result["job"]["id"]
            log("INFO", "Export job created", job_id=job_id)
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        log("ERROR", "Failed to create export job", http_code=e.code, response=body_text)
        return response(e.code, {"error": f"Canva export failed: {body_text}"})
    except Exception as e:
        log("ERROR", "Unexpected error creating export job", error=str(e))
        return response(500, {"error": str(e)})

    # Step 2: Poll export job until complete
    poll_url = f"{CANVA_API_BASE}/exports/{job_id}"
    download_url = None

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
                log("INFO", "Export job status", attempt=attempt + 1, status=status)

                if status == "success":
                    urls = poll_result["job"].get("urls", [])
                    if not urls:
                        log("ERROR", "No export URLs in response")
                        return response(500, {"error": "No export URLs returned"})
                    download_url = urls[0]
                    break
                elif status == "failed":
                    log("ERROR", "Export job failed")
                    return response(500, {"error": "Canva export job failed"})
        except Exception as e:
            log("ERROR", "Export polling error", attempt=attempt + 1, error=str(e))
            return response(500, {"error": str(e)})
    else:
        log("ERROR", "Export polling timed out")
        return response(500, {"error": "Export polling timed out"})

    # Step 3: Download exported file from Canva
    try:
        with urllib.request.urlopen(download_url) as dl_resp:
            file_data = dl_resp.read()
        log("INFO", "Design downloaded", bytes=len(file_data))
    except Exception as e:
        log("ERROR", "Failed to download export", error=str(e))
        return response(500, {"error": "Failed to download from Canva"})

    # Step 4: Upload to S3
    s3_key = f"canva-exports/{design_id}.png"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_data,
            ContentType="image/png",
        )
        log("INFO", "Design saved to S3", s3_key=s3_key)
    except Exception as e:
        log("ERROR", "Failed to upload to S3", error=str(e))
        return response(500, {"error": "Failed to save to S3"})

    # Step 5: Save to DynamoDB
    try:
        item = {
            "asset_id": f"canva_export_{design_id}",
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
            "s3_bucket": S3_BUCKET,
            "s3_key": s3_key,
            "file_name": f"{design_id}.png",
            "file_size": len(file_data),
            "mime_type": "image/png",
            "status": "COMPLETE",
            "sync_direction": "canva_to_s3",
            "canva_design_id": design_id,
        }
        table.put_item(Item=item)
        log("INFO", "Reverse sync saved to DynamoDB", design_id=design_id)
    except Exception as e:
        log("ERROR", "Failed to save to DynamoDB", error=str(e))

    return response(200, {
        "status": "COMPLETE",
        "design_id": design_id,
        "s3_key": s3_key,
        "s3_bucket": S3_BUCKET,
        "bytes": len(file_data),
    })


def response(status_code: int, body: dict) -> dict:
    """Helper to build API Gateway response with CORS headers"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(body),
    }