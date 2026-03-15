import json
import os
import boto3
import urllib.request
import urllib.error
import base64
from boto3.dynamodb.conditions import Attr
from decimal import Decimal


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

dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
table = dynamodb.Table(DYNAMODB_TABLE)


def lambda_handler(event, context):
    log("INFO", "API request received", event=json.dumps(event))

    http_method = event.get("httpMethod", "")
    path = event.get("path", "")
    query_params = event.get("queryStringParameters") or {}
    body = json.loads(event.get("body") or "{}")

    # Route requests
    if http_method == "GET" and path == "/assets":
        return get_assets(query_params)

    elif http_method == "POST" and path == "/sync/trigger":
        return trigger_sync(body)

    elif http_method == "GET" and path.startswith("/sync/") and len(path.split("/")) == 3:
        job_id = path.split("/")[2]
        return get_sync_status(job_id)

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

    # Invoke the upload Lambda directly
    lambda_client = boto3.client("lambda", region_name="ap-southeast-2")
    try:
        # Build a mock EventBridge event for the upload Lambda
        mock_event = {
            "detail": {
                "bucket": {"name": s3_bucket},
                "object": {"key": s3_key, "size": 1},
            }
        }
        lambda_client.invoke(
            FunctionName="canva-asset-upload-handler",
            InvocationType="Event",  # Async invocation
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
    """GET /sync/{job_id} — check status of a sync job via DynamoDB"""
    try:
        # For manual syncs, derive the s3_key back from job_id
        # In Phase 2 full implementation this would use a jobs table
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