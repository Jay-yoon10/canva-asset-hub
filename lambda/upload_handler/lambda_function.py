import json
import os
import time
import boto3
import urllib.parse
import urllib.request
import urllib.error
import base64
from datetime import datetime, timezone
from decimal import Decimal


def log(level: str, message: str, **kwargs):
    """Structured JSON logger for CloudWatch"""
    entry = {"level": level, "message": message, **kwargs}
    print(json.dumps(entry))


def convert_floats(obj):
    """Recursively convert float values to Decimal for DynamoDB compatibility"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    return obj


CANVA_ACCESS_TOKEN = os.environ.get("CANVA_ACCESS_TOKEN", "")
CANVA_API_BASE = os.environ.get("CANVA_API_BASE", "https://api.canva.com/rest/v1")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "canva-asset-hub-assets")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-haiku-4-5")
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", "ap-southeast-2")

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
table = dynamodb.Table(DYNAMODB_TABLE)
bedrock_client = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

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

    # Skip files exceeding size limit
    if object_size > MAX_SIZE_BYTES:
        log("WARN", "Skipping file too large", key=key, size=object_size)
        return {"statusCode": 200, "body": "Skipped — file too large"}

    # Validate file extension
    # Detect actual mime type from file header (magic bytes)
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

    # Override mime type based on actual file content (magic bytes)
    if file_data[:3] == b'\xff\xd8\xff':
        mime_type = "image/jpeg"
    elif file_data[:8] == b'\x89PNG\r\n\x1a\n':
        mime_type = "image/png"

    log("INFO", "S3 download complete", file_name=file_name, bytes=len(file_data), detected_mime=mime_type)

    # Generate AI tags via Bedrock (non-blocking — failure won't stop upload)
    ai_tags = generate_ai_tags(file_data, mime_type, file_name)

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
        save_to_dynamodb(
            asset_id=asset_id,
            s3_bucket=bucket,
            s3_key=key,
            file_name=file_name,
            file_size=object_size,
            mime_type=mime_type,
            ai_tags=ai_tags,
        )
        return {"statusCode": 200, "body": json.dumps({"asset_id": asset_id})}
    else:
        log("ERROR", "Upload failed after 3 attempts", file_name=file_name)
        return {"statusCode": 500, "body": "Canva upload failed"}


def generate_ai_tags(file_data: bytes, mime_type: str, file_name: str) -> dict:
    """
    Use Claude Haiku 4.5 via Bedrock to generate business-context tags.
    Returns a dict of tags, or empty dict if tagging fails.
    """
    log("INFO", "Generating AI tags via Bedrock", file_name=file_name)

    image_b64 = base64.b64encode(file_data).decode()

    prompt = """You are a brand asset classifier for an enterprise marketing team.
    Analyse this image and return ONLY a valid JSON object. No explanation, no markdown, no extra text.

    Classification criteria:

    brand_tier:
    - "premium": Professional studio lighting, high resolution, polished composition, luxury or aspirational feel
    - "standard": Decent quality stock photo, acceptable for general marketing use
    - "budget": Low resolution, amateur composition, poor lighting, or heavily filtered

    content_type:
    - "product": Physical product is the main subject
    - "lifestyle": People or scenarios showing product/brand in real life context
    - "abstract": Geometric, artistic, or non-representational imagery
    - "nature": Landscapes, animals, natural environments with no people
    - "people": Portraits or people as main subject without lifestyle context
    - "other": Does not fit any above category

    campaign_type:
    - "seasonal": Strongly tied to a specific season or holiday (snow, beach, Christmas etc.)
    - "evergreen": Timeless, usable year-round regardless of season
    - "promotional": Suggests sale, discount, or urgency
    - "brand_awareness": Conveys brand identity, values, or lifestyle
    - "unknown": Cannot determine intended campaign use

    approved_for (select ALL that apply):
    - "social_media": Visually engaging, works at small sizes
    - "web": Good resolution for web display
    - "print": High enough resolution and quality for print
    - "email": Clean, not too busy, works in email layout

    mood:
    - "energetic": Dynamic, high contrast, action-oriented
    - "calm": Peaceful, low contrast, soft tones
    - "professional": Clean, corporate, formal
    - "playful": Fun, bright colours, lighthearted
    - "inspirational": Uplifting, aspirational, emotionally evocative

    Return this exact JSON structure:
    {
    "brand_tier": "premium" | "standard" | "budget",
    "content_type": "product" | "lifestyle" | "abstract" | "nature" | "people" | "other",
    "campaign_type": "seasonal" | "evergreen" | "promotional" | "brand_awareness" | "unknown",
    "approved_for": ["social_media", "web", "print", "email"],
    "mood": "energetic" | "calm" | "professional" | "playful" | "inspirational",
    "dominant_colors": ["color1", "color2"],
    "confidence": 0.0-1.0
    }"""

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 300,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime_type,
                            "data": image_b64,
                        },
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
    }

    try:
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json",
        )
        result = json.loads(response["body"].read())
        raw_text = result["content"][0]["text"].strip()

        # Strip markdown code fences if present
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]

        ai_tags = json.loads(raw_text)
        ai_tags = convert_floats(ai_tags)  # Convert floats to Decimal for DynamoDB
        log("INFO", "AI tags generated", file_name=file_name, tags=str(ai_tags))
        return ai_tags

    except Exception as e:
        # Non-blocking — log error but continue with upload
        log("WARN", "AI tagging failed, continuing without tags", error=str(e))
        return {}


def save_to_dynamodb(
    asset_id: str,
    s3_bucket: str,
    s3_key: str,
    file_name: str,
    file_size: int,
    mime_type: str,
    ai_tags: dict,
) -> None:
    """Save asset metadata and AI tags to DynamoDB"""
    uploaded_at = datetime.now(timezone.utc).isoformat()
    item = {
        "asset_id": asset_id,
        "uploaded_at": uploaded_at,
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "file_name": file_name,
        "file_size": file_size,
        "mime_type": mime_type,
        "status": "COMPLETE",
        "sync_direction": "s3_to_canva",
        "ai_tags": ai_tags,
    }
    try:
        table.put_item(Item=item)
        log("INFO", "Saved to DynamoDB", asset_id=asset_id, s3_key=s3_key, has_ai_tags=bool(ai_tags))
    except Exception as e:
        log("ERROR", "Failed to save to DynamoDB", asset_id=asset_id, error=str(e))


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