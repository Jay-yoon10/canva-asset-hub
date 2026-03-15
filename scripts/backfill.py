"""
Backfill script — manually insert existing Canva assets into DynamoDB.
Run once locally with: python backfill.py
Requires: pip install boto3
"""
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
table = dynamodb.Table("canva-asset-hub-assets")

# Add the assets you uploaded during Phase 1 testing
# Format: (asset_id, s3_key, file_name, file_size, mime_type)
existing_assets = [
    ("MAHDtm9kkwM", "snowy_mountain_3.jpg", "snowy_mountain_3.jpg", 368193, "image/jpeg"),
    ("MAHDuGNfRwQ", "space2.jpg", "space2.jpg", 152717, "image/jpeg"),
    # Add more here from your CloudWatch logs
]

for asset_id, s3_key, file_name, file_size, mime_type in existing_assets:
    item = {
        "asset_id": asset_id,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "s3_bucket": "canva-asset-hub-raw",
        "s3_key": s3_key,
        "file_name": file_name,
        "file_size": file_size,
        "mime_type": mime_type,
        "status": "COMPLETE",
        "sync_direction": "s3_to_canva",
    }
    table.put_item(Item=item)
    print(f"Inserted: {asset_id} — {file_name}")

print("Backfill complete!")