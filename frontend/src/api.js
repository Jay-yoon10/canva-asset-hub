import { fetchAuthSession } from "aws-amplify/auth";

const API_BASE = "https://56774rnqz5.execute-api.ap-southeast-2.amazonaws.com/dev";

async function getAuthHeader() {
    const session = await fetchAuthSession();
    const token = session.tokens?.idToken?.toString();
    return { Authorization: `Bearer ${token}` };
}

export async function getAssets(status = null) {
  const headers = await getAuthHeader();
  const url = status
    ? `${API_BASE}/assets?status=${status}`
    : `${API_BASE}/assets`;
  const res = await fetch(url, { headers });
  return res.json();
}

export async function triggerSync(s3Bucket, s3Key) {
  const headers = await getAuthHeader();
  const res = await fetch(`${API_BASE}/sync/trigger`, {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/json" },
    body: JSON.stringify({ s3_bucket: s3Bucket, s3_key: s3Key }),
  });
  return res.json();
}

export async function exportToS3(designId) {
  const headers = await getAuthHeader();
  const res = await fetch(`${API_BASE}/export/canva`, {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/json" },
    body: JSON.stringify({ design_id: designId }),
  });
  return res.json();
}