import { useEffect, useState } from "react";
import { signInWithRedirect, signOut, getCurrentUser } from "aws-amplify/auth";
import { getAssets, triggerSync, exportToS3 } from "./api";

export default function App() {
  const [user, setUser] = useState(null);
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [exportDesignId, setExportDesignId] = useState("");
  const [syncKey, setSyncKey] = useState("");
  const [message, setMessage] = useState(null);

  // Check auth state on load
  useEffect(() => {
    getCurrentUser()
      .then(setUser)
      .catch(() => setUser(null));
  }, []);

  // Load assets when logged in
  useEffect(() => {
    if (user) fetchAssets();
  }, [user]);

  async function fetchAssets() {
    setLoading(true);
    try {
      const data = await getAssets();
      setAssets(data.assets || []);
    } catch (e) {
      showMessage("Failed to load assets", "error");
    } finally {
      setLoading(false);
    }
  }

  async function handleTriggerSync() {
    if (!syncKey) return showMessage("Enter an S3 key", "error");
    setLoading(true);
    try {
      const res = await triggerSync("canva-asset-hub-raw", syncKey);
      showMessage(`Sync triggered: ${res.job_id}`, "success");
    } catch (e) {
      showMessage("Sync failed", "error");
    } finally {
      setLoading(false);
    }
  }

  async function handleExport() {
    if (!exportDesignId) return showMessage("Enter a Canva design ID", "error");
    setLoading(true);
    try {
      const res = await exportToS3(exportDesignId);
      showMessage(`Exported to S3: ${res.s3_key}`, "success");
      fetchAssets();
    } catch (e) {
      showMessage("Export failed", "error");
    } finally {
      setLoading(false);
    }
  }

  function showMessage(text, type) {
    setMessage({ text, type });
    setTimeout(() => setMessage(null), 4000);
  }

  // Not logged in
  if (!user) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-white rounded-2xl shadow-lg p-10 text-center max-w-md w-full">
          <div className="text-4xl mb-4">🎨</div>
          <h1 className="text-2xl font-bold text-gray-900 mb-2">
            Canva Asset Hub
          </h1>
          <p className="text-gray-500 mb-8">
            Enterprise brand asset sync between AWS S3 and Canva
          </p>
          <button
            onClick={() => signInWithRedirect()}
            className="w-full bg-purple-600 text-white py-3 rounded-xl font-semibold hover:bg-purple-700 transition"
          >
            Sign in with Canva Asset Hub
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b px-6 py-4 flex justify-between items-center">
        <div className="flex items-center gap-3">
          <span className="text-2xl">🎨</span>
          <h1 className="text-xl font-bold text-gray-900">Canva Asset Hub</h1>
        </div>
        <div className="flex items-center gap-4">
          <span className="text-sm text-gray-500">{user.username}</span>
          <button
            onClick={() => signOut()}
            className="text-sm text-red-500 hover:text-red-700"
          >
            Sign out
          </button>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-6 py-8 space-y-8">

        {/* Message banner */}
        {message && (
          <div className={`rounded-xl px-4 py-3 text-sm font-medium ${
            message.type === "success"
              ? "bg-green-50 text-green-700 border border-green-200"
              : "bg-red-50 text-red-700 border border-red-200"
          }`}>
            {message.text}
          </div>
        )}

        {/* Actions */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">

          {/* S3 → Canva Sync */}
          <div className="bg-white rounded-2xl shadow-sm border p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-1">
              S3 → Canva Sync
            </h2>
            <p className="text-sm text-gray-500 mb-4">
              Manually trigger upload from S3 to Canva
            </p>
            <input
              type="text"
              placeholder="S3 key (e.g. ocean_1.jpg)"
              value={syncKey}
              onChange={(e) => setSyncKey(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 text-sm mb-3 focus:outline-none focus:ring-2 focus:ring-purple-300"
            />
            <button
              onClick={handleTriggerSync}
              disabled={loading}
              className="w-full bg-purple-600 text-white py-2 rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50 transition"
            >
              {loading ? "Processing..." : "Trigger Sync →"}
            </button>
          </div>

          {/* Canva → S3 Export */}
          <div className="bg-white rounded-2xl shadow-sm border p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-1">
              Canva → S3 Export
            </h2>
            <p className="text-sm text-gray-500 mb-4">
              Export a Canva design back to S3
            </p>
            <input
              type="text"
              placeholder="Canva Design ID (e.g. DAHD_XULf5k)"
              value={exportDesignId}
              onChange={(e) => setExportDesignId(e.target.value)}
              className="w-full border rounded-lg px-3 py-2 text-sm mb-3 focus:outline-none focus:ring-2 focus:ring-purple-300"
            />
            <button
              onClick={handleExport}
              disabled={loading}
              className="w-full bg-pink-500 text-white py-2 rounded-lg text-sm font-medium hover:bg-pink-600 disabled:opacity-50 transition"
            >
              {loading ? "Exporting..." : "Export to S3 ↓"}
            </button>
          </div>
        </div>

        {/* Asset List */}
        <div className="bg-white rounded-2xl shadow-sm border p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-lg font-semibold text-gray-900">
              Synced Assets
              <span className="ml-2 text-sm font-normal text-gray-400">
                ({assets.length} total)
              </span>
            </h2>
            <button
              onClick={fetchAssets}
              className="text-sm text-purple-600 hover:text-purple-800"
            >
              Refresh
            </button>
          </div>

          {loading && (
            <div className="text-center py-8 text-gray-400 text-sm">
              Loading assets...
            </div>
          )}

          {!loading && assets.length === 0 && (
            <div className="text-center py-8 text-gray-400 text-sm">
              No assets synced yet. Upload an image to S3 to get started.
            </div>
          )}

          {!loading && assets.length > 0 && (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left text-gray-500">
                    <th className="pb-3 font-medium">File</th>
                    <th className="pb-3 font-medium">Direction</th>
                    <th className="pb-3 font-medium">AI Tags</th>
                    <th className="pb-3 font-medium">Status</th>
                    <th className="pb-3 font-medium">Uploaded</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {assets.map((asset) => (
                    <tr key={asset.asset_id} className="hover:bg-gray-50">
                      <td className="py-3 font-medium text-gray-800">
                        {asset.file_name}
                      </td>
                      <td className="py-3">
                        <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                          asset.sync_direction === "s3_to_canva"
                            ? "bg-purple-100 text-purple-700"
                            : "bg-pink-100 text-pink-700"
                        }`}>
                          {asset.sync_direction === "s3_to_canva"
                            ? "S3 → Canva"
                            : "Canva → S3"}
                        </span>
                      </td>
                      <td className="py-3 text-gray-500">
                        {asset.ai_tags?.brand_tier
                          ? `${asset.ai_tags.brand_tier} · ${asset.ai_tags.mood}`
                          : "—"}
                      </td>
                      <td className="py-3">
                        <span className="px-2 py-1 bg-green-100 text-green-700 rounded-full text-xs font-medium">
                          {asset.status}
                        </span>
                      </td>
                      <td className="py-3 text-gray-400">
                        {new Date(asset.uploaded_at).toLocaleDateString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}