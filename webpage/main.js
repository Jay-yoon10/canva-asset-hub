window.addEventListener('scroll', () => {
  let cur = 'home';
  ['home','overview','architecture','api-reference','demo'].forEach(id => {
    const el = document.getElementById(id);
    if (el && window.scrollY >= el.offsetTop - 80) cur = id;
  });
  document.querySelectorAll('.nav-links a').forEach(a => {
    a.classList.toggle('active', a.getAttribute('href') === '#' + cur);
  });
});

function setPhase(p) {
  const fp1 = document.getElementById('fp1');
  const fp2 = document.getElementById('fp2');
  const dc1 = document.getElementById('dc-p1');
  const dc2 = document.getElementById('dc-p2');

  if (fp1) fp1.classList.toggle('active', p === 'p1');
  if (fp2) fp2.classList.toggle('active', p === 'p2');
  if (dc1) dc1.style.display = p === 'p1' ? 'grid' : 'none';
  if (dc2) dc2.style.display = 'none';

  document.querySelectorAll('.arch-tab').forEach((b, i) =>
    b.classList.toggle('active', (i === 0 && p === 'p1') || (i === 1 && p === 'p2'))
  );
}

function toggleEp(el) { el.classList.toggle('open'); }

function runDemo() {
  const btn = document.getElementById('run-btn');
  const log = document.getElementById('demo-log');
  const banner = document.getElementById('success-banner');
  const bucket = document.getElementById('db').value || 'acme-brand-assets';
  const key    = document.getElementById('dk').value || 'brands/summer/hero.png';
  const ai     = document.getElementById('dai').value === '1';
  const jobId  = 'sync_' + Math.random().toString(36).slice(2,10);
  const cjId   = 'CUJ_'  + Math.random().toString(36).slice(2,8).toUpperCase();
  const assetId= 'BAB'   + Math.random().toString(36).slice(2,8).toUpperCase();

  btn.disabled = true; btn.textContent = '⟳ Running...';
  banner.classList.remove('show'); log.innerHTML = '';

  const t = () => new Date().toTimeString().slice(0,8);
  const steps = [
    [0,    'l-info', `[EventBridge] S3 PutObject received — s3://${bucket}/${key}`],
    [300,  'l-info', `[EventBridge] Rule matched → invoking Lambda (upload-handler)`],
    [700,  'l-ok',   `[Lambda] Object retrieved from S3 ✓`],
    ...(ai ? [
      [1100, 'l-info', `[Bedrock] Invoking Claude Model for asset analysis`],
      [2200, 'l-ok',   `[Bedrock] → brand_tier: premium · campaign_type: seasonal · approved_for: social_media, web`],
    ] : []),
    [ai?2600:1100, 'l-info', `[Canva API] POST /v1/asset-uploads → job: ${cjId}`],
    [ai?3000:1500, 'l-info', `[Canva API] Poll attempt 1 → in_progress`],
    [ai?3600:2100, 'l-info', `[Canva API] Poll attempt 2 → in_progress`],
    [ai?4300:2800, 'l-ok',   `[Canva API] Upload complete → asset_id: ${assetId} ✓`],
    [ai?4600:3100, 'l-ok',   `[DynamoDB] Sync record written → ${jobId}`],
    [ai?4900:3400, 'l-ok',   `[CloudWatch] SyncSuccess=1 · Duration=${ai?4900:3400}ms`],
    [ai?5100:3600, 'l-ok',   `✓ Done — ${assetId} is live in Canva Brand Kit 🎉`],
  ];

  steps.forEach(([delay, cls, msg]) => {
    setTimeout(() => {
      const div = document.createElement('div');
      div.className = 'log-line';
      div.innerHTML = `<span class="log-t">${t()}</span><span class="${cls}">${msg}</span>`;
      log.appendChild(div); log.scrollTop = log.scrollHeight;
    }, delay);
  });

  const last = steps[steps.length-1][0] + 400;
  setTimeout(() => {
    btn.disabled = false; btn.textContent = '▶ Run Pipeline';
    document.getElementById('success-text').textContent = `Sync complete — ${assetId} live in Canva`;
    banner.classList.add('show');
  }, last);
}
