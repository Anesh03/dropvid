import os
import glob
import json
import threading
import uuid
import time
import shutil
import logging
from flask import Flask, request, jsonify, send_file, Response, stream_with_context
import yt_dlp

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Job store ──────────────────────────────────────────────────────────────────
# job_id → { status, percent, speed, eta, bytes, filepath, error, ts }
jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()

OUTPUT_DIR           = "temp_downloads"
CLEANUP_DELAY        = 300          # seconds before temp dir removed (5 min)
MAX_JOB_AGE          = 60 * 60     # purge stale job entries after 1 hour
CONCURRENT_FRAGMENTS = 16
BUFFER_SIZE          = 4 * 1024 * 1024
HTTP_CHUNK_SIZE      = 10 * 1024 * 1024

os.makedirs(OUTPUT_DIR, exist_ok=True)

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36'
)

# ── Background: purge stale jobs ───────────────────────────────────────────────
def _purge_loop():
    while True:
        time.sleep(300)
        cutoff = time.time() - MAX_JOB_AGE
        with jobs_lock:
            stale = [k for k, v in jobs.items() if v.get('ts', 0) < cutoff]
            for k in stale:
                del jobs[k]
                log.info('Purged stale job %s', k)

threading.Thread(target=_purge_loop, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════════════
#  FRONTEND
# ══════════════════════════════════════════════════════════════════════════════
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DropVid — Turbo Downloader</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Space+Grotesk:wght@600;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07080d;--surface:#0d0f17;--card:#111420;
  --border:#1c2135;--border2:#242b3d;
  --a1:#5c6fff;--a2:#8b5cf6;--a3:#06b6d4;
  --glow:rgba(92,111,255,.22);
  --ok:#10b981;--err:#ef4444;
  --text:#e8eaf0;--muted:#64748b;--sub:#94a3b8;
  --font-d:'Space Grotesk',sans-serif;
  --font-u:'Inter',sans-serif;
}
html,body{min-height:100vh;background:var(--bg);color:var(--text);font-family:var(--font-u);-webkit-font-smoothing:antialiased}
body{display:flex;flex-direction:column;align-items:center;padding:40px 16px 80px}
.orb{position:fixed;border-radius:50%;filter:blur(130px);pointer-events:none;z-index:0;opacity:.35}
.o1{width:600px;height:600px;background:#5c6fff2a;top:-220px;left:-180px}
.o2{width:500px;height:500px;background:#8b5cf61e;bottom:-180px;right:-150px}
.o3{width:300px;height:300px;background:#06b6d415;top:50%;right:-80px}
.brand{position:relative;z-index:1;margin-bottom:44px;display:flex;flex-direction:column;align-items:center;gap:10px}
.brand-logo{width:62px;height:62px;background:linear-gradient(135deg,var(--a1),var(--a2));border-radius:20px;display:flex;align-items:center;justify-content:center;font-size:28px;box-shadow:0 0 40px var(--glow),0 8px 24px rgba(0,0,0,.4)}
.brand-name{font-family:var(--font-d);font-size:36px;font-weight:700;letter-spacing:-.8px;background:linear-gradient(135deg,#fff 30%,var(--a1) 70%,var(--a2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.brand-sub{font-size:12px;color:var(--muted);letter-spacing:.4px;display:flex;align-items:center;gap:8px}
.speed-badge{background:linear-gradient(90deg,var(--a1),var(--a3));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;font-weight:700;font-size:12px}
.card{position:relative;z-index:1;background:var(--card);border:1px solid var(--border);border-radius:24px;padding:32px;width:100%;max-width:600px;box-shadow:0 32px 80px rgba(0,0,0,.6)}
.card::before{content:'';position:absolute;inset:-1px;border-radius:25px;background:linear-gradient(135deg,rgba(92,111,255,.1) 0%,transparent 45%,rgba(139,92,246,.06) 100%);pointer-events:none;z-index:-1}
.url-wrap{display:flex;gap:10px;margin-bottom:6px}
.url-in{flex:1;background:var(--surface);border:1.5px solid var(--border2);border-radius:12px;padding:14px 16px;color:var(--text);font-family:var(--font-u);font-size:14px;outline:none;transition:border-color .2s,box-shadow .2s}
.url-in::placeholder{color:var(--muted)}
.url-in:focus{border-color:var(--a1);box-shadow:0 0 0 3px var(--glow)}
.url-in.err{border-color:var(--err);box-shadow:0 0 0 3px rgba(239,68,68,.15)}
.hint{font-size:11.5px;color:var(--muted);margin-bottom:24px}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;padding:13px 22px;border:none;border-radius:12px;font-family:var(--font-u);font-size:14px;font-weight:600;cursor:pointer;outline:none;transition:all .18s;white-space:nowrap}
.btn-scan{background:linear-gradient(135deg,var(--a1),var(--a2));color:#fff;box-shadow:0 4px 20px var(--glow)}
.btn-scan:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 8px 30px var(--glow)}
.btn-scan:disabled{opacity:.5;cursor:not-allowed}
.btn-dl{background:linear-gradient(135deg,var(--ok),#059669);color:#fff;width:100%;padding:16px;font-size:15px;font-weight:700;border-radius:13px;box-shadow:0 4px 20px rgba(16,185,129,.3);letter-spacing:.2px}
.btn-dl:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 8px 30px rgba(16,185,129,.4)}
.btn-dl:disabled{opacity:.5;cursor:not-allowed;transform:none}
.div{display:flex;align-items:center;gap:12px;margin:22px 0;color:var(--muted);font-size:11px;letter-spacing:.8px;font-weight:600}
.div::before,.div::after{content:'';flex:1;height:1px;background:var(--border)}
.meta{display:flex;gap:14px;align-items:flex-start;background:var(--surface);border:1px solid var(--border);border-radius:13px;padding:14px;margin-bottom:18px}
.thumb{position:relative;flex-shrink:0;width:110px;height:67px;border-radius:9px;overflow:hidden;background:var(--border)}
.thumb img{width:100%;height:100%;object-fit:cover}
.thumb-dur{position:absolute;bottom:4px;right:4px;background:rgba(0,0,0,.82);color:#fff;font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px}
.meta-info{flex:1;min-width:0}
.meta-title{font-size:13.5px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:8px}
.badges{display:flex;flex-wrap:wrap;gap:5px}
.badge{font-size:10.5px;font-weight:600;padding:3px 9px;border-radius:20px;background:var(--border2);color:var(--sub)}
.badge.yt{background:rgba(255,0,0,.12);color:#ff6060}
.badge.speed{background:rgba(6,182,212,.12);color:#06b6d4}
.sec-label{font-size:11.5px;font-weight:700;color:var(--muted);letter-spacing:.6px;margin-bottom:10px;display:block;text-transform:uppercase}
.fmt-row{display:flex;gap:8px;margin-bottom:18px}
.fmt-btn{flex:1;padding:10px 6px;background:var(--surface);border:1.5px solid var(--border2);border-radius:9px;font-size:13px;font-weight:600;cursor:pointer;text-align:center;color:var(--sub);transition:all .18s;user-select:none}
.fmt-btn:hover{border-color:var(--a2);color:var(--a2)}
.fmt-btn.on{border-color:var(--a2);background:rgba(139,92,246,.12);color:var(--a2)}
.q-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(78px,1fr));gap:7px;margin-bottom:18px}
.q-card{background:var(--surface);border:1.5px solid var(--border2);border-radius:9px;padding:9px 4px;text-align:center;cursor:pointer;transition:all .18s;user-select:none}
.q-card:hover:not(.disabled){border-color:var(--a1)}
.q-card.on{border-color:var(--a1);background:rgba(92,111,255,.1);box-shadow:0 0 0 3px var(--glow)}
.q-card.disabled{opacity:.32;cursor:not-allowed;pointer-events:none}
.q-res{font-size:15px;font-weight:800;font-family:var(--font-d)}
.q-tag{font-size:9.5px;font-weight:600;margin-top:2px;opacity:.7}
.q-card.on .q-res,.q-card.on .q-tag{color:var(--a1);opacity:1}
.speed-row{display:flex;gap:8px;margin-bottom:20px}
.sp-btn{flex:1;padding:10px 4px;background:var(--surface);border:1.5px solid var(--border2);border-radius:9px;font-size:11.5px;font-weight:600;cursor:pointer;text-align:center;color:var(--sub);transition:all .18s;user-select:none}
.sp-btn:hover{border-color:var(--a3);color:var(--a3)}
.sp-btn.on{border-color:var(--a3);background:rgba(6,182,212,.1);color:var(--a3)}
.sp-icon{display:block;font-size:17px;margin-bottom:3px}
/* Progress */
.prog-wrap{margin-top:20px}
.prog-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.prog-lbl{font-size:12.5px;color:var(--sub)}
.prog-pct{font-size:14px;font-weight:800;color:var(--a1);font-family:var(--font-d)}
.bar-bg{height:9px;border-radius:20px;background:var(--border);overflow:hidden}
.bar-fill{height:100%;border-radius:20px;background:linear-gradient(90deg,var(--a1),var(--a3));width:0%;transition:width .4s ease}
.bar-fill.pulse::after{content:'';display:block;height:100%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.28),transparent);animation:sweep 1.4s linear infinite}
@keyframes sweep{from{transform:translateX(-100%)}to{transform:translateX(400%)}}
.prog-stats{display:flex;gap:18px;margin-top:8px;flex-wrap:wrap}
.pstat{font-size:11px;color:var(--muted)}
.pstat span{color:var(--sub);font-weight:600}
.concur-bar{display:flex;gap:3px;margin-top:10px;align-items:center;flex-wrap:wrap}
.concur-label{font-size:10px;color:var(--muted);margin-right:4px;white-space:nowrap}
.frag{width:13px;height:13px;border-radius:3px;background:var(--border2);transition:background .15s,box-shadow .15s}
.frag.active{background:var(--a3);box-shadow:0 0 5px var(--a3)}
.frag.done{background:var(--ok);box-shadow:none}
/* Status */
.status{display:flex;align-items:center;gap:9px;margin-top:16px;padding:12px 15px;border-radius:12px;font-size:12.5px;line-height:1.45}
.status.info{background:rgba(92,111,255,.08);border:1px solid rgba(92,111,255,.2);color:#8b9dff}
.status.ok{background:rgba(16,185,129,.08);border:1px solid rgba(16,185,129,.2);color:#34d399}
.status.err{background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.2);color:#f87171}
.dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.status.info .dot{background:#8b9dff;animation:pulse 1.4s infinite}
.status.ok .dot{background:#34d399}
.status.err .dot{background:#f87171}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.spin{width:16px;height:16px;border:2px solid transparent;border-top-color:currentColor;border-radius:50%;animation:_spin .65s linear infinite;flex-shrink:0}
@keyframes _spin{to{transform:rotate(360deg)}}
/* Queue */
.queue-panel{position:relative;z-index:1;margin-top:22px;width:100%;max-width:600px}
.queue-title{font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.7px;text-transform:uppercase;margin-bottom:10px;padding-left:4px}
.q-item{background:var(--card);border:1px solid var(--border);border-radius:13px;padding:14px 16px;margin-bottom:8px;display:flex;align-items:center;gap:12px;transition:border-color .2s}
.q-item.active-job{border-color:var(--a1)}
.qi-thumb{width:62px;height:40px;border-radius:6px;object-fit:cover;background:var(--border2);flex-shrink:0}
.qi-info{flex:1;min-width:0}
.qi-title{font-size:12px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:5px}
.qi-bar-bg{height:4px;border-radius:20px;background:var(--border)}
.qi-bar-fill{height:4px;border-radius:20px;background:linear-gradient(90deg,var(--a1),var(--a3));width:0%;transition:width .3s}
.qi-status{font-size:10px;color:var(--muted);margin-top:3px}
.qi-pct{font-size:12px;font-weight:700;color:var(--a1);flex-shrink:0;font-family:var(--font-d);min-width:34px;text-align:right}
.qi-cancel{background:none;border:none;color:var(--muted);cursor:pointer;font-size:17px;padding:4px 2px;line-height:1;flex-shrink:0}
.qi-cancel:hover{color:var(--err)}
.feats{display:flex;gap:22px;justify-content:center;flex-wrap:wrap;margin-top:30px;position:relative;z-index:1}
.feat{display:flex;align-items:center;gap:6px;font-size:11.5px;color:var(--muted)}
.hidden{display:none!important}
@media(max-width:500px){
  .card{padding:22px 16px}
  .url-wrap{flex-direction:column}
  .btn-scan{width:100%}
  .q-grid{grid-template-columns:repeat(4,1fr)}
  .speed-row{flex-direction:column}
}
</style>
</head>
<body>
<div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div>

<div class="brand">
  <div class="brand-logo">⚡</div>
  <div class="brand-name">DropVid</div>
  <div class="brand-sub">
    Turbo Video Downloader &nbsp;·&nbsp;
    <span class="speed-badge">16× Parallel Streams</span>
    &nbsp;·&nbsp; 360p → 8K
  </div>
</div>

<div class="card">
  <div class="url-wrap">
    <input id="urlIn" class="url-in" type="text" placeholder="Paste YouTube URL here…"
           oninput="clearErr()" onkeydown="if(event.key==='Enter')scanVideo()">
    <button class="btn btn-scan" id="scanBtn" onclick="scanVideo()">
      <span id="scanIco">🔍</span><span id="scanTxt">Scan</span>
    </button>
  </div>
  <p class="hint">Supports youtube.com &amp; youtu.be &nbsp;·&nbsp; Concurrent fragment downloading for maximum speed</p>

  <div id="metaSec" class="hidden">
    <div class="div">VIDEO FOUND</div>
    <div class="meta">
      <div class="thumb">
        <img id="thumbImg" src="" alt="">
        <span id="thumbDur" class="thumb-dur"></span>
      </div>
      <div class="meta-info">
        <div class="meta-title" id="metaTitle"></div>
        <div class="badges">
          <span class="badge yt">YouTube</span>
          <span class="badge speed" id="badgeFrags">⚡ 16 threads</span>
          <span class="badge" id="badgeViews"></span>
          <span class="badge" id="badgeUp"></span>
        </div>
      </div>
    </div>

    <span class="sec-label">Format</span>
    <div class="fmt-row">
      <div class="fmt-btn on" id="fmtMp4"  onclick="setFmt('mp4')">🎬 MP4</div>
      <div class="fmt-btn"    id="fmtMp3"  onclick="setFmt('mp3')">🎵 MP3</div>
      <div class="fmt-btn"    id="fmtWebm" onclick="setFmt('webm')">📦 WebM</div>
    </div>

    <div id="qualSec">
      <span class="sec-label">Quality — 360p to 8K</span>
      <div class="q-grid" id="qualGrid"></div>
    </div>

    <span class="sec-label">Download Mode</span>
    <div class="speed-row">
      <div class="sp-btn on" id="spTurbo"  onclick="setSpeed('turbo')"><span class="sp-icon">🚀</span>Turbo (16×)</div>
      <div class="sp-btn"    id="spFast"   onclick="setSpeed('fast')"><span class="sp-icon">⚡</span>Fast (8×)</div>
      <div class="sp-btn"    id="spNormal" onclick="setSpeed('normal')"><span class="sp-icon">🔄</span>Normal (4×)</div>
    </div>

    <button class="btn btn-dl" id="dlBtn" onclick="startDownload()">⬇&nbsp; Download Now</button>

    <div id="progSec" class="hidden prog-wrap">
      <div class="prog-hdr">
        <span class="prog-lbl" id="progLbl">Starting…</span>
        <span class="prog-pct" id="progPct">0%</span>
      </div>
      <div class="bar-bg"><div class="bar-fill" id="progFill"></div></div>
      <div class="prog-stats">
        <div class="pstat">Speed: <span id="progSpd">—</span></div>
        <div class="pstat">ETA: <span id="progEta">—</span></div>
        <div class="pstat">Size: <span id="progBytes">—</span></div>
        <div class="pstat">Threads: <span id="progThreads">—</span></div>
      </div>
      <div class="concur-bar" id="fragBar"><span class="concur-label">Fragments:</span></div>
    </div>
  </div>

  <div id="statusBox" class="status info hidden">
    <div class="dot"></div><span id="statusMsg"></span>
  </div>
</div>

<div class="queue-panel hidden" id="queuePanel">
  <div class="queue-title">Download Queue</div>
  <div id="queueList"></div>
</div>

<div class="feats">
  <div class="feat">⚡ 16× fragment threads</div>
  <div class="feat">📺 360p → 8K UHD</div>
  <div class="feat">🎵 MP3 @ 320kbps</div>
  <div class="feat">🔒 Zero storage</div>
  <div class="feat">📋 Batch queue</div>
</div>

<script>
/* ════════ STATE ════════ */
let selQuality = null, selFormat = 'mp4', selSpeed = 'turbo', fragCount = 16;
let queue = [], isProcessing = false, currentJobId = null, fragTimer = null;
const SPEED_FRAGS = { turbo:16, fast:8, normal:4 };

/* ════════ UI ════════ */
const $ = id => document.getElementById(id);
function showStatus(msg, type='info'){
  $('statusBox').className='status '+type;
  $('statusMsg').textContent=msg;
  $('statusBox').classList.remove('hidden');
}
function hideStatus(){ $('statusBox').classList.add('hidden'); }
function setScanBusy(on){
  $('scanBtn').disabled=on;
  $('scanIco').innerHTML=on?'<span class="spin"></span>':'🔍';
  $('scanTxt').textContent=on?'Scanning…':'Scan';
}
function clearErr(){ $('urlIn').classList.remove('err'); }

function setProgress(pct, lbl, spd, eta, bytes){
  const fill=$('progFill');
  fill.style.width=pct+'%';
  fill.classList.toggle('pulse', pct>0&&pct<100);
  $('progPct').textContent=Math.round(pct)+'%';
  $('progLbl').textContent=lbl;
  $('progSpd').textContent=spd||'—';
  $('progEta').textContent=eta||'—';
  $('progBytes').textContent=bytes||'—';
}

function buildFragBar(n){
  const bar=$('fragBar');
  bar.innerHTML='<span class="concur-label">Fragments:</span>';
  for(let i=0;i<n;i++){
    const d=document.createElement('div');
    d.className='frag'; d.id='frag_'+i; bar.appendChild(d);
  }
}
function animateFrags(n){
  clearInterval(fragTimer);
  fragTimer=setInterval(()=>{
    for(let i=0;i<n;i++){
      const el=$('frag_'+i);
      if(el&&!el.classList.contains('done'))
        el.classList.toggle('active',Math.random()>.42);
    }
  },170);
}
function markFragsDone(n){
  clearInterval(fragTimer);
  for(let i=0;i<n;i++){
    const el=$('frag_'+i);
    if(el){el.classList.remove('active');el.classList.add('done');}
  }
}

/* ════════ SELECTORS ════════ */
function setFmt(f){
  selFormat=f;
  ['mp4','mp3','webm'].forEach(x=>{
    $('fmt'+x[0].toUpperCase()+x.slice(1)).classList.toggle('on',x===f);
  });
  $('qualSec').classList.toggle('hidden',f==='mp3');
}
function setSpeed(s){
  selSpeed=s; fragCount=SPEED_FRAGS[s];
  $('badgeFrags').textContent=`⚡ ${fragCount} threads`;
  $('progThreads').textContent=fragCount+'×';
  ['turbo','fast','normal'].forEach(x=>{
    $('sp'+x[0].toUpperCase()+x.slice(1)).classList.toggle('on',x===s);
  });
}
function buildQualGrid(resolutions){
  const grid=$('qualGrid'); grid.innerHTML='';
  const colorMap={360:'#6b7280',480:'#8b5cf6',720:'#3b82f6',1080:'#10b981',1440:'#f59e0b',2160:'#ef4444',4320:'#ec4899'};
  const tagMap={360:'SD',480:'nHD',720:'HD',1080:'Full HD',1440:'2K QHD',2160:'4K UHD',4320:'8K UHD'};
  const all=[360,480,720,1080,1440,2160,4320];
  const avail=new Set(resolutions);
  selQuality=resolutions.length?Math.max(...resolutions):1080;
  all.forEach(res=>{
    const ok=avail.has(res);
    const d=document.createElement('div');
    d.className='q-card'+(ok?'':' disabled')+(res===selQuality&&ok?' on':'');
    d.innerHTML=`<div class="q-res" style="color:${ok?colorMap[res]:'var(--muted)'}">${res}p</div>
      <div class="q-tag" style="color:${ok?colorMap[res]:'var(--muted)'}">${tagMap[res]||''}</div>
      ${!ok?'<div style="font-size:9px;color:var(--muted);margin-top:1px">N/A</div>':''}`;
    if(ok) d.onclick=()=>{
      document.querySelectorAll('.q-card').forEach(c=>c.classList.remove('on'));
      d.classList.add('on'); selQuality=res;
    };
    grid.appendChild(d);
  });
}

/* ════════ SCAN ════════ */
async function scanVideo(){
  const url=$('urlIn').value.trim();
  if(!url){$('urlIn').classList.add('err');showStatus('Paste a YouTube URL first.','err');return;}
  setScanBusy(true);
  $('metaSec').classList.add('hidden');
  showStatus('Fetching metadata…','info');
  try{
    const r=await fetch('/api/info',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
    const d=await r.json();
    if(d.error){showStatus('Error: '+d.error,'err');return;}
    $('metaTitle').textContent=d.title;
    $('thumbImg').src=d.thumbnail||'';
    $('thumbDur').textContent=d.duration||'';
    $('badgeViews').textContent=d.views||'';
    $('badgeUp').textContent=d.uploader||'';
    buildQualGrid(d.resolutions||[]);
    $('metaSec').classList.remove('hidden');
    $('progSec').classList.add('hidden');
    hideStatus();
  }catch(e){showStatus('Network error: '+e.message,'err');}
  finally{setScanBusy(false);}
}

/* ════════ QUEUE ════════ */
function genId(){return typeof crypto.randomUUID==='function'?crypto.randomUUID():Date.now().toString(36)+Math.random().toString(36).slice(2);}

function startDownload(){
  const url=$('urlIn').value.trim();
  const title=$('metaTitle').textContent;
  const thumb=$('thumbImg').src;
  if(!url||!title){showStatus('Scan a video first.','err');return;}
  const job={jobId:genId(),url,title,thumb,quality:selQuality,format:selFormat,frags:fragCount};
  queue.push(job);
  renderQueue();
  if(!isProcessing) processNext();
}

function renderQueue(){
  const panel=$('queuePanel'), list=$('queueList');
  if(!queue.length){panel.classList.add('hidden');return;}
  panel.classList.remove('hidden');
  list.innerHTML='';
  queue.forEach((j,i)=>{
    const div=document.createElement('div');
    div.className='q-item'+(i===0&&isProcessing?' active-job':'');
    div.id='qi_'+j.jobId;
    div.innerHTML=`
      <img class="qi-thumb" src="${j.thumb}" alt="" onerror="this.style.display='none'">
      <div class="qi-info">
        <div class="qi-title">${esc(j.title)}</div>
        <div class="qi-bar-bg"><div class="qi-bar-fill" id="qib_${j.jobId}"></div></div>
        <div class="qi-status" id="qis_${j.jobId}">${i===0&&isProcessing?'Downloading…':i===0?'Queued':'Waiting…'}</div>
      </div>
      <div class="qi-pct" id="qip_${j.jobId}">0%</div>
      <button class="qi-cancel" onclick="cancelJob('${j.jobId}')">✕</button>`;
    list.appendChild(div);
  });
}
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function cancelJob(id){
  if(currentJobId===id)return;
  queue=queue.filter(j=>j.jobId!==id);
  renderQueue();
}
function updateQI(id,pct,status){
  const bar=$('qib_'+id),lbl=$('qis_'+id),pEl=$('qip_'+id);
  if(bar)bar.style.width=pct+'%';
  if(lbl)lbl.textContent=status;
  if(pEl)pEl.textContent=Math.round(pct)+'%';
}

/* ════════ CORE PROCESS LOOP ════════ */
async function processNext(){
  if(!queue.length){isProcessing=false;currentJobId=null;return;}
  isProcessing=true;
  const job=queue[0];
  currentJobId=job.jobId;
  renderQueue();

  $('progSec').classList.remove('hidden');
  setProgress(0,'Starting download on server…','','','');
  buildFragBar(job.frags);
  animateFrags(job.frags);
  $('progThreads').textContent=job.frags+'×';
  $('dlBtn').disabled=true;
  $('dlBtn').innerHTML='<span class="spin"></span>&nbsp;Downloading…';

  try{
    /* STEP 1 — kick off background download on server */
    const startRes=await fetch('/api/start',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url:job.url,quality:job.quality,format:job.format,job_id:job.jobId,frags:job.frags}),
    });
    const startData=await startRes.json();
    if(!startRes.ok||startData.error) throw new Error(startData.error||'Failed to start download');

    /* STEP 2 — poll /api/status/:id until done (no browser timeout risk) */
    await pollUntilDone(job.jobId, job.frags);

    /* STEP 3 — trigger browser file save via direct link (no blob fetch needed) */
    const a=document.createElement('a');
    a.href=`/api/file/${job.jobId}`;
    a.download='';   // server sets Content-Disposition filename
    document.body.appendChild(a);
    a.click();
    setTimeout(()=>document.body.removeChild(a),1000);

    setProgress(100,'✅ Complete! Saving to disk…','','','');
    markFragsDone(job.frags);
    updateQI(job.jobId,100,'✅ Done');
    showStatus(`✅ "${job.title}" — download complete!`,'ok');

  }catch(e){
    clearInterval(fragTimer);
    showStatus('❌ '+e.message,'err');
    updateQI(job.jobId,0,'❌ Failed');
  }finally{
    queue.shift();
    isProcessing=false; currentJobId=null;
    $('dlBtn').disabled=false;
    $('dlBtn').innerHTML='⬇&nbsp; Download Now';
    renderQueue();
    if(queue.length) setTimeout(processNext,800);
  }
}

/* Poll /api/status every 600ms until status=done|error */
async function pollUntilDone(jobId, frags){
  const MAX_WAIT=25*60*1000;   // 25 minutes absolute max
  const INTERVAL=600;
  const deadline=Date.now()+MAX_WAIT;

  while(Date.now()<deadline){
    await sleep(INTERVAL);
    const r=await fetch('/api/status/'+jobId);
    if(!r.ok) throw new Error('Status check failed');
    const d=await r.json();

    if(d.status==='downloading'){
      const pct=d.percent||0;
      setProgress(pct,'Downloading…',d.speed||'',d.eta||'',d.bytes||'');
      updateQI(jobId,pct,`Downloading at ${d.speed||'—'}`);
    } else if(d.status==='merging'){
      setProgress(95,'Merging audio + video…','','','');
      markFragsDone(frags);
      updateQI(jobId,95,'Merging…');
    } else if(d.status==='done'){
      return;   // success — proceed to file link
    } else if(d.status==='error'){
      throw new Error(d.error||'Server-side download failed');
    }
    // status==='pending' → keep polling
  }
  throw new Error('Download timed out after 25 minutes. The file may be too large or the server is slow.');
}

function sleep(ms){return new Promise(r=>setTimeout(r,ms));}
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════════
#  BACKEND
# ══════════════════════════════════════════════════════════════════════════════

def _stamp(d: dict) -> dict:
    d['ts'] = time.time()
    return d


def _fmt_bytes(b: int) -> str:
    if b >= 1_000_000_000: return f'{b/1e9:.2f} GB'
    if b >= 1_000_000:     return f'{b/1e6:.1f} MB'
    if b >= 1_000:         return f'{b/1e3:.0f} KB'
    return f'{b} B'


def _frag_opts(frags: int) -> dict:
    return {
        'concurrent_fragment_downloads': frags,
        'http_chunk_size':               HTTP_CHUNK_SIZE,
        'buffersize':                    BUFFER_SIZE,
        'retries':                       8,
        'fragment_retries':              15,
        'file_access_retries':           5,
        'extractor_retries':             5,
        'socket_timeout':                20,
        'hls_use_mpegts':                True,
        'noprogress':                    False,
    }


def _build_format_string(fmt: str, quality: int):
    if fmt == 'mp3':
        return (
            'bestaudio/best',
            [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '320'}],
            None,
        )
    if fmt == 'webm':
        return (
            f'bestvideo[height<={quality}][ext=webm]+bestaudio[ext=webm]'
            f'/bestvideo[height<={quality}]+bestaudio/best[ext=webm]/best',
            [], 'webm',
        )
    return (
        f'bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]'
        f'/bestvideo[height<={quality}]+bestaudio'
        f'/best[height<={quality}][ext=mp4]/best[height<={quality}]/best',
        [], 'mp4',
    )


def _locate_file(job_dir: str, fmt: str, merge_fmt) -> str | None:
    exts = ['mp3'] if fmt == 'mp3' else ([merge_fmt] if merge_fmt else ['mp4','webm','mkv'])
    for ext in exts:
        found = glob.glob(os.path.join(job_dir, f'*.{ext}'))
        if found:
            return max(found, key=os.path.getsize)
    all_files = [f for f in glob.glob(os.path.join(job_dir, '*')) if os.path.isfile(f)]
    return max(all_files, key=os.path.getsize) if all_files else None


def _do_download(job_id: str, url: str, quality: int, fmt: str, frags: int):
    """Runs in a background thread. Updates jobs[job_id] as it progresses."""
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    def progress_hook(d):
        with jobs_lock:
            if d['status'] == 'downloading':
                pct = 0.0
                raw = (d.get('_percent_str') or '').strip().rstrip('%')
                try:
                    pct = float(raw)
                except ValueError:
                    pass
                if pct == 0 and d.get('total_bytes'):
                    pct = 100 * (d.get('downloaded_bytes') or 0) / d['total_bytes']
                elif pct == 0 and d.get('total_bytes_estimate'):
                    pct = 100 * (d.get('downloaded_bytes') or 0) / d['total_bytes_estimate']
                jobs[job_id] = _stamp({
                    'status':  'downloading',
                    'percent': round(min(pct, 99), 1),
                    'speed':   (d.get('_speed_str') or '').strip(),
                    'eta':     (d.get('_eta_str') or '').strip(),
                    'bytes':   _fmt_bytes(d.get('downloaded_bytes') or 0),
                })
            elif d['status'] == 'finished':
                jobs[job_id] = _stamp({'status': 'merging', 'percent': 95})

    fmt_str, postprocessors, merge_fmt = _build_format_string(fmt, quality)
    ydl_opts = {
        'format':               fmt_str,
        'outtmpl':              os.path.join(job_dir, '%(title)s.%(ext)s'),
        'quiet':                True,
        'no_warnings':          True,
        'progress_hooks':       [progress_hook],
        'postprocessors':       postprocessors,
        'merge_output_format':  merge_fmt,
        'http_headers':         {'User-Agent': UA},
        **_frag_opts(frags),
    }
    if not merge_fmt:
        del ydl_opts['merge_output_format']

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        filepath = _locate_file(job_dir, fmt, merge_fmt)
        if not filepath:
            raise FileNotFoundError('Downloaded file not found after yt-dlp completed.')

        with jobs_lock:
            jobs[job_id] = _stamp({
                'status':   'done',
                'percent':  100,
                'filepath': filepath,
                'filename': os.path.basename(filepath),
                'fmt':      fmt,
            })
        log.info('Job %s done: %s', job_id, filepath)

    except Exception as e:
        log.error('Job %s failed: %s', job_id, e)
        with jobs_lock:
            jobs[job_id] = _stamp({'status': 'error', 'error': str(e)})
        shutil.rmtree(job_dir, ignore_errors=True)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route('/')
def home():
    return HTML_PAGE


@app.route('/api/info', methods=['POST'])
def get_info():
    data = request.get_json(silent=True) or {}
    url  = (data.get('url') or '').strip()
    if not url:
        return jsonify({'error': 'No URL provided'}), 400
    try:
        with yt_dlp.YoutubeDL({'quiet':True,'no_warnings':True,'skip_download':True,
                                'socket_timeout':15,'http_headers':{'User-Agent':UA}}) as ydl:
            info = ydl.extract_info(url, download=False)
        formats = info.get('formats') or []
        resolutions = sorted(
            {f.get('height') for f in formats if f.get('height') and f.get('height') >= 360},
            reverse=True,
        )
        dur = int(info.get('duration') or 0)
        m, s = divmod(dur, 60); h, m = divmod(m, 60)
        v = info.get('view_count') or 0
        return jsonify({
            'title':       info.get('title','YouTube Video'),
            'thumbnail':   info.get('thumbnail',''),
            'duration':    f'{h:02d}:{m:02d}:{s:02d}' if h else f'{m:02d}:{s:02d}',
            'views':       (f'{v/1e6:.1f}M views' if v>=1_000_000 else f'{v/1e3:.0f}K views' if v>=1000 else f'{v} views'),
            'uploader':    info.get('uploader',''),
            'resolutions': resolutions,
        })
    except Exception as e:
        log.error('Info error: %s', e)
        return jsonify({'error': str(e)}), 400


@app.route('/api/start', methods=['POST'])
def start_download():
    """Kick off a background download. Returns immediately with {job_id}."""
    data    = request.get_json(silent=True) or {}
    url     = (data.get('url') or '').strip()
    quality = int(data.get('quality') or 1080)
    fmt     = data.get('format', 'mp4')
    job_id  = data.get('job_id') or str(uuid.uuid4())
    frags   = max(1, min(int(data.get('frags') or CONCURRENT_FRAGMENTS), 32))

    if not url:
        return jsonify({'error': 'No URL provided'}), 400

    with jobs_lock:
        jobs[job_id] = _stamp({'status': 'pending', 'percent': 0})

    t = threading.Thread(target=_do_download, args=(job_id, url, quality, fmt, frags), daemon=True)
    t.start()

    return jsonify({'job_id': job_id, 'status': 'started'})


@app.route('/api/status/<job_id>')
def get_status(job_id):
    """Lightweight poll endpoint — returns current job state."""
    with jobs_lock:
        state = jobs.get(job_id)
    if not state:
        return jsonify({'status': 'not_found'}), 404
    # Don't expose filepath to client
    safe = {k: v for k, v in state.items() if k not in ('filepath', 'ts')}
    return jsonify(safe)


@app.route('/api/file/<job_id>')
def serve_file(job_id):
    """Serve the completed file. Browser triggers this after polling confirms done."""
    with jobs_lock:
        state = jobs.get(job_id)

    if not state:
        return jsonify({'error': 'Job not found'}), 404
    if state.get('status') != 'done':
        return jsonify({'error': 'File not ready yet'}), 425   # 425 Too Early
    if not state.get('filepath') or not os.path.isfile(state['filepath']):
        return jsonify({'error': 'File missing on server'}), 500

    filepath = state['filepath']
    fmt      = state.get('fmt', 'mp4')
    filename = state.get('filename', f'video.{fmt}')
    safe_name = filename.encode('ascii', 'ignore').decode('ascii').replace('"', '') or f'video.{fmt}'

    mime_map = {'mp4':'video/mp4','mp3':'audio/mpeg','webm':'video/webm','mkv':'video/x-matroska'}
    mimetype = mime_map.get(fmt, 'application/octet-stream')

    def cleanup():
        time.sleep(CLEANUP_DELAY)
        job_dir = os.path.join(OUTPUT_DIR, job_id)
        shutil.rmtree(job_dir, ignore_errors=True)
        with jobs_lock:
            jobs.pop(job_id, None)
        log.info('Cleaned up job %s', job_id)

    threading.Thread(target=cleanup, daemon=True).start()

    return send_file(
        filepath,
        as_attachment=True,
        download_name=safe_name,
        mimetype=mimetype,
        conditional=True,
    )


if __name__ == '__main__':
    log.info('DropVid v3 starting on http://0.0.0.0:5000')
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
