# DropVid — Turbo Video Downloader

Single-file Flask app · 16× parallel fragment downloads · 360p → 8K · Batch queue

## ⚡ Quick Start

```bash
pip install -r requirements.txt
sudo apt install ffmpeg -y      # Ubuntu  |  brew install ffmpeg  # macOS
python app.py
```

Open → http://localhost:5000

## Bug Fixes (v2)

- **Random download stops** — fixed by streaming the response body via `ReadableStream`
  instead of loading the entire file into memory. Downloads of any size now complete reliably.
- **Progress freezing** — SSE heartbeats every 0.35 s keep the connection alive through
  proxies and load-balancers; a `': heartbeat'` comment prevents idle timeouts.
- **Large file browser crashes** — switched from `res.blob()` (loads all bytes at once) to
  a chunked `reader.read()` loop that yields to the browser incrementally.
- **Filename corruption** — Unicode filenames are now ASCII-sanitised properly before being
  sent as `Content-Disposition`; the fallback is always `video.<ext>` instead of blank.
- **Progress % stuck at 0** — added `total_bytes_estimate` fallback in the progress hook
  so estimated totals (e.g. HLS) are used when exact size isn't known up-front.
- **Stale progress store** — background thread now purges entries older than 30 minutes.
- **Format selection bug** — highest available resolution is now auto-selected correctly
  using `max(resolutions)` instead of `resolutions[0]` (order was unreliable).
- **Download abort on large files** — browser-side `AbortSignal.timeout` set to 15 minutes
  instead of the default 2 minutes, preventing premature cancellation.
- **User-Agent bot block** — real Chrome UA header added to both `/api/info` and
  `/api/download` to reduce bot-detection failures.
- **Range requests** — `conditional=True` on `send_file` enables HTTP Range support,
  allowing browsers to resume interrupted transfers.

## Speed Tiers

| Mode   | Threads | Use case                    |
|--------|---------|-----------------------------|
| Turbo  | 16×     | Fast internet, large files  |
| Fast   | 8×      | Balanced                    |
| Normal | 4×      | Slow/metered connections    |

## Quality Range

360p · 480p · 720p HD · 1080p Full HD · 1440p 2K · 2160p 4K · 4320p 8K

(Only qualities available on the video are selectable; others shown as N/A)

## Optional: aria2c for ultra-fast downloads

```bash
sudo apt install aria2 -y   # Ubuntu
brew install aria2           # macOS
```
Then uncomment the 4 `external_downloader` lines in `_frag_opts()`.
This gives 16 connections × 16 splits = up to 256 concurrent streams.

## Production

```bash
pip install gunicorn
gunicorn -w 4 --threads 8 --timeout 900 -b 0.0.0.0:5000 app:app
```

`--timeout 900` is important — without it Gunicorn kills workers mid-download.
