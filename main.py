"""Rime stream resolver — yt-dlp --get-url + CORS byte proxy.

Routes:
  GET /health                    → {"ok": true}
  GET /resolve?v=<videoId>       → {"url": "https://...googlevideo.com/...", "mime": "audio/mp4", "expires": ...}
  GET /stream?u=<encoded url>    → ranged audio bytes from *.googlevideo.com
"""

import asyncio
import re
import subprocess
import time
import urllib.parse
from typing import Iterable

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="rime-resolver")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

STREAM_HOSTS = re.compile(r"^.+\.googlevideo\.com$")
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")

_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-encoding",
}

# Simple in-memory cache: videoId → (url, expires_at_epoch)
_cache: dict[str, tuple[str, float]] = {}
_CACHE_HEADROOM = 600  # 10 min before actual expiry


def _clean_headers(headers, drop: Iterable[str] = ()) -> dict:
    drop_set = _HOP_HEADERS | {h.lower() for h in drop}
    return {k: v for k, v in headers.items() if k.lower() not in drop_set}


@app.get("/health")
async def health():
    return {"ok": True, "service": "rime-resolver", "cached": len(_cache)}


@app.get("/resolve")
async def resolve(v: str = ""):
    if not v or not VIDEO_ID_RE.match(v):
        raise HTTPException(400, "invalid video id")

    # Check cache
    cached = _cache.get(v)
    if cached and cached[1] - _CACHE_HEADROOM > time.time():
        return JSONResponse({"url": cached[0], "expires": cached[1], "cached": True})

    # Run yt-dlp --get-url -f bestaudio
    yt_url = f"https://www.youtube.com/watch?v={v}"
    try:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "--get-url",
            "-f", "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio",
            "--no-playlist",
            "--no-warnings",
            yt_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=25)
    except asyncio.TimeoutError:
        raise HTTPException(504, "yt-dlp timed out")
    except FileNotFoundError:
        raise HTTPException(500, "yt-dlp not installed")

    if proc.returncode != 0:
        err = stderr.decode(errors="replace").strip()
        raise HTTPException(502, f"yt-dlp failed: {err[:500]}")

    lines = stdout.decode(errors="replace").strip().splitlines()
    # Pick the first direct googlevideo URL (not HLS manifest)
    url = None
    for line in lines:
        line = line.strip()
        if "googlevideo.com/videoplayback" in line:
            url = line
            break
    if not url:
        # Take any URL returned
        url = lines[0].strip() if lines else None
    if not url:
        raise HTTPException(502, "yt-dlp returned no URL")

    # Parse expiry from the URL
    expire_match = re.search(r"expire[=/](\d{10})", url)
    expires = int(expire_match.group(1)) if expire_match else int(time.time()) + 14400

    _cache[v] = (url, expires)

    # Evict old cache entries (lazy)
    now = time.time()
    stale = [k for k, (_, exp) in _cache.items() if exp < now]
    for k in stale:
        del _cache[k]

    return JSONResponse({"url": url, "expires": expires, "cached": False})


@app.get("/stream")
async def stream_proxy(request: Request):
    raw = request.query_params.get("u", "")
    if not raw:
        raise HTTPException(400, "missing ?u=")
    try:
        parsed = urllib.parse.urlparse(raw)
    except ValueError:
        raise HTTPException(400, "invalid url")
    if parsed.scheme != "https":
        raise HTTPException(400, "https only")
    host = (parsed.hostname or "").lower()
    if not STREAM_HOSTS.match(host):
        raise HTTPException(403, f"host not allowed: {host}")

    fwd = {"user-agent": "com.google.ios.youtube/19.29.1 (iPhone16,2; U; CPU iOS 17_5_1 like Mac OS X;)"}
    rng = request.headers.get("range")
    if rng:
        fwd["Range"] = rng

    client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=120.0), follow_redirects=True)
    req = client.build_request("GET", raw, headers=fwd)
    resp = await client.send(req, stream=True)
    if resp.status_code >= 400:
        try:
            await resp.aclose()
        finally:
            await client.aclose()
        raise HTTPException(status_code=resp.status_code, detail=f"upstream {resp.status_code}")

    out = {"Accept-Ranges": "bytes", "Cache-Control": "no-store"}
    for k in ("content-length", "content-range", "content-type"):
        v = resp.headers.get(k)
        if v:
            out[k.title()] = v

    async def gen():
        try:
            async for chunk in resp.aiter_bytes(64 * 1024):
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    return StreamingResponse(gen(), status_code=resp.status_code, headers=out)
