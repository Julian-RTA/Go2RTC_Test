import os
import glob
import asyncio
import subprocess
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
import yaml
from pathlib import Path
import threading
from pydantic import BaseModel

GO2RTC_API = os.getenv("GO2RTC_API", "http://go2rtc:1984")
GO2RTC_RTSP = os.getenv("GO2RTC_RTSP", "rtsp://go2rtc:8554")
RECORDINGS_DIR = "/data/recordings"
CLIPS_DIR = "/data/clips"
CONFIG_FILE = Path("videoConfig.yaml")
CONFIG_LOCK = threading.Lock()

os.makedirs(RECORDINGS_DIR, exist_ok=True)
os.makedirs(CLIPS_DIR, exist_ok=True)

app = FastAPI(title="Stream Recorder & Clip Service")
# Mount clips directory so browsers can play them directly via HTTP
app.mount("/clips", StaticFiles(directory=CLIPS_DIR), name="clips")

active_recorders = {}  # stream_name -> subprocess.Popen

config_data = {}

class ConfigUpdate(BaseModel):
    key: str
    value: str

def load_config():
    """Load configuration from YAML file."""
    if not CONFIG_FILE.exists():
        return {}
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_config(data: dict[str, any]):
    """Save configuration to YAML file."""
    with CONFIG_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f)


def reload_config():
    """Reload config into memory."""
    global config_data
    with CONFIG_LOCK:
        config_data = load_config()


@app.get("/config")
def get_config():
    """Get current configuration."""
    with CONFIG_LOCK:
        return config_data


@app.put("/config")
def update_config(update: ConfigUpdate):
    """Update a config key and persist it."""
    with CONFIG_LOCK:
        config_data[update.key] = update.value
        save_config(config_data)
    return {"message": f"Config '{update.key}' updated successfully."}


@app.post("/config/reload")
def manual_reload():
    """Manually reload config from file."""
    reload_config()
    return {"message": "Configuration reloaded from file."}


def start_ffmpeg_worker(stream_name: str):
    stream_dir = os.path.join(RECORDINGS_DIR, stream_name)
    os.makedirs(stream_dir, exist_ok=True)
    
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-rtsp_transport", "tcp",
        "-i", f"{GO2RTC_RTSP}/{stream_name}",
        "-c", "copy",
        "-f", "segment",
        "-segment_time", str(config_data["clipDurationSeconds"]),
        "-segment_format", config_data["containerFormat"],
        "-reset_timestamps", "1",
        "-strftime", "1",
        f"{stream_dir}/%Y%m%d_%H%M%S.{config_data['containerFormat']}"
    ]
    return subprocess.Popen(cmd)



async def stream_reconciler_loop():
    """Background task: detects new streams in go2rtc and manages FFMPEG workers."""
    while True:
        reload_config()
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{GO2RTC_API}/api/streams", timeout=3.0)
                if res.status_code == 200:
                    streams = res.json().keys()
                    
                    # 1. Start workers for new/crashed streams
                    for name in streams:
                        proc = active_recorders.get(name)
                        if proc is None or proc.poll() is not None:
                            active_recorders[name] = start_ffmpeg_worker(name)

                    # 2. Stop workers for removed streams
                    for dead in list(active_recorders.keys()):
                        if dead not in streams:
                            active_recorders[dead].terminate()
                            del active_recorders[dead]
        except Exception:
            pass  # Suppress connection drops during go2rtc restarts
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    reload_config()
    asyncio.create_task(stream_reconciler_loop())

@app.post("/api/clip")
def extract_clip(camera: str, start_iso: str, end_iso: str):
    """
    Slices raw 60s segments into a single alarm event clip.
    start_iso / end_iso format: '2026-09-30T15:30:00Z'
    """
    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    
    stream_dir = os.path.join(RECORDINGS_DIR, camera)
    if not os.path.exists(stream_dir):
        raise HTTPException(status_code=404, detail="Camera recordings directory not found")

    # Locate relevant segment files by timestamp naming
    all_files = sorted(glob.glob(f"{stream_dir}/*.{config_data['containerFormat']}"))
    matched_files = []
    
    for f in all_files:
        base = os.path.splitext(os.path.basename(f))[0]
        try:
            file_time = datetime.strptime(base, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            # Include files within a 60s tolerance window
            if file_time <= end_dt and (file_time.timestamp() + config_data["clipDurationSeconds"]) >= start_dt.timestamp():
                matched_files.append(f)
        except ValueError:
            continue

    if not matched_files:
        raise HTTPException(status_code=404, detail="No footage found for requested time window")

    clip_id = f"{camera}_{int(start_dt.timestamp())}_{int(end_dt.timestamp())}.{config_data['containerFormat']}"
    output_path = os.path.join(CLIPS_DIR, clip_id)

    # Lossless single-file slice or multi-file stitch
    if len(matched_files) == 1:
        subprocess.run([
            "ffmpeg", "-y", "-i", matched_files[0],
            "-ss", str(max(0, int((start_dt - file_time).total_seconds()))),
            "-to", str(int((end_dt - file_time).total_seconds())),
            "-c:v", config_data["videoCodec"], 
            "-c:a", config_data["audioCodec"],
            output_path
        ], check=True)
    else:
        # Multi-segment concat
        list_file = f"/tmp/{clip_id}.txt"
        with open(list_file, "w") as lf:
            for mf in matched_files:
                lf.write(f"file '{mf}'\n")
        subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_file,
            "-c:v", config_data["videoCodec"], 
            "-c:a", config_data["audioCodec"],
            output_path
        ], check=True)

    return {"url": f"/clips/{clip_id}", "filename": clip_id}