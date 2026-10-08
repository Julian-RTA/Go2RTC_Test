import os
import glob
import asyncio
import subprocess
from datetime import datetime, timezone, timedelta
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
import yaml
from pathlib import Path
import threading
from pydantic import BaseModel
import time

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
def update_config(key: str, value: str):
    """Update a config key and persist it."""
    with CONFIG_LOCK:
        #probably not ideal but google said it was the most reliable
        #converts int values to ints, leaves strings as strings
        try:
            config_data[key] = int(value)
        except ValueError:
            config_data[key] = value
        save_config(config_data)
    return {"message": f"Config '{key}' updated successfully."}


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
        "-hwaccel", "cuda",
        "-i", f"{GO2RTC_RTSP}/{stream_name}",
        # "-c", "copy", 
        "-vf", "fps=20",
        "-c:v", config_data["videoCodec"], 
        "-crf", "35",
        "-preset", "fast",
        "-c:a", config_data["audioCodec"],
        # fixes the metadata issue, however the -c copy option makes it unplayable
        # if we can guarantee this runs on a powerful cpu or get gpu encoding, we can
        # switch this to the options of the clip recorder to save storage and have the clips use -c copy for speed 
        "-movflags", "empty_moov+omit_tfhd_offset+frag_keyframe", 
        "-f", "segment",
        "-segment_time", str(config_data["clipDurationSeconds"]),
        "-segment_format", config_data["containerFormat"],
        "-segment_wrap", str(config_data["maximumAmountOfBufferClips"]),
        "-reset_timestamps", "1",
        f"{stream_dir}/temp_vid%d.{config_data['containerFormat']}"
    ]
    return subprocess.Popen(cmd)



async def stream_reconciler_loop():
    """Background task: detects new streams in go2rtc and manages FFMPEG workers."""
    while True:
        try:
            reload_config()
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


def _extract_clip_sync(camera: str, start_iso: str, end_iso: str):
    """
    Slices raw 60s segments into a single alarm event clip.
    start_iso / end_iso format: '2026-09-30T15:30:00Z'
    """
    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    
    start_dt = start_dt - timedelta(seconds=config_data["minimumTimeBeforeAlarm"])
    end_dt = end_dt - timedelta(seconds=config_data["minimumTimeBeforeAlarm"])


    stream_dir = os.path.join(RECORDINGS_DIR, camera)
    if not os.path.exists(stream_dir):
        raise HTTPException(status_code=404, detail="Camera recordings directory not found")

    # Locate relevant segment files by timestamp naming
    all_files = sorted(glob.glob(f"{stream_dir}/*.{config_data['containerFormat']}"))
    matched_files = []
    
    clip_id = f"{camera}_{int(start_dt.timestamp())}_{int(end_dt.timestamp())}.{config_data['containerFormat']}"
    output_path = os.path.join(CLIPS_DIR, clip_id)
    #allows for more freedom of deleting temp recordings/"cache hit" speed increase
    if os.path.isfile(output_path):
        return {"url": f"/clips/{clip_id}", "filename": clip_id}


    for f in all_files:
        #base = os.path.splitext(os.path.basename(f))[0]
        try:
            unix_time = os.path.getmtime(f)
            file_time = datetime.fromtimestamp(unix_time).replace(tzinfo=timezone.utc)
            # Include files within a 60s tolerance window
            if file_time <= end_dt and (file_time.timestamp() + config_data["clipDurationSeconds"]) >= start_dt.timestamp():
                matched_files.append(f)
                single_file_time = file_time
        except ValueError:
            continue

    if not matched_files:
        raise HTTPException(status_code=404, detail="No footage found for requested time window")

    

    
    if len(matched_files) == 1:
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-hwaccel", "cuda",
            "-i", matched_files[0],
            "-ss", str(max(0, int((start_dt - single_file_time).total_seconds()))),
            "-to", str(int((end_dt - single_file_time).total_seconds())),
            # "-c", "copy", 
            "-c:v", config_data["videoCodec"], 
            "-crf", "38",
            #supposed to decrease file size, doesnt seem to work for h265
            "-preset", "slow",
            # "-c:a", config_data["audioCodec"],
            "-c:a", "copy",
            output_path
        ], check=True)
    else:
        # Multi-segment concat
        list_file = f"/tmp/{clip_id}.txt"
        with open(list_file, "w") as lf:
            for mf in matched_files:
                lf.write(f"file '{mf}'\n")
        subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-loglevel", "error",
            "-hwaccel", "cuda",
            "-i", list_file,
            # "-c", "copy", 
            "-c:v", config_data["videoCodec"], 
            "-crf", "38",
            "-preset", "slow",
            # "-c:a", config_data["audioCodec"],
            "-c:a", "copy",
            output_path
        ], check=True)

    return {"url": f"/clips/{clip_id}", "filename": clip_id}

@app.get("/api/storage")
def getStorageUsage():
    """
    Returns storage usage of the clips folder. Recordings folder is temporary, so less important, but still important
    """
    clipsFolder = CLIPS_DIR
    counter = 0
    for filename in os.listdir(clipsFolder):
        file_path = os.path.join(clipsFolder, filename)
        counter = counter + os.path.getsize(file_path)
    
    recordingsFolder = RECORDINGS_DIR
    for subdirectory in os.scandir(recordingsFolder):
        #for saving a demo folder
        if subdirectory.is_dir() and subdirectory.name == "DEMO_CAMERA_ONLY":
            continue
        for filename in os.listdir(subdirectory):
            file_path = os.path.join(subdirectory, filename)
            if os.path.isfile(file_path):
                counter = counter + os.path.getsize(file_path)

    return {"bytes": counter}

@app.post("/api/clip")
async def extract_clip(camera: str, start_iso: str, end_iso: str):
    """
    Slices raw segments into a single alarm event clip with a 5-minute timeout.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_extract_clip_sync, camera, start_iso, end_iso),
            timeout=300.0
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Clip extraction timed out after 5 minutes.")



@app.delete("/api/deleteRecordings")
def delete_recordings(ageInMinutes: int):
    """Delete old recordings in the data/recordings/(camera name) folders."""
    cutoff_time = time.time() - (ageInMinutes * 60) 
    recordingsFolder = RECORDINGS_DIR
    counter = 0
    for subdirectory in os.scandir(recordingsFolder):
        #for saving a demo folder
        if subdirectory.is_dir() and subdirectory.name == "DEMO_CAMERA_ONLY":
            continue
        for filename in os.listdir(subdirectory):
            file_path = os.path.join(subdirectory, filename)
            if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
                os.remove(file_path)
                counter = counter + 1
    return {"message": f"Deleted {counter} items", "counter":counter}


@app.delete("/api/deleteClips")
def delete_recordings(ageInMinutes: int):
    """Delete old clips in the data/clips folders."""
    cutoff_time = time.time() - (ageInMinutes * 60) 
    clipsFolder = CLIPS_DIR
    counter = 0
    for filename in os.listdir(clipsFolder):
        if filename.startswith("DEMO_CAMERA_ONLY"):
            continue
        file_path = os.path.join(clipsFolder, filename)
        if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
            os.remove(file_path)
            counter = counter + 1
    return {"message": f"Deleted {counter} clips", "counter":counter}
