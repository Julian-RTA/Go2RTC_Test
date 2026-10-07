FROM python:3.11-slim

# Install ffmpeg and cleanup package caches
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ffmpeg_management_server.py .
COPY videoConfig.yaml .

EXPOSE 8000

CMD ["uvicorn", "ffmpeg_management_server:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "300"]

#currently cant use gpu. have to fix that later for prod
# # Step 1: Extract pre-compiled NVIDIA-FFmpeg binaries
# FROM jrottenberg/ffmpeg:7.0-nvidia AS ffmpeg_base

# # Step 2: Use the official NVIDIA CUDA 13.2 runtime environment
# FROM nvidia/cuda:13.2.0-runtime-ubuntu24.04

# # Copy GPU-compiled FFmpeg binaries & core library hooks from Step 1
# COPY --from=ffmpeg_base /usr/local /usr/local

# # Install Python 3.11 and package management essentials
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     python3.11 \
#     python3-pip \
#     python3.11-dev \
#     && rm -rf /var/lib/apt/lists/*

# # Redirect standard 'python' and 'pip' commands to use Python 3.11
# RUN ln -s /usr/bin/python3.11 /usr/bin/python

# WORKDIR /app

# # Optimize build caching: copy and run pip install before copying source code
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy your FastAPI application files into the container
# COPY ffmpeg_management_server.py .
# COPY videoConfig.yaml .

# # Expose the internal port your server binds to
# EXPOSE 8000

# # Your updated backend server start command
# CMD ["uvicorn", "ffmpeg_management_server:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "300"]

