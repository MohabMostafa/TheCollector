FROM nvidia/cuda:12.8.0-cudnn-runtime-ubuntu22.04

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt update && \
    apt install -y build-essential software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt update && \
    apt install -y python3.10 python-is-python3 python3-pip ffmpeg curl && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dialect_server.py .
COPY startup.sh .

# Make the startup script executable
RUN chmod +x /app/startup.sh

# Create directories for mounted volumes
RUN mkdir -p /app/audio-and-captions

# Expose the port
EXPOSE 3003

# Command to run the startup script
CMD ["/app/startup.sh"]
