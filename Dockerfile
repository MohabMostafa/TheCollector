# Use an official Python image as a base
FROM python:3.10

# Set working directory inside container
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install -r requirements.txt
RUN apt update && apt install -y ffmpeg

# Ensure required directories exist
RUN mkdir -p keywords url_list dagster_home && touch dagster_home/dagster.yaml

# Set environment variable for Dagster
ENV DAGSTER_HOME=/app/dagster_home

# Use dagster dev so that the daemon (and sensors) are started automatically.
# We override the default dagit port via the DAGIT_PORT environment variable.
CMD ["dagster", "dev", "-w", "/app/workspace.yaml"]
