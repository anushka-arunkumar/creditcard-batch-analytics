FROM python:3.11-slim-bookworm

# Install required system utilities for Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Copies dependencies.txt file from local machine into the container
COPY dependencies.txt /tmp/dependencies.txt

# Install Python dependencies
RUN pip install --no-cache-dir \
    -r /tmp/dependencies.txt

# Ensure PySpark uses this Python interpreter
ENV PYSPARK_PYTHON=/usr/local/bin/python

# Create the spark user and group with UID/GID 1099
RUN groupadd -g 1099 spark && \
    useradd -u 1099 -g 1099 -d /home/spark -m spark

USER spark