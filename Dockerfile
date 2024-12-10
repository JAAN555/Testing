FROM apache/airflow:2.6.0

USER root

# Install additional libraries
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    build-essential

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow