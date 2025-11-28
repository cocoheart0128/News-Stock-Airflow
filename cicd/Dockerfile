# Dockerfile
FROM apache/airflow:3.1.3

USER root
RUN apt-get update && apt-get install -y git

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

