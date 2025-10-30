FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy application files
COPY ./main.py /app/main.py
COPY ./config.yaml /app/config.yaml

# Install dependencies
RUN pip install --no-cache-dir \
    firecrawl-py==1.11.0 \
    pyyaml==6.0.2 \
    requests==2.32.3

ENTRYPOINT ["python", "/app/main.py"]
