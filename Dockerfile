FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY src/ ./src/
COPY static/ ./static/

ENV DATA_DIR=/data
ENV BATCH_SIZE=100
ENV BATCH_INTERVAL=1.0
VOLUME ["/data"]

CMD ["python", "main.py"]
