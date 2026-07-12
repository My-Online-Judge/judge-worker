# judge-worker/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir kafka-python==2.0.2 requests==2.32.3
COPY *.py ./
CMD ["python", "main.py"]
