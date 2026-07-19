# judge-worker/Dockerfile
FROM python:3.11-slim
WORKDIR /app
# Run in Vietnam time (GMT+7) so worker log timestamps line up with the rest of the stack.
# python:3.11-slim (Debian) ships no /usr/share/zoneinfo, so TZ alone would silently fall back to
# UTC — tzdata provides the zone files that make TZ resolve.
ENV TZ=Asia/Ho_Chi_Minh
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
CMD ["python", "main.py"]
