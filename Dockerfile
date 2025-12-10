FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY flydelta ./flydelta

RUN pip install -q --no-cache-dir ".[server]"

EXPOSE 8815

ENTRYPOINT ["flydelta", "serve"]
