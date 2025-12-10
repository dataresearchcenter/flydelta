FROM python:3.13-slim

RUN apt-get update -y \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY flydelta ./flydelta

RUN pip install --no-cache-dir -U pip setuptools
RUN pip install --no-cache-dir ".[server]"

EXPOSE 8815

ENTRYPOINT ["flydelta", "serve"]
