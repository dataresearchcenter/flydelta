FROM python:3.14-slim

WORKDIR /app

RUN pip install --no-cache-dir flydelta

EXPOSE 8815

ENTRYPOINT ["flydelta", "serve"]
