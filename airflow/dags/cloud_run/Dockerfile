FROM python:3.12-slim-bullseye

USER root
WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir polars

CMD ["python", "main.py"]