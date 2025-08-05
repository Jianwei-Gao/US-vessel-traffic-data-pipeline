FROM python:3.12-slim-bullseye

USER root
WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

RUN mkdir /mnt/gcs

ENTRYPOINT ["python", "main.py"]