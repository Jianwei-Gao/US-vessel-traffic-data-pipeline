FROM apache/airflow:3.0.2

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV SHELL=/bin/bash
COPY ./requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

WORKDIR $AIRFLOW_HOME
