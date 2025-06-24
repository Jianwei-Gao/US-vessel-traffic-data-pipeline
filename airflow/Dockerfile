FROM apache/airflow:latest

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* 

USER airflow
COPY ./airflow/requirements.txt .
COPY ./credentials/airflow-google-credentials.json /opt/airflow/google/airflow-google-credentials.json
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

WORKDIR $AIRFLOW_HOME
