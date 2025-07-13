FROM apache/spark:4.0.0-scala2.13-java17-python3-r-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-pip; \
    apt-get clean 

COPY ./requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/requirements.txt; \ 
    # rm requirements.txt; \
    rm -rf /var/lib/apt/lists/*
RUN curl -LfO --output-dir /opt/spark/jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
ARG HOST_UID=185
ARG HOST_GID=185    

RUN groupmod -g ${HOST_UID} spark &&\
    usermod -d /opt/spark/ -u ${HOST_UID} -g ${HOST_GID} spark &&\
    chown -R --from=185 spark /opt &&\
    chown -R --from=:185 :spark /opt

ENV PYTHONPATH="${SPARK_HOME}/python/"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.9-src.zip:$PYTHONPATH"
ENV PATH="/opt/spark/bin:$PATH"


USER spark
WORKDIR /opt/spark/
ENTRYPOINT ["/bin/sh"]