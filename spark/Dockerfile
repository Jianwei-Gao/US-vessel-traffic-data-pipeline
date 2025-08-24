FROM apache/spark:3.5.6-scala2.12-java17-python3-r-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-pip; \
    apt-get clean 

COPY ./requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/requirements.txt; \ 
    rm -rf /var/lib/apt/lists/*
RUN curl -LfO --output-dir /opt/spark/jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
ARG HOST_UID=185
ARG HOST_GID=185    

RUN groupmod -g ${HOST_UID} spark &&\
    usermod -d /opt/spark/ -u ${HOST_UID} -g ${HOST_GID} spark &&\
    chown -R --from=185 spark /opt &&\
    chown -R --from=:185 :spark /opt

ENV PYTHONPATH="${SPARK_HOME}/python/"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
ENV PATH="/opt/spark/bin:$PATH"

RUN curl -O --output-dir $SPARK_HOME/jars https://repo.maven.apache.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.7.2/sedona-spark-shaded-3.5_2.12-1.7.2.jar\
    && curl -O --output-dir $SPARK_HOME/jars https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/1.7.2-28.5/geotools-wrapper-1.7.2-28.5.jar


USER spark
ENV SHELL=/bin/bash
WORKDIR /opt/spark/
ENTRYPOINT ["/bin/bash"]