FROM apache/airflow:2.10.3

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir scikit-learn \
    && pip install --no-cache-dir openai

#RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"1

