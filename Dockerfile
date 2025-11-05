FROM apache/airflow:2.10.0

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
ENV PYSPARK_SUBMIT_ARGS="--master local[*] pyspark-shell"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
