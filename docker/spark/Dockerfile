# Spark image for the Silver/Gold transforms.
#
# Built on python:3.11-slim with pip-installed PySpark rather than on the
# official apache/spark image, which ships Python 3.8 — this project targets
# 3.11+ (CLAUDE.md §8) and running pipeline code on an interpreter we never test
# against is how subtle bugs reach production.
#
# The pyspark wheel ships the full jar set and `bin/spark-class`, but NOT the
# `sbin/start-master.sh` / `start-worker.sh` launchers. docker-compose therefore
# starts the standalone daemons through `spark-class` directly, which is the
# better container pattern regardless: the JVM stays in the foreground as PID 1
# and receives signals, instead of being daemonized away from the supervisor.
#
# Pinned to bookworm, not plain `-slim`: the unsuffixed tag now tracks Debian
# trixie, which dropped openjdk-17 in favour of 21, and Spark 3.5 supports only
# Java 8/11/17. Bumping this base image means revisiting the JRE version.
FROM python:3.11-slim-bookworm

# procps: Spark's sbin scripts need `ps` to track daemons.
# curl: fetching the s3a jars below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH=/opt/football
ENV PYSPARK_PYTHON=python3

COPY docker/spark/requirements.txt /tmp/spark-requirements.txt
RUN pip install --no-cache-dir -r /tmp/spark-requirements.txt

# s3a:// support for MinIO. These versions are not free to choose: hadoop-aws
# must match the hadoop-client jars bundled in this PySpark release (3.3.4), and
# aws-java-sdk-bundle must be the version hadoop-aws 3.3.4 was built against.
# A mismatch fails at runtime with NoSuchMethodError, not at build time.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2
RUN curl -fsSL -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# JDBC driver for the Silver -> Gold load. Needed in the image rather than via
# --packages because the executors open their own connections to Postgres, so
# every node has to have it on the classpath.
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN curl -fsSL -o "${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        "${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# Spark writes scratch, shuffle files and per-executor work dirs as the running
# user. Their defaults sit under SPARK_HOME — i.e. inside root-owned
# site-packages — so a non-root worker cannot create them and dies on startup
# with AccessDeniedException. Redirect them somewhere the `spark` user owns.
ENV SPARK_WORKER_DIR=/home/spark/work
ENV SPARK_LOCAL_DIRS=/home/spark/local

RUN useradd --create-home --shell /bin/bash spark \
    && mkdir -p /opt/football /home/spark/work /home/spark/local /home/spark/events \
    && chown -R spark:spark /opt/football /home/spark

USER spark
WORKDIR /opt/football
