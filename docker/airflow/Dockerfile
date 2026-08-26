# Airflow image: scheduler, api-server, dag-processor and triggerer all run from
# this one image, and so does the Spark *client*.
#
# Why a custom image at all: the DAG submits the Silver and Gold transforms with
# SparkSubmitOperator, which shells out to the `spark-submit` binary and runs the
# driver in-process (client mode). That means this image needs a JVM, a Spark
# distribution, and the same connector jars as the cluster — none of which ship
# in the stock Airflow image.
FROM apache/airflow:3.1.8-python3.11

# python3.11 is not the default variant (that is 3.10) but is what CLAUDE.md §8
# targets, and it is also the newest interpreter Spark 3.5 officially supports.

USER root

# procps: spark-submit's launcher inspects processes while tracking the driver.
# curl: fetching the connector jars below.
# The JRE is the only heavyweight addition — Spark 3.5 supports Java 8/11/17,
# and bookworm (this image's base) still carries 17.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# PySpark is pinned to the cluster's exact version, NOT to the version in
# Airflow's own constraints file (which resolves pyspark to 4.1.1). A Spark 4
# client cannot drive a Spark 3.5 standalone master: the deploy RPC and the
# task-serialization format both changed across the major version, and the
# failure is an opaque handshake error at submit time rather than anything that
# names a version. The provider itself only requires pyspark>=3.5.2, so pinning
# down to the cluster version is a supported configuration — the constraint file
# is a resolver snapshot, not a compatibility floor.
#
# Keep this equal to docker/spark/requirements.txt.
ARG SPARK_VERSION=3.5.3
ARG AIRFLOW_VERSION=3.1.8

# Constraints keep the provider install from disturbing the carefully resolved
# dependency set the base image ships with. pyspark is filtered out of them so
# our pin wins rather than being overridden back up to 4.1.1 — and py4j with it,
# because the constraint file pins the py4j that pyspark 4.1.1 requires
# (0.10.9.9) while 3.5.3 requires exactly 0.10.9.7. Leaving py4j constrained
# makes the resolve impossible rather than merely wrong, which at least fails
# loudly at build time.
RUN curl -fsSL -o /tmp/constraints.txt \
        "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
    && grep -vE '^(pyspark|py4j)==' /tmp/constraints.txt > /tmp/constraints-spark.txt \
    && pip install --no-cache-dir --constraint /tmp/constraints-spark.txt \
        "apache-airflow-providers-apache-spark==5.5.1" \
        "pyspark==${SPARK_VERSION}" \
    && rm /tmp/constraints.txt /tmp/constraints-spark.txt

# The remaining runtime dependencies of src/ — requests, tenacity, boto3,
# python-dotenv, psycopg2-binary — are already in the base image, so there is
# nothing to add for the extract and data-quality tasks.

# Connector jars, identical in version to docker/spark/Dockerfile. The driver
# runs here, so it resolves s3a:// paths and opens the JDBC staging writes
# itself; without these the Silver read fails with ClassNotFoundException before
# a single executor is contacted. Versions are not free to choose — hadoop-aws
# must match the hadoop-client jars bundled in this PySpark release (3.3.4), and
# aws-java-sdk-bundle must be what hadoop-aws 3.3.4 was built against.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN=https://repo1.maven.org/maven2
RUN SPARK_JARS="$(python -c 'import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).parent / "jars")')" \
    && curl -fsSL -o "${SPARK_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_JARS}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        "${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# spark-submit lives in pyspark's bin/, which pip does not put on PATH.
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# The project is mounted at /opt/football (see docker-compose.yml). Both the
# in-process tasks (extract, data quality) and the spark-submit driver import
# `src.*`, and the driver inherits this from the task process's environment.
ENV PYTHONPATH=/opt/football

# The client-mode driver writes scratch and shuffle files; its default sits
# under SPARK_HOME inside site-packages, which the airflow user cannot write to.
ENV SPARK_LOCAL_DIRS=/home/airflow/spark-local
RUN mkdir -p /home/airflow/spark-local
