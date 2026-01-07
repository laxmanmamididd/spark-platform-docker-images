# Spark Docker Image for Testing
# Supports both batch jobs and Spark Connect interactive sessions

ARG SPARK_VERSION=3.5.0
ARG JAVA_VERSION=17

# ============================================
# LAYER 1: Base OS + JVM
# ============================================
FROM openjdk:${JAVA_VERSION}-slim as base

# Install required packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    procps \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ============================================
# LAYER 2: Spark Runtime
# ============================================
FROM base as spark

ARG SPARK_VERSION
ENV SPARK_VERSION=${SPARK_VERSION}
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download and install Spark
RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME

# Install PySpark
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION}

# ============================================
# LAYER 3: Connectors & Dependencies
# ============================================
FROM spark as connectors

# Add Hadoop AWS connector for S3 access
ARG HADOOP_AWS_VERSION=3.3.4
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o $SPARK_HOME/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar

# Add AWS SDK
ARG AWS_SDK_VERSION=1.12.367
RUN curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o $SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Add Iceberg runtime
ARG ICEBERG_VERSION=1.4.2
RUN curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    -o $SPARK_HOME/jars/iceberg-spark-runtime.jar

# ============================================
# LAYER 4: Configuration & Entrypoint
# ============================================
FROM connectors as final

# Copy configuration files
COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY entrypoint.sh /opt/entrypoint.sh

# Make entrypoint executable
RUN chmod +x /opt/entrypoint.sh

# Environment variables for role detection
ENV SPARK_ROLE=driver
ENV SPARK_CONNECT_ENABLED=true
ENV SPARK_CONNECT_PORT=15002

# Expose ports
# 4040: Spark UI
# 7077: Spark Master (standalone mode)
# 15002: Spark Connect gRPC
# 18080: History Server
EXPOSE 4040 7077 15002 18080

# Set working directory
WORKDIR $SPARK_HOME

# Entrypoint handles role-based startup
ENTRYPOINT ["/opt/entrypoint.sh"]
