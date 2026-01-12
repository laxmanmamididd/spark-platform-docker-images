# ============================================================================
# DOORDASH SPARK CONNECT SERVER - Docker Image
# ============================================================================
# This is the SERVER-SIDE component that runs on SK8 (Kubernetes)
# Equivalent to Databricks Runtime (DBR) but for DoorDash
#
# Contains:
#   - Apache Spark 3.5 with Spark Connect Server
#   - Unity Catalog plugin
#   - Cloud storage connectors (S3, Iceberg)
#   - Python environment with data science packages
# ============================================================================

FROM apache/spark:3.5.3-java17

LABEL maintainer="data-infrastructure@doordash.com"
LABEL description="DoorDash Spark Connect Server for SK8"
LABEL version="1.0.0"

USER root

# ============================================================================
# 1. ENVIRONMENT VARIABLES
# ============================================================================
ENV SPARK_HOME=/opt/spark
ENV SPARK_CONNECT_ENABLED=true
ENV SPARK_CONNECT_GRPC_PORT=15002
ENV PYTHONUNBUFFERED=1

# ============================================================================
# 2. INSTALL ADDITIONAL SYSTEM DEPENDENCIES
# ============================================================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    procps \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# 3. DOWNLOAD SPARK CONNECT + UNITY CATALOG + CLOUD JARS
# ============================================================================
WORKDIR /opt/spark/jars

# Spark Connect (required for gRPC server)
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.3/spark-connect_2.12-3.5.3.jar

# Unity Catalog Spark Plugin
RUN wget -q https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-spark_2.12/0.2.0/unitycatalog-spark_2.12-0.2.0.jar

# AWS S3 Support
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Iceberg Support
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

# Delta Lake Support
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# gRPC dependencies (if not included in spark-connect)
RUN wget -q https://repo1.maven.org/maven2/io/grpc/grpc-netty-shaded/1.59.0/grpc-netty-shaded-1.59.0.jar

# ============================================================================
# 4. SPARK CONFIGURATION
# ============================================================================
WORKDIR /opt/spark

# Create spark-defaults.conf with Spark Connect and Unity Catalog settings
RUN mkdir -p /opt/spark/conf && \
    cat > /opt/spark/conf/spark-defaults.conf << 'EOF'
# ============================================================================
# Spark Connect Server Configuration
# ============================================================================

# Enable Spark Connect plugin
spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

# Spark Connect gRPC server port
spark.connect.grpc.binding.port=15002

# ============================================================================
# Unity Catalog Configuration
# ============================================================================

# Default catalog
spark.sql.catalog.datalake=io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.datalake.uri=${UNITY_CATALOG_URI:-http://unity-catalog.doordash.team:8080}
spark.sql.catalog.datalake.token=${UNITY_CATALOG_TOKEN:-}

# Iceberg catalog (via Unity Catalog)
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=rest
spark.sql.catalog.iceberg.uri=${UNITY_CATALOG_URI:-http://unity-catalog.doordash.team:8080}/api/2.1/unity-catalog/iceberg

# Default to datalake catalog
spark.sql.defaultCatalog=datalake

# ============================================================================
# SQL Extensions
# ============================================================================
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# ============================================================================
# AWS S3 Configuration
# ============================================================================
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain

# ============================================================================
# Performance Settings
# ============================================================================
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer

# ============================================================================
# Memory Settings (will be overridden by SparkApplication CRD)
# ============================================================================
spark.driver.memory=4g
spark.executor.memory=4g
EOF

# Create log4j2.properties for logging
RUN cat > /opt/spark/conf/log4j2.properties << 'EOF'
# Set root logger level to INFO
rootLogger.level = INFO
rootLogger.appenderRef.stdout.ref = console

# Console appender
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_OUT
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n

# Spark Connect specific logging
logger.connect.name = org.apache.spark.sql.connect
logger.connect.level = INFO

# gRPC logging
logger.grpc.name = io.grpc
logger.grpc.level = WARN
EOF

# ============================================================================
# 5. PYTHON ENVIRONMENT
# ============================================================================
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    pyarrow>=14.0.0 \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    grpcio>=1.59.0 \
    grpcio-status>=1.59.0 \
    protobuf>=4.25.0

# ============================================================================
# 6. ENTRYPOINT SCRIPT
# ============================================================================
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

# ============================================================================
# 7. HEALTH CHECK
# ============================================================================
COPY healthcheck.sh /opt/spark/healthcheck.sh
RUN chmod +x /opt/spark/healthcheck.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD /opt/spark/healthcheck.sh

# ============================================================================
# 8. EXPOSE PORTS
# ============================================================================
# 15002 - Spark Connect gRPC server
# 4040  - Spark UI
# 7077  - Spark Master (if standalone mode)
# 8080  - Spark Master Web UI
EXPOSE 15002 4040 7077 8080

# ============================================================================
# 9. SET USER AND WORKING DIRECTORY
# ============================================================================
USER spark
WORKDIR /opt/spark

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
