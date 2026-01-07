package com.doordash.spark.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * TestContainer for Spark with Spark Connect support.
 *
 * This container starts a Spark driver with Spark Connect server enabled,
 * allowing clients to connect via the sc:// protocol.
 *
 * Architecture:
 * <pre>
 * ┌─────────────────────────────────────────────────┐
 * │              SparkContainer                      │
 * │  ┌─────────────────────────────────────────┐    │
 * │  │  Spark Driver                           │    │
 * │  │  ┌─────────────────────────────────┐    │    │
 * │  │  │  Spark Connect Server (15002)   │◄───┼────┼── Test Client
 * │  │  └─────────────────────────────────┘    │    │
 * │  │  ┌─────────────────────────────────┐    │    │
 * │  │  │  Spark UI (4040)                │◄───┼────┼── Browser
 * │  │  └─────────────────────────────────┘    │    │
 * │  └─────────────────────────────────────────┘    │
 * └─────────────────────────────────────────────────┘
 * </pre>
 */
public class SparkContainer extends GenericContainer<SparkContainer> {

    public static final int SPARK_CONNECT_PORT = 15002;
    public static final int SPARK_UI_PORT = 4040;
    public static final int SPARK_MASTER_PORT = 7077;

    private static final String DEFAULT_IMAGE = "spark-testing:latest";
    private static final String DEFAULT_SPARK_VERSION = "3.5.0";

    private final Map<String, String> sparkConfig = new HashMap<>();

    /**
     * Creates a SparkContainer with the default image.
     */
    public SparkContainer() {
        this(DockerImageName.parse(DEFAULT_IMAGE));
    }

    /**
     * Creates a SparkContainer with a custom image.
     *
     * @param dockerImageName the Docker image to use
     */
    public SparkContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        // Expose ports
        withExposedPorts(SPARK_CONNECT_PORT, SPARK_UI_PORT, SPARK_MASTER_PORT);

        // Set environment variables
        withEnv("SPARK_ROLE", "driver");
        withEnv("SPARK_CONNECT_ENABLED", "true");
        withEnv("SPARK_CONNECT_PORT", String.valueOf(SPARK_CONNECT_PORT));

        // Wait strategy - wait for Spark Connect to be ready
        waitingFor(Wait.forListeningPort()
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Sets a Spark configuration property.
     *
     * @param key   the configuration key (e.g., "spark.executor.memory")
     * @param value the configuration value
     * @return this container for chaining
     */
    public SparkContainer withSparkConfig(String key, String value) {
        sparkConfig.put(key, value);
        return this;
    }

    /**
     * Sets multiple Spark configuration properties.
     *
     * @param config map of configuration key-value pairs
     * @return this container for chaining
     */
    public SparkContainer withSparkConfigs(Map<String, String> config) {
        sparkConfig.putAll(config);
        return this;
    }

    /**
     * Enables Unity Catalog integration.
     *
     * @param catalogUri    the Unity Catalog URI
     * @param catalogToken  the authentication token
     * @return this container for chaining
     */
    public SparkContainer withUnityCatalog(String catalogUri, String catalogToken) {
        withSparkConfig("spark.sql.catalog.unity", "org.apache.iceberg.spark.SparkCatalog");
        withSparkConfig("spark.sql.catalog.unity.type", "rest");
        withSparkConfig("spark.sql.catalog.unity.uri", catalogUri);
        withSparkConfig("spark.sql.catalog.unity.token", catalogToken);
        return this;
    }

    /**
     * Configures S3 access (for MinIO or LocalStack in tests).
     *
     * @param endpoint  the S3 endpoint
     * @param accessKey the access key
     * @param secretKey the secret key
     * @return this container for chaining
     */
    public SparkContainer withS3Config(String endpoint, String accessKey, String secretKey) {
        withSparkConfig("spark.hadoop.fs.s3a.endpoint", endpoint);
        withSparkConfig("spark.hadoop.fs.s3a.access.key", accessKey);
        withSparkConfig("spark.hadoop.fs.s3a.secret.key", secretKey);
        withSparkConfig("spark.hadoop.fs.s3a.path.style.access", "true");
        return this;
    }

    @Override
    protected void configure() {
        super.configure();

        // Apply Spark configuration as environment variables
        for (Map.Entry<String, String> entry : sparkConfig.entrySet()) {
            String envKey = "SPARK_CONF_" + entry.getKey().replace(".", "_").toUpperCase();
            withEnv(envKey, entry.getValue());
        }
    }

    /**
     * Returns the Spark Connect URL for connecting to this container.
     *
     * @return the Spark Connect URL in format sc://host:port
     */
    public String getSparkConnectUrl() {
        return String.format("sc://%s:%d", getHost(), getMappedPort(SPARK_CONNECT_PORT));
    }

    /**
     * Returns the Spark UI URL for this container.
     *
     * @return the Spark UI URL
     */
    public String getSparkUIUrl() {
        return String.format("http://%s:%d", getHost(), getMappedPort(SPARK_UI_PORT));
    }

    /**
     * Returns the mapped Spark Connect port.
     *
     * @return the host port mapped to Spark Connect
     */
    public int getSparkConnectPort() {
        return getMappedPort(SPARK_CONNECT_PORT);
    }

    /**
     * Returns the Spark version running in this container.
     *
     * @return the Spark version
     */
    public String getSparkVersion() {
        return DEFAULT_SPARK_VERSION;
    }

    /**
     * Creates a builder for SparkContainer with common configurations.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for SparkContainer with fluent API.
     */
    public static class Builder {
        private String image = DEFAULT_IMAGE;
        private int executorMemoryMb = 1024;
        private int driverMemoryMb = 1024;
        private final Map<String, String> sparkConfig = new HashMap<>();

        public Builder withImage(String image) {
            this.image = image;
            return this;
        }

        public Builder withExecutorMemory(int memoryMb) {
            this.executorMemoryMb = memoryMb;
            return this;
        }

        public Builder withDriverMemory(int memoryMb) {
            this.driverMemoryMb = memoryMb;
            return this;
        }

        public Builder withSparkConfig(String key, String value) {
            this.sparkConfig.put(key, value);
            return this;
        }

        public SparkContainer build() {
            SparkContainer container = new SparkContainer(DockerImageName.parse(image));
            container.withSparkConfig("spark.executor.memory", executorMemoryMb + "m");
            container.withSparkConfig("spark.driver.memory", driverMemoryMb + "m");
            container.withSparkConfigs(sparkConfig);
            return container;
        }
    }
}
