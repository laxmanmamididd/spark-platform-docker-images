package com.doordash.spark.connect;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Spark Connect Client for DoorDash Pedregal Platform.
 *
 * <p>This class provides a standardized way to connect to Spark Connect clusters
 * through Spark Gateway. Teams use the Apache Spark Connect client library
 * with DoorDash-specific configuration helpers.</p>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * SparkSession spark = SparkConnectClient.builder()
 *     .team("feature-engineering")
 *     .environment(Environment.PROD)
 *     .region(Region.US_WEST_2)
 *     .build();
 *
 * Dataset<Row> df = spark.table("pedregal.feature_store.user_features");
 * }</pre>
 */
public class SparkConnectClient {

    private static final Logger logger = LoggerFactory.getLogger(SparkConnectClient.class);

    /**
     * Supported environments for Spark Connect clusters.
     */
    public enum Environment {
        DEV("dev"),
        STAGING("staging"),
        CI("ci"),
        PROD("prod");

        private final String value;

        Environment(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * Supported regions for Spark Connect clusters.
     */
    public enum Region {
        US_WEST_2("us-west-2", "spark-gateway-us-west-2.doordash.team"),
        US_EAST_1("us-east-1", "spark-gateway-us-east-1.doordash.team"),
        EU_WEST_1("eu-west-1", "spark-gateway-eu-west-1.doordash.team");

        private final String value;
        private final String gatewayHost;

        Region(String value, String gatewayHost) {
            this.value = value;
            this.gatewayHost = gatewayHost;
        }

        public String getValue() {
            return value;
        }

        public String getGatewayHost() {
            return gatewayHost;
        }
    }

    private SparkConnectClient() {
        // Use builder
    }

    /**
     * Create a new builder for SparkConnectClient.
     *
     * @return A new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Quick factory method to create a Spark session.
     *
     * @param team        Team name (e.g., "feature-engineering")
     * @param environment Environment (dev, staging, ci, prod)
     * @param region      Region (us-west-2, us-east-1, eu-west-1)
     * @return SparkSession connected to the remote cluster
     */
    public static SparkSession createSession(String team, Environment environment, Region region) {
        return builder()
                .team(team)
                .environment(environment)
                .region(region)
                .build();
    }

    /**
     * Create a Spark session from environment variables.
     *
     * <p>Reads the following environment variables:</p>
     * <ul>
     *   <li>SPARK_CONNECT_TEAM - Team name (required)</li>
     *   <li>SPARK_CONNECT_ENV - Environment (default: dev)</li>
     *   <li>SPARK_CONNECT_REGION - Region (default: us-west-2)</li>
     * </ul>
     *
     * @return SparkSession connected to the remote cluster
     */
    public static SparkSession createSessionFromEnv() {
        String team = System.getenv("SPARK_CONNECT_TEAM");
        if (team == null || team.isEmpty()) {
            throw new IllegalStateException("SPARK_CONNECT_TEAM environment variable is required");
        }

        String envStr = Optional.ofNullable(System.getenv("SPARK_CONNECT_ENV")).orElse("dev");
        String regionStr = Optional.ofNullable(System.getenv("SPARK_CONNECT_REGION")).orElse("us-west-2");

        Environment environment = Environment.valueOf(envStr.toUpperCase());
        Region region = Region.valueOf(regionStr.toUpperCase().replace("-", "_"));

        return createSession(team, environment, region);
    }

    /**
     * Builder for creating Spark Connect sessions.
     */
    public static class Builder {
        private String team;
        private Environment environment = Environment.DEV;
        private Region region = Region.US_WEST_2;
        private String appName;
        private int port = 15002;
        private final Map<String, String> extraConfig = new HashMap<>();

        /**
         * Set the team name.
         *
         * @param team Team name (e.g., "feature-engineering", "ml-platform")
         * @return this builder
         */
        public Builder team(String team) {
            this.team = team;
            return this;
        }

        /**
         * Set the environment.
         *
         * @param environment Environment (DEV, STAGING, CI, PROD)
         * @return this builder
         */
        public Builder environment(Environment environment) {
            this.environment = environment;
            return this;
        }

        /**
         * Set the region.
         *
         * @param region Region (US_WEST_2, US_EAST_1, EU_WEST_1)
         * @return this builder
         */
        public Builder region(Region region) {
            this.region = region;
            return this;
        }

        /**
         * Set the application name.
         *
         * @param appName Application name for the Spark session
         * @return this builder
         */
        public Builder appName(String appName) {
            this.appName = appName;
            return this;
        }

        /**
         * Set the gateway port.
         *
         * @param port Gateway port (default: 15002)
         * @return this builder
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Add extra Spark configuration.
         *
         * @param key   Configuration key
         * @param value Configuration value
         * @return this builder
         */
        public Builder config(String key, String value) {
            this.extraConfig.put(key, value);
            return this;
        }

        /**
         * Build the SparkSession.
         *
         * @return SparkSession connected to the remote Spark Connect cluster
         */
        public SparkSession build() {
            if (team == null || team.isEmpty()) {
                throw new IllegalStateException("Team is required");
            }

            String endpoint = buildEndpoint();
            logger.info("Connecting to Spark Connect at: {}", endpoint);

            SparkSession.Builder sparkBuilder = SparkSession.builder()
                    .remote(endpoint);

            // Set app name
            String effectiveAppName = appName != null ? appName : team + "-" + environment.getValue() + "-session";
            sparkBuilder.appName(effectiveAppName);

            // Apply extra config
            for (Map.Entry<String, String> entry : extraConfig.entrySet()) {
                sparkBuilder.config(entry.getKey(), entry.getValue());
            }

            return sparkBuilder.getOrCreate();
        }

        private String buildEndpoint() {
            // Build team-specific subdomain
            // Format: sc://[team-short]-[env-short]-[region-short].doordash.team:15002
            String teamShort = team.replace("-", "").substring(0, Math.min(10, team.replace("-", "").length()));
            String envShort = environment.getValue().substring(0, 3);
            String regionShort = region.getValue().replace("-", "");

            String subdomain = teamShort + "-" + envShort + "-" + regionShort;
            return "sc://" + subdomain + ".doordash.team:" + port;
        }
    }

    /**
     * Main method demonstrating usage.
     */
    public static void main(String[] args) {
        System.out.println("Spark Connect Client for DoorDash");
        System.out.println("=".repeat(50));

        // Show example endpoint
        Builder builder = new Builder()
                .team("feature-engineering")
                .environment(Environment.PROD)
                .region(Region.US_WEST_2);

        System.out.println("Example endpoint: sc://featureeng-pro-uswest2.doordash.team:15002");

        System.out.println("\nUsage:");
        System.out.println("  SparkSession spark = SparkConnectClient.builder()");
        System.out.println("      .team(\"feature-engineering\")");
        System.out.println("      .environment(Environment.PROD)");
        System.out.println("      .region(Region.US_WEST_2)");
        System.out.println("      .build();");
        System.out.println("");
        System.out.println("  Dataset<Row> df = spark.table(\"pedregal.feature_store.user_features\");");
    }
}
