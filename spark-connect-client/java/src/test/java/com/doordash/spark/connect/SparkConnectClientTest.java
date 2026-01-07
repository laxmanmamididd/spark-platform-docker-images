package com.doordash.spark.connect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for SparkConnectClient.
 */
class SparkConnectClientTest {

    @Test
    @DisplayName("Builder should require team name")
    void builderRequiresTeam() {
        assertThatThrownBy(() -> SparkConnectClient.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Team is required");
    }

    @Test
    @DisplayName("Environment enum should have correct values")
    void environmentValues() {
        assertThat(SparkConnectClient.Environment.DEV.getValue()).isEqualTo("dev");
        assertThat(SparkConnectClient.Environment.STAGING.getValue()).isEqualTo("staging");
        assertThat(SparkConnectClient.Environment.CI.getValue()).isEqualTo("ci");
        assertThat(SparkConnectClient.Environment.PROD.getValue()).isEqualTo("prod");
    }

    @Test
    @DisplayName("Region enum should have correct gateway hosts")
    void regionGatewayHosts() {
        assertThat(SparkConnectClient.Region.US_WEST_2.getGatewayHost())
                .isEqualTo("spark-gateway-us-west-2.doordash.team");
        assertThat(SparkConnectClient.Region.US_EAST_1.getGatewayHost())
                .isEqualTo("spark-gateway-us-east-1.doordash.team");
        assertThat(SparkConnectClient.Region.EU_WEST_1.getGatewayHost())
                .isEqualTo("spark-gateway-eu-west-1.doordash.team");
    }

    @Test
    @DisplayName("createSessionFromEnv should fail without SPARK_CONNECT_TEAM")
    void createSessionFromEnvRequiresTeam() {
        // Note: This test assumes SPARK_CONNECT_TEAM is not set in the environment
        // In CI, you may need to unset it explicitly
        assertThatThrownBy(() -> SparkConnectClient.createSessionFromEnv())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SPARK_CONNECT_TEAM");
    }
}
