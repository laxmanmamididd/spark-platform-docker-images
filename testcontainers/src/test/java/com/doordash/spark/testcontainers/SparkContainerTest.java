package com.doordash.spark.testcontainers;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SparkContainer using TestContainers.
 *
 * These tests demonstrate how to:
 * 1. Start a Spark container with Spark Connect
 * 2. Connect to it using the Spark Connect client
 * 3. Execute Spark SQL queries
 * 4. Verify results
 */
@Testcontainers
class SparkContainerTest {

    @Container
    static SparkContainer sparkContainer = SparkContainer.builder()
            .withDriverMemory(2048)
            .withSparkConfig("spark.sql.shuffle.partitions", "4")
            .build();

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        // Connect to the Spark container using Spark Connect
        String connectUrl = sparkContainer.getSparkConnectUrl();
        System.out.println("Connecting to Spark at: " + connectUrl);

        spark = SparkSession.builder()
                .remote(connectUrl)
                .appName("SparkContainerTest")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.close();
        }
    }

    @Test
    void shouldCreateSparkSession() {
        assertThat(spark).isNotNull();
        assertThat(spark.version()).isEqualTo(sparkContainer.getSparkVersion());
    }

    @Test
    void shouldExecuteSimpleQuery() {
        // Create a simple DataFrame
        var df = spark.sql("SELECT 1 as id, 'hello' as message");

        // Verify results
        var rows = df.collectAsList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("hello");
    }

    @Test
    void shouldExecuteAggregation() {
        // Create test data
        spark.sql("CREATE OR REPLACE TEMP VIEW test_data AS " +
                "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'a') " +
                "AS t(id, category)");

        // Execute aggregation
        var result = spark.sql(
                "SELECT category, COUNT(*) as cnt, SUM(id) as total " +
                        "FROM test_data GROUP BY category ORDER BY category");

        // Verify results
        var rows = result.collectAsList();
        assertThat(rows).hasSize(2);

        // Category 'a'
        assertThat(rows.get(0).getString(0)).isEqualTo("a");
        assertThat(rows.get(0).getLong(1)).isEqualTo(3);
        assertThat(rows.get(0).getLong(2)).isEqualTo(9); // 1 + 3 + 5

        // Category 'b'
        assertThat(rows.get(1).getString(0)).isEqualTo("b");
        assertThat(rows.get(1).getLong(1)).isEqualTo(2);
        assertThat(rows.get(1).getLong(2)).isEqualTo(6); // 2 + 4
    }

    @Test
    void shouldHandleComplexTransformation() {
        // Create test data
        spark.sql("CREATE OR REPLACE TEMP VIEW orders AS " +
                "SELECT * FROM VALUES " +
                "(1, 'user1', 100.0, '2024-01-01'), " +
                "(2, 'user1', 200.0, '2024-01-02'), " +
                "(3, 'user2', 150.0, '2024-01-01'), " +
                "(4, 'user2', 50.0, '2024-01-03') " +
                "AS t(order_id, user_id, amount, order_date)");

        // Complex query with window function
        var result = spark.sql(
                "SELECT " +
                        "  user_id, " +
                        "  SUM(amount) as total_amount, " +
                        "  AVG(amount) as avg_amount, " +
                        "  COUNT(*) as order_count " +
                        "FROM orders " +
                        "GROUP BY user_id " +
                        "ORDER BY total_amount DESC");

        var rows = result.collectAsList();
        assertThat(rows).hasSize(2);

        // user1 has higher total
        assertThat(rows.get(0).getString(0)).isEqualTo("user1");
        assertThat(rows.get(0).getDouble(1)).isEqualTo(300.0);
    }

    @Test
    void sparkUIUrlShouldBeAccessible() {
        String sparkUIUrl = sparkContainer.getSparkUIUrl();
        assertThat(sparkUIUrl).contains("http://");
        assertThat(sparkUIUrl).contains(String.valueOf(sparkContainer.getMappedPort(SparkContainer.SPARK_UI_PORT)));
    }
}
