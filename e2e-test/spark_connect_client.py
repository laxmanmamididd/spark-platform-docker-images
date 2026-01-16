#!/usr/bin/env python3
"""
Spark Connect Client - End-to-End Test

This script demonstrates connecting to a Spark Connect server and running queries.
It can connect to:
1. Local Docker-based Spark Connect server
2. Remote Spark Gateway (e.g., via port-forward)
3. Direct Spark Connect endpoints

Usage:
    python spark_connect_client.py [--host HOST] [--port PORT]

Prerequisites:
    pip install pyspark[connect]
"""

import argparse
import sys
import time
from typing import Optional

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, rand
except ImportError:
    print("Error: pyspark[connect] not installed.")
    print("Install with: pip install 'pyspark[connect]'")
    sys.exit(1)


def create_spark_session(host: str = "localhost", port: int = 15002) -> SparkSession:
    """Create a Spark Connect session."""
    remote_url = f"sc://{host}:{port}"
    print(f"Connecting to Spark Connect at: {remote_url}")

    spark = SparkSession.builder \
        .remote(remote_url) \
        .getOrCreate()

    return spark


def test_basic_operations(spark: SparkSession) -> bool:
    """Run basic Spark operations to verify connectivity."""
    print("\n" + "=" * 60)
    print("RUNNING SPARK CONNECT END-TO-END TESTS")
    print("=" * 60)

    # Test 1: Simple SQL query
    print("\n[TEST 1] Simple SQL Query...")
    try:
        result = spark.sql("SELECT 1 as test_value, 'hello' as test_string")
        result.show()
        print("[PASS] SQL query executed successfully")
    except Exception as e:
        print(f"[FAIL] SQL query failed: {e}")
        return False

    # Test 2: Create DataFrame from data
    print("\n[TEST 2] Create DataFrame from Python data...")
    try:
        data = [
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Charlie", 300),
        ]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.show()
        print(f"[PASS] Created DataFrame with {df.count()} rows")
    except Exception as e:
        print(f"[FAIL] DataFrame creation failed: {e}")
        return False

    # Test 3: DataFrame transformations
    print("\n[TEST 3] DataFrame Transformations...")
    try:
        result_df = df.filter(col("value") > 100) \
                      .withColumn("doubled", col("value") * 2) \
                      .orderBy("value")
        result_df.show()
        print("[PASS] Transformations executed successfully")
    except Exception as e:
        print(f"[FAIL] Transformations failed: {e}")
        return False

    # Test 4: Aggregations
    print("\n[TEST 4] Aggregations...")
    try:
        agg_result = df.agg({"value": "sum", "id": "count"})
        agg_result.show()
        print("[PASS] Aggregations executed successfully")
    except Exception as e:
        print(f"[FAIL] Aggregations failed: {e}")
        return False

    # Test 5: Spark version
    print("\n[TEST 5] Spark Version...")
    try:
        version = spark.version
        print(f"[PASS] Connected to Spark version: {version}")
    except Exception as e:
        print(f"[FAIL] Could not get Spark version: {e}")
        return False

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60)
    return True


def run_pi_calculation(spark: SparkSession, num_samples: int = 100000) -> float:
    """
    Calculate Pi using Monte Carlo simulation.
    This is a common Spark benchmark test.
    """
    print(f"\n[PI CALCULATION] Running Monte Carlo with {num_samples} samples...")

    # Create DataFrame with random x, y coordinates
    df = spark.range(num_samples) \
        .withColumn("x", rand()) \
        .withColumn("y", rand()) \
        .withColumn("inside_circle", (col("x") ** 2 + col("y") ** 2) <= 1)

    # Count points inside the unit circle
    inside_count = df.filter(col("inside_circle")).count()

    # Pi = 4 * (inside / total)
    pi_estimate = 4.0 * inside_count / num_samples

    print(f"[RESULT] Estimated Pi: {pi_estimate}")
    print(f"[RESULT] Actual Pi:    {3.14159265359}")
    print(f"[RESULT] Difference:   {abs(pi_estimate - 3.14159265359):.6f}")

    return pi_estimate


def main():
    parser = argparse.ArgumentParser(description="Spark Connect Client Test")
    parser.add_argument("--host", default="localhost", help="Spark Connect server host")
    parser.add_argument("--port", type=int, default=15002, help="Spark Connect server port")
    parser.add_argument("--pi", action="store_true", help="Run Pi calculation benchmark")
    parser.add_argument("--samples", type=int, default=100000, help="Number of samples for Pi calculation")
    args = parser.parse_args()

    print("=" * 60)
    print("SPARK CONNECT CLIENT")
    print("=" * 60)

    # Wait for server to be ready (useful when starting with docker-compose)
    max_retries = 30
    for i in range(max_retries):
        try:
            spark = create_spark_session(args.host, args.port)
            break
        except Exception as e:
            if i < max_retries - 1:
                print(f"Connection attempt {i + 1}/{max_retries} failed, retrying...")
                time.sleep(2)
            else:
                print(f"Failed to connect after {max_retries} attempts: {e}")
                sys.exit(1)

    print(f"Successfully connected to Spark Connect server!")

    # Run basic tests
    success = test_basic_operations(spark)

    # Run Pi calculation if requested
    if args.pi:
        run_pi_calculation(spark, args.samples)

    # Clean up
    spark.stop()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
