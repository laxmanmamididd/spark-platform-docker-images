"""
Feature Engineering Tests using Spark Connect

This module contains integration tests for feature engineering pipelines
that run against a Spark Connect cluster in CI/CD environments.

Prerequisites:
    pip install pytest pyspark[connect]

Environment Variables:
    SPARK_CONNECT_TEAM: Team name (default: feature-engineering)
    SPARK_CONNECT_ENV: Environment (default: ci)
    SPARK_CONNECT_REGION: Region (default: us-west-2)

Usage:
    pytest feature_tests.py -v
"""

import os
import pytest
from datetime import datetime, timedelta

# Import our Spark Connect client
import sys
sys.path.insert(0, '../../spark-connect-client/python')
from spark_connect_client import SparkConnectClient


# Test configuration
TEAM = os.environ.get("SPARK_CONNECT_TEAM", "feature-engineering")
ENVIRONMENT = os.environ.get("SPARK_CONNECT_ENV", "ci")
REGION = os.environ.get("SPARK_CONNECT_REGION", "us-west-2")


@pytest.fixture(scope="module")
def spark():
    """
    Create a Spark session for the test module.
    Uses CI environment by default.
    """
    session = SparkConnectClient.create_session(
        team=TEAM,
        environment=ENVIRONMENT,
        region=REGION,
        app_name="feature-tests"
    )
    yield session
    session.stop()


class TestDataAvailability:
    """Tests for data availability in Unity Catalog."""

    def test_can_access_raw_events_table(self, spark):
        """Verify we can access the raw events table."""
        result = spark.sql("DESCRIBE TABLE pedregal.raw.events").collect()
        assert len(result) > 0, "Events table should have columns"

    def test_can_query_raw_events(self, spark):
        """Verify we can query data from raw events."""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        result = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM pedregal.raw.events
            WHERE ds = '{yesterday}'
        """).collect()

        assert result[0]['cnt'] >= 0, "Query should return a count"

    def test_feature_store_database_exists(self, spark):
        """Verify feature store database exists."""
        databases = spark.sql("SHOW DATABASES IN pedregal").collect()
        db_names = [row['namespace'] for row in databases]
        assert 'feature_store' in db_names, "feature_store database should exist"


class TestUserFeatures:
    """Tests for user feature computation."""

    def test_user_features_schema(self, spark):
        """Verify user features table has expected schema."""
        result = spark.sql("DESCRIBE TABLE pedregal.feature_store.user_features").collect()
        column_names = [row['col_name'] for row in result]

        expected_columns = [
            'user_id',
            'total_events',
            'active_days',
            'view_count',
            'click_count',
            'purchase_count',
            'click_through_rate',
            'ds'
        ]

        for col in expected_columns:
            assert col in column_names, f"Column {col} should exist in user_features"

    def test_user_features_not_empty(self, spark):
        """Verify user features table has data."""
        result = spark.sql("""
            SELECT COUNT(*) as cnt
            FROM pedregal.feature_store.user_features
            WHERE ds >= date_sub(current_date(), 7)
        """).collect()

        assert result[0]['cnt'] > 0, "User features should have recent data"

    def test_user_features_no_nulls_in_key_columns(self, spark):
        """Verify no null values in key columns."""
        result = spark.sql("""
            SELECT
                SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) as null_user_id,
                SUM(CASE WHEN ds IS NULL THEN 1 ELSE 0 END) as null_ds
            FROM pedregal.feature_store.user_features
            WHERE ds >= date_sub(current_date(), 7)
        """).collect()

        assert result[0]['null_user_id'] == 0, "user_id should not have nulls"
        assert result[0]['null_ds'] == 0, "ds should not have nulls"

    def test_user_features_ctr_in_valid_range(self, spark):
        """Verify click-through rate is in valid range [0, 1]."""
        result = spark.sql("""
            SELECT
                SUM(CASE WHEN click_through_rate < 0 OR click_through_rate > 1 THEN 1 ELSE 0 END) as invalid_ctr
            FROM pedregal.feature_store.user_features
            WHERE ds >= date_sub(current_date(), 7)
        """).collect()

        assert result[0]['invalid_ctr'] == 0, "CTR should be between 0 and 1"


class TestItemFeatures:
    """Tests for item feature computation."""

    def test_item_features_schema(self, spark):
        """Verify item features table has expected schema."""
        result = spark.sql("DESCRIBE TABLE pedregal.feature_store.item_features").collect()
        column_names = [row['col_name'] for row in result]

        expected_columns = [
            'item_id',
            'unique_users',
            'total_interactions',
            'views',
            'clicks',
            'purchases',
            'conversion_rate',
            'ds'
        ]

        for col in expected_columns:
            assert col in column_names, f"Column {col} should exist in item_features"

    def test_item_features_positive_counts(self, spark):
        """Verify item features have non-negative counts."""
        result = spark.sql("""
            SELECT
                SUM(CASE WHEN unique_users < 0 THEN 1 ELSE 0 END) as negative_users,
                SUM(CASE WHEN total_interactions < 0 THEN 1 ELSE 0 END) as negative_interactions
            FROM pedregal.feature_store.item_features
            WHERE ds >= date_sub(current_date(), 7)
        """).collect()

        assert result[0]['negative_users'] == 0, "unique_users should be non-negative"
        assert result[0]['negative_interactions'] == 0, "total_interactions should be non-negative"


class TestFeatureConsistency:
    """Tests for cross-table consistency."""

    def test_user_event_counts_consistent(self, spark):
        """Verify user event counts are consistent with raw data."""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Count from features table
        feature_count = spark.sql(f"""
            SELECT SUM(total_events) as total
            FROM pedregal.feature_store.user_features
            WHERE ds = '{yesterday}'
        """).collect()[0]['total']

        # Count from raw table
        raw_count = spark.sql(f"""
            SELECT COUNT(*) as total
            FROM pedregal.raw.events
            WHERE ds = '{yesterday}'
              AND user_id IS NOT NULL
        """).collect()[0]['total']

        # Allow for some difference due to processing time
        if feature_count is not None and raw_count is not None:
            diff_pct = abs(feature_count - raw_count) / max(raw_count, 1) * 100
            assert diff_pct < 5, f"Event counts should be within 5%, got {diff_pct:.2f}%"


class TestDataQuality:
    """Data quality tests."""

    def test_no_duplicate_user_features(self, spark):
        """Verify no duplicate user_id + ds combinations."""
        result = spark.sql("""
            SELECT user_id, ds, COUNT(*) as cnt
            FROM pedregal.feature_store.user_features
            WHERE ds >= date_sub(current_date(), 7)
            GROUP BY user_id, ds
            HAVING COUNT(*) > 1
        """).collect()

        assert len(result) == 0, f"Found {len(result)} duplicate user features"

    def test_freshness(self, spark):
        """Verify features are fresh (updated within last 24 hours)."""
        result = spark.sql("""
            SELECT MAX(ds) as latest_ds
            FROM pedregal.feature_store.user_features
        """).collect()

        latest_ds = result[0]['latest_ds']
        if latest_ds:
            # Parse date string
            latest_date = datetime.strptime(str(latest_ds), "%Y-%m-%d")
            days_old = (datetime.now() - latest_date).days
            assert days_old <= 1, f"Features are {days_old} days old, expected <= 1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
