"""
Feature Engineering Pipeline using Spark Connect

This example demonstrates how to use Spark Connect in a Metaflow pipeline
for feature engineering workloads. The pipeline connects to a team-specific
Spark Connect cluster through Spark Gateway.

Prerequisites:
    pip install metaflow pyspark[connect]

Usage:
    python feature_pipeline.py run

    # With specific environment
    SPARK_CONNECT_ENV=prod python feature_pipeline.py run
"""

import os
from metaflow import FlowSpec, step, Parameter, resources, kubernetes

# Import our Spark Connect client helper
import sys
sys.path.insert(0, '../../spark-connect-client/python')
from spark_connect_client import SparkConnectClient


class FeatureEngineeringFlow(FlowSpec):
    """
    A Metaflow pipeline that uses Spark Connect for feature engineering.

    This demonstrates the recommended pattern for ML teams at DoorDash:
    1. Connect to Spark via Spark Gateway
    2. Read data from Unity Catalog tables
    3. Transform data using Spark DataFrames
    4. Export features for model training
    """

    # Parameters
    environment = Parameter(
        'environment',
        help='Environment (dev, staging, prod)',
        default='dev'
    )

    region = Parameter(
        'region',
        help='AWS region for Spark cluster',
        default='us-west-2'
    )

    date_partition = Parameter(
        'date',
        help='Date partition to process (YYYY-MM-DD)',
        default='2024-01-01'
    )

    @step
    def start(self):
        """
        Initialize the pipeline and connect to Spark.
        """
        print(f"Starting feature pipeline for {self.date_partition}")
        print(f"Environment: {self.environment}")
        print(f"Region: {self.region}")

        self.next(self.extract_raw_data)

    @kubernetes(cpu=2, memory=4096)
    @step
    def extract_raw_data(self):
        """
        Connect to Spark and extract raw data from Unity Catalog.
        """
        # Create Spark session connected to team's cluster
        spark = SparkConnectClient.create_session(
            team="feature-engineering",
            environment=self.environment,
            region=self.region,
            app_name=f"feature-pipeline-{self.date_partition}"
        )

        try:
            # Read raw events from Unity Catalog
            self.raw_events_count = spark.sql(f"""
                SELECT COUNT(*) as cnt
                FROM pedregal.raw.events
                WHERE ds = '{self.date_partition}'
            """).collect()[0]['cnt']

            print(f"Found {self.raw_events_count} raw events")

            # Extract user events
            user_events_df = spark.sql(f"""
                SELECT
                    user_id,
                    event_type,
                    event_timestamp,
                    properties
                FROM pedregal.raw.events
                WHERE ds = '{self.date_partition}'
                  AND user_id IS NOT NULL
            """)

            # Cache for reuse
            user_events_df.cache()
            self.user_events_count = user_events_df.count()

            print(f"Extracted {self.user_events_count} user events")

        finally:
            spark.stop()

        self.next(self.compute_user_features)

    @kubernetes(cpu=4, memory=8192)
    @step
    def compute_user_features(self):
        """
        Compute user-level features using Spark.
        """
        spark = SparkConnectClient.create_session(
            team="feature-engineering",
            environment=self.environment,
            region=self.region,
            app_name=f"user-features-{self.date_partition}"
        )

        try:
            # Compute user features
            user_features_df = spark.sql(f"""
                WITH user_events AS (
                    SELECT
                        user_id,
                        event_type,
                        event_timestamp,
                        properties
                    FROM pedregal.raw.events
                    WHERE ds = '{self.date_partition}'
                      AND user_id IS NOT NULL
                )
                SELECT
                    user_id,

                    -- Activity features
                    COUNT(*) as total_events,
                    COUNT(DISTINCT DATE(event_timestamp)) as active_days,

                    -- Event type features
                    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as view_count,
                    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as click_count,
                    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,

                    -- Timing features
                    MIN(event_timestamp) as first_event_time,
                    MAX(event_timestamp) as last_event_time,

                    -- Computed metrics
                    CASE
                        WHEN SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) > 0
                        THEN SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 1.0 /
                             SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END)
                        ELSE 0
                    END as click_through_rate,

                    -- Metadata
                    '{self.date_partition}' as ds

                FROM user_events
                GROUP BY user_id
            """)

            # Get feature stats
            feature_stats = user_features_df.describe().toPandas()
            print("Feature Statistics:")
            print(feature_stats.to_string())

            # Write to feature store
            user_features_df.writeTo("pedregal.feature_store.user_features") \
                .option("partitionOverwriteMode", "dynamic") \
                .overwritePartitions()

            self.features_written = user_features_df.count()
            print(f"Wrote {self.features_written} user features")

        finally:
            spark.stop()

        self.next(self.compute_item_features)

    @kubernetes(cpu=4, memory=8192)
    @step
    def compute_item_features(self):
        """
        Compute item-level features using Spark.
        """
        spark = SparkConnectClient.create_session(
            team="feature-engineering",
            environment=self.environment,
            region=self.region,
            app_name=f"item-features-{self.date_partition}"
        )

        try:
            # Compute item features
            item_features_df = spark.sql(f"""
                WITH item_events AS (
                    SELECT
                        get_json_object(properties, '$.item_id') as item_id,
                        event_type,
                        user_id,
                        event_timestamp
                    FROM pedregal.raw.events
                    WHERE ds = '{self.date_partition}'
                      AND get_json_object(properties, '$.item_id') IS NOT NULL
                )
                SELECT
                    item_id,

                    -- Engagement features
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(*) as total_interactions,

                    -- Event breakdown
                    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
                    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
                    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as adds_to_cart,
                    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,

                    -- Conversion metrics
                    CASE
                        WHEN SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) > 0
                        THEN SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 1.0 /
                             SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END)
                        ELSE 0
                    END as conversion_rate,

                    -- Metadata
                    '{self.date_partition}' as ds

                FROM item_events
                GROUP BY item_id
            """)

            # Write to feature store
            item_features_df.writeTo("pedregal.feature_store.item_features") \
                .option("partitionOverwriteMode", "dynamic") \
                .overwritePartitions()

            self.item_features_written = item_features_df.count()
            print(f"Wrote {self.item_features_written} item features")

        finally:
            spark.stop()

        self.next(self.validate_features)

    @kubernetes(cpu=2, memory=4096)
    @step
    def validate_features(self):
        """
        Validate the computed features.
        """
        spark = SparkConnectClient.create_session(
            team="feature-engineering",
            environment=self.environment,
            region=self.region,
            app_name=f"feature-validation-{self.date_partition}"
        )

        try:
            # Validate user features
            user_validation = spark.sql(f"""
                SELECT
                    COUNT(*) as row_count,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN total_events <= 0 THEN 1 ELSE 0 END) as invalid_events,
                    SUM(CASE WHEN click_through_rate < 0 OR click_through_rate > 1 THEN 1 ELSE 0 END) as invalid_ctr
                FROM pedregal.feature_store.user_features
                WHERE ds = '{self.date_partition}'
            """).collect()[0]

            self.validation_results = {
                'user_row_count': user_validation['row_count'],
                'unique_users': user_validation['unique_users'],
                'invalid_events': user_validation['invalid_events'],
                'invalid_ctr': user_validation['invalid_ctr'],
            }

            # Check for data quality issues
            if user_validation['invalid_events'] > 0:
                raise ValueError(f"Found {user_validation['invalid_events']} rows with invalid events")

            if user_validation['invalid_ctr'] > 0:
                raise ValueError(f"Found {user_validation['invalid_ctr']} rows with invalid CTR")

            print("Feature validation passed!")
            print(f"  User rows: {user_validation['row_count']}")
            print(f"  Unique users: {user_validation['unique_users']}")

        finally:
            spark.stop()

        self.next(self.end)

    @step
    def end(self):
        """
        Pipeline complete.
        """
        print("=" * 50)
        print("Feature Engineering Pipeline Complete!")
        print("=" * 50)
        print(f"Date: {self.date_partition}")
        print(f"Raw events processed: {self.raw_events_count}")
        print(f"User features written: {self.features_written}")
        print(f"Item features written: {self.item_features_written}")
        print(f"Validation: PASSED")


if __name__ == '__main__':
    FeatureEngineeringFlow()
