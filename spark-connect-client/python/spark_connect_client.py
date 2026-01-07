"""
Spark Connect Client for DoorDash Pedregal Platform

This module provides a standardized way to connect to Spark Connect clusters
through Spark Gateway. Teams use the open-source pyspark[connect] library
with DoorDash-specific configuration helpers.

Usage:
    pip install pyspark[connect]

    from spark_connect_client import SparkConnectClient

    # Connect to your team's cluster
    spark = SparkConnectClient.create_session(
        team="feature-engineering",
        environment="prod",
        region="us-west-2"
    )

    # Use Spark as normal
    df = spark.table("pedregal.feature_store.user_features")
"""

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum


class Environment(Enum):
    DEV = "dev"
    STAGING = "staging"
    CI = "ci"
    PROD = "prod"


class Region(Enum):
    US_WEST_2 = "us-west-2"
    US_EAST_1 = "us-east-1"
    EU_WEST_1 = "eu-west-1"


@dataclass
class SparkConnectConfig:
    """Configuration for Spark Connect connection."""
    team: str
    environment: Environment
    region: Region
    gateway_host: str = "spark-gateway.doordash.team"
    gateway_port: int = 15002
    use_ssl: bool = True
    auth_token: Optional[str] = None

    @property
    def endpoint(self) -> str:
        """
        Generate the Spark Connect endpoint URL.

        Format: sc://[team]-[env]-[region].doordash.team:15002

        Examples:
            sc://feature-eng-prod-us-west-2.doordash.team:15002
            sc://ml-platform-dev-eu-west-1.doordash.team:15002
        """
        team_short = self.team.replace("-", "")[:10]
        env_short = self.environment.value[:3]
        region_short = self.region.value.replace("-", "")

        subdomain = f"{team_short}-{env_short}-{region_short}"
        return f"sc://{subdomain}.doordash.team:{self.gateway_port}"

    @property
    def direct_endpoint(self) -> str:
        """Direct endpoint through Spark Gateway with headers."""
        return f"sc://{self.gateway_host}:{self.gateway_port}"


class SparkConnectClient:
    """
    Factory for creating Spark Connect sessions connected to DoorDash clusters.

    This class provides helper methods to create properly configured SparkSession
    instances that connect through Spark Gateway to team-specific clusters.
    """

    # Default gateway endpoints by region
    GATEWAY_ENDPOINTS = {
        Region.US_WEST_2: "spark-gateway-us-west-2.doordash.team",
        Region.US_EAST_1: "spark-gateway-us-east-1.doordash.team",
        Region.EU_WEST_1: "spark-gateway-eu-west-1.doordash.team",
    }

    @classmethod
    def create_session(
        cls,
        team: str,
        environment: str = "dev",
        region: str = "us-west-2",
        app_name: Optional[str] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Create a SparkSession connected to the team's Spark Connect cluster.

        Args:
            team: Team name (e.g., "feature-engineering", "ml-platform")
            environment: Environment name (dev, staging, ci, prod)
            region: AWS region (us-west-2, us-east-1, eu-west-1)
            app_name: Optional application name for the Spark session
            extra_config: Optional additional Spark configuration

        Returns:
            SparkSession connected to the remote Spark Connect cluster

        Example:
            spark = SparkConnectClient.create_session(
                team="feature-engineering",
                environment="prod",
                region="us-west-2"
            )
            df = spark.sql("SELECT * FROM pedregal.raw.events LIMIT 10")
        """
        from pyspark.sql import SparkSession

        # Parse environment and region
        env = Environment(environment)
        reg = Region(region)

        # Create config
        config = SparkConnectConfig(
            team=team,
            environment=env,
            region=reg,
            gateway_host=cls.GATEWAY_ENDPOINTS.get(reg, cls.GATEWAY_ENDPOINTS[Region.US_WEST_2]),
        )

        # Get auth token from environment or credential provider
        auth_token = os.environ.get("SPARK_CONNECT_AUTH_TOKEN")
        if auth_token:
            config.auth_token = auth_token

        # Build SparkSession
        builder = SparkSession.builder

        # Set remote endpoint
        builder = builder.remote(config.endpoint)

        # Set app name
        if app_name:
            builder = builder.appName(app_name)
        else:
            builder = builder.appName(f"{team}-{environment}-session")

        # Apply extra config
        if extra_config:
            for key, value in extra_config.items():
                builder = builder.config(key, value)

        return builder.getOrCreate()

    @classmethod
    def create_session_from_env(cls):
        """
        Create a SparkSession using environment variables.

        Environment variables:
            SPARK_CONNECT_TEAM: Team name
            SPARK_CONNECT_ENV: Environment (dev, staging, ci, prod)
            SPARK_CONNECT_REGION: Region (us-west-2, us-east-1, eu-west-1)
            SPARK_CONNECT_AUTH_TOKEN: (Optional) Auth token

        Returns:
            SparkSession connected to the remote cluster
        """
        team = os.environ.get("SPARK_CONNECT_TEAM")
        environment = os.environ.get("SPARK_CONNECT_ENV", "dev")
        region = os.environ.get("SPARK_CONNECT_REGION", "us-west-2")

        if not team:
            raise ValueError("SPARK_CONNECT_TEAM environment variable is required")

        return cls.create_session(team=team, environment=environment, region=region)

    @classmethod
    def create_session_direct(cls, endpoint: str, app_name: Optional[str] = None):
        """
        Create a SparkSession with a direct endpoint URL.

        Args:
            endpoint: Full Spark Connect URL (e.g., sc://feature-eng.doordash.team:15002)
            app_name: Optional application name

        Returns:
            SparkSession connected to the specified endpoint
        """
        from pyspark.sql import SparkSession

        builder = SparkSession.builder.remote(endpoint)

        if app_name:
            builder = builder.appName(app_name)

        return builder.getOrCreate()


# Convenience functions for common use cases

def get_spark_session(
    team: str,
    environment: str = "dev",
    region: str = "us-west-2",
) -> "SparkSession":
    """
    Convenience function to get a Spark session.

    Example:
        from spark_connect_client import get_spark_session

        spark = get_spark_session("feature-engineering", "prod")
        df = spark.table("pedregal.feature_store.user_features")
    """
    return SparkConnectClient.create_session(
        team=team,
        environment=environment,
        region=region,
    )


def get_feature_engineering_spark(environment: str = "dev") -> "SparkSession":
    """Get a Spark session for the Feature Engineering team."""
    return SparkConnectClient.create_session(
        team="feature-engineering",
        environment=environment,
        region="us-west-2",
    )


def get_ml_platform_spark(environment: str = "dev") -> "SparkSession":
    """Get a Spark session for the ML Platform team."""
    return SparkConnectClient.create_session(
        team="ml-platform",
        environment=environment,
        region="us-west-2",
    )


if __name__ == "__main__":
    # Example usage
    print("Spark Connect Client for DoorDash")
    print("=" * 50)

    # Show example endpoint
    config = SparkConnectConfig(
        team="feature-engineering",
        environment=Environment.PROD,
        region=Region.US_WEST_2,
    )
    print(f"Example endpoint: {config.endpoint}")

    # Show usage
    print("\nUsage:")
    print("  from spark_connect_client import get_spark_session")
    print("  spark = get_spark_session('feature-engineering', 'prod')")
    print("  df = spark.table('pedregal.feature_store.user_features')")
