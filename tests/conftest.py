"""Test conftest."""

import pytest
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.postgres import PostgresContainer

PG_IMAGE = "registry.gitlab.com/dalibo/postgresql_anonymizer:latest"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DBNAME = "postgres"
PG_DRIVER = "psycopg2"
LOGLEVEL = "WARNING"


@wait_container_is_ready()
@pytest.fixture(name="postgres_instance_args", scope="session")
def get_pg_instance():
    """Create a single pg instance."""
    with PostgresContainer(
        image=PG_IMAGE,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DBNAME,
        driver=PG_DRIVER,
    ) as postgres:
        uri = {
            "user": postgres.POSTGRES_USER,
            "password": postgres.POSTGRES_PASSWORD,
            "host": postgres.get_container_host_ip(),
            "port": postgres.get_exposed_port("5432"),
            "dbname": postgres.POSTGRES_DB,
            "sslmode": "disable",
        }
        yield uri
