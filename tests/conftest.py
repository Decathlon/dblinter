"""Test conftest."""

import pytest
from testcontainers.postgres import PostgresContainer

# PG_IMAGE = "registry.gitlab.com/dalibo/postgresql_anonymizer:latest"
PG_IMAGE = "postgres:14"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DBNAME = "postgres"
PG_DRIVER = "psycopg2"
LOGLEVEL = "WARNING"

postgres = PostgresContainer(
    image=PG_IMAGE,
    port=PG_PORT,
    username=PG_USER,
    password=PG_PASSWORD,
    dbname=PG_DBNAME,
    driver=PG_DRIVER,
)


@pytest.fixture(name="postgres_instance_args", scope="session", autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)
    uri = {
        "user": postgres.username,
        "password": postgres.password,
        "host": postgres.get_container_host_ip(),
        "port": postgres.get_exposed_port(5432),
        "dbname": postgres.dbname,
        "sslmode": "disable",
    }
    return uri
