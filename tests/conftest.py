"""Test conftest."""

import pytest
import time
import psycopg2
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
    return manage_postgres(request, postgres)

def wait_for_postgres(uri, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(
                user=uri["user"],
                password=uri["password"],
                host=uri["host"],
                port=uri["port"],
                dbname=uri["dbname"],
                sslmode=uri["sslmode"],
            )
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Postgres did not become ready in time")

def manage_postgres(request, pg) -> dict[str, str | int]:
    pg.start()


    def remove_container():
        pg.stop()

    request.addfinalizer(remove_container)
    uri = {
        "user": pg.username,
        "password": pg.password,
        "host": pg.get_container_host_ip(),
        "port": pg.get_exposed_port(5432),
        "dbname": pg.dbname,
        "sslmode": "disable",
    }
    wait_for_postgres(uri,30)
    return uri
