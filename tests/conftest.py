"""Test conftest."""

import time

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

# PG_IMAGE = "registry.gitlab.com/dalibo/postgresql_anonymizer:latest"
PG_IMAGE = "postgres:14"
PG_IMAGE_12 = "postgres:12"
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

postgres_12 = PostgresContainer(
    image=PG_IMAGE_12,
    port=PG_PORT,
    username=PG_USER,
    password=PG_PASSWORD,
    dbname=PG_DBNAME,
    driver=PG_DRIVER,
)


@pytest.fixture(name="postgres_instance_args", scope="session", autouse=True)
def setup(request):
    return manage_postgres(request, postgres)


@pytest.fixture(name="postgres12_instance_args", scope="session", autouse=True)
def setup12(request):
    return manage_postgres(request, postgres_12)


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
        except psycopg2.OperationalError:
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
    wait_for_postgres(uri, 30)
    return uri


@pytest.fixture(scope="function", autouse=True)
def cleanup_database(request):
    """Clean up database after each test to prevent test pollution.

    This fixture runs automatically for all tests that use postgres databases.
    """
    # Only run cleanup if the test uses a postgres fixture
    uses_pg = "postgres_instance_args" in request.fixturenames
    uses_pg12 = "postgres12_instance_args" in request.fixturenames

    if not uses_pg and not uses_pg12:
        yield
        return

    yield
    # Cleanup after test

    # Determine which instance to clean
    instance_args = None
    if uses_pg:
        instance_args = request.getfixturevalue("postgres_instance_args")
    elif uses_pg12:
        instance_args = request.getfixturevalue("postgres12_instance_args")

    if not instance_args:
        return

    try:
        conn = psycopg2.connect(
            user=instance_args["user"],
            password=instance_args["password"],
            host=instance_args["host"],
            port=instance_args["port"],
            dbname=instance_args["dbname"],
            sslmode=instance_args["sslmode"],
            connect_timeout=10,
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Drop all user-created schemas (except public)
        cur.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public',
                                      'pg_toast', '_timescaledb_catalog', '_timescaledb_config',
                                      '_timescaledb_internal', '_timescaledb_cache', '_timescaledb_functions',
                                      'timescaledb', 'pgaudit')
        """
        )
        schemas = cur.fetchall()
        for (schema,) in schemas:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

        # Drop all tables in public schema
        cur.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
        """
        )
        tables = cur.fetchall()
        for (table,) in tables:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')

        # Drop all user-created roles
        cur.execute(
            """
            SELECT rolname
            FROM pg_roles
            WHERE rolname NOT LIKE 'pg_%'
            AND rolname != 'postgres'
        """
        )
        roles = cur.fetchall()
        for (role,) in roles:
            # Revoke all privileges first
            cur.execute(f'REASSIGN OWNED BY "{role}" TO postgres')
            cur.execute(f'DROP OWNED BY "{role}" CASCADE')
            cur.execute(f'DROP ROLE IF EXISTS "{role}"')

        cur.close()
        conn.close()
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Cleanup warning: {e}")
