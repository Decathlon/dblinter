import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

from dblinter.database_connection import DatabaseConnection, log_psycopg2_exception


def test_connection_superuser() -> None:
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
            "user": postgres.POSTGRES_USER,
            "password": postgres.POSTGRES_PASSWORD,
            "host": postgres.get_container_host_ip(),
            "port": postgres.get_exposed_port("5432"),
            "dbname": postgres.POSTGRES_DB,
            "sslmode": "disable",
        }
        # uri = f"postgresql://{postgres.POSTGRES_USER}:{postgres.POSTGRES_PASSWORD}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port('5432')}/{postgres.POSTGRES_DB}"
        db = DatabaseConnection(uri)
        result = db.query("SELECT rolsuper FROM pg_roles pr WHERE rolname=current_user")
        assert result[0][0] is True
        db.close()


def test_log_psycopg2_exception(caplog) -> None:
    err = psycopg2.IntegrityError
    log_psycopg2_exception(err)
    error_message = [
        "\npsycopg2 ERROR: <class 'psycopg2.IntegrityError'>",
        "pgcode: <member 'pgcode' of 'psycopg2.Error' objects>",
    ]
    assert error_message == [rec.message for rec in caplog.records]


def test_query_return_exception():
    # try to execute a querywith the connection closed must return an exception
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
            "user": postgres.POSTGRES_USER,
            "password": postgres.POSTGRES_PASSWORD,
            "host": postgres.get_container_host_ip(),
            "port": postgres.get_exposed_port("5432"),
            "dbname": postgres.POSTGRES_DB,
            "sslmode": "disable",
        }
        db = DatabaseConnection(uri)

        with pytest.raises(psycopg2.ProgrammingError):
            db.query(
                "SELECT bad_column_name FROM pg_roles pr WHERE rolname=current_user"
            )
        db.close()


def test_unable_to_connect():
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
            "user": postgres.POSTGRES_USER,
            "password": postgres.POSTGRES_PASSWORD,
            "host": postgres.get_container_host_ip(),
            "port": postgres.get_exposed_port("5432"),
            "dbname": postgres.POSTGRES_DB,
            "sslmode": "",
        }
        with pytest.raises(psycopg2.OperationalError):
            DatabaseConnection(uri)


def test_query_without_a_connection():
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
            "user": postgres.POSTGRES_USER,
            "password": postgres.POSTGRES_PASSWORD,
            "host": postgres.get_container_host_ip(),
            "port": postgres.get_exposed_port("5432"),
            "dbname": postgres.POSTGRES_DB,
            "sslmode": "disable",
        }
        db = DatabaseConnection(uri)
        db.close()
        with pytest.raises(psycopg2.InterfaceError):
            db.query("SELECT rolsuper FROM pg_roles")


def test_close_connection():
    pass
