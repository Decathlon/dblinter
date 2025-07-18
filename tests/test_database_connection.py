import psycopg2
import pytest

from dblinter.database_connection import DatabaseConnection, log_psycopg2_exception


def test_connection_superuser(postgres_instance_args) -> None:
    db = DatabaseConnection(postgres_instance_args)
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


def test_query_return_exception(postgres_instance_args):
    # try to execute a querywith the connection closed must return an exception
    db = DatabaseConnection(postgres_instance_args)

    with pytest.raises(psycopg2.ProgrammingError):
        db.query("SELECT bad_column_name FROM pg_roles pr WHERE rolname=current_user")
    db.close()


def test_unable_to_connect(postgres_instance_args):
    uri = postgres_instance_args.copy()
    uri["password"] = "random_password"
    uri["ssl"] = "nothing"
    with pytest.raises(psycopg2.OperationalError):
        DatabaseConnection(uri)


def test_query_without_a_connection(postgres_instance_args):
    db = DatabaseConnection(postgres_instance_args)
    db.close()
    with pytest.raises(psycopg2.InterfaceError):
        db.query("SELECT rolsuper FROM pg_roles")


def test_close_connection():
    pass
