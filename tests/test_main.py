import json
import tempfile
from os.path import exists
from unittest.mock import Mock, patch

import psycopg2
import pytest
from pydantic_yaml import parse_yaml_raw_as
from testcontainers.postgres import PostgresContainer

from dblinter.configuration import Configuration
from dblinter.configuration_model import ConfigurationModel
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument
from dblinter.scan import (
    dblinter,
    perform_schema_check,
    perform_table_check,
    save_report,
)


def test_dblinter_start() -> None:
    """Test --Simple check to prove dblinter can run
    step:
    1 - call __main__
    result: should_pass
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "mytestdb"
    ) as postgres:
        temp_name = tempfile.mktemp()
        dblinter(
            user="postgres",
            password="postgres",
            host="localhost",
            port=postgres.get_exposed_port(5432),
            dbname="mytestdb",
            output=temp_name,
            filename="",
            append=False,
        )
        assert exists(temp_name)


def test_dblinter_with_wrong_pg_connection() -> None:
    """Test --Simple check to prove dblinter can run on a failing connection
    step:
    1 - call __main__
    result: should_fail
    """
    with PostgresContainer("postgres:14", 5432, "postgres", "postgres", "mytestdb"):
        temp_name = tempfile.mktemp()
        with pytest.raises(psycopg2.OperationalError):
            dblinter(
                user="postgres",
                password="postgres",
                host="localhost",
                port="1234",
                dbname="mytestdb",
                output=temp_name,
                append=False,
            )


def test_save_report_to_disk() -> None:
    """Test --Wrie output report to disk
    step:
    1 - call save_report
    result: should_pass
    """
    source = {
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "dblinter",
                        "informationUri": "https://github.com/decathlon/dblinter",
                        "version": "0.1",
                    }
                },
                "results": [],
            },
            {
                "tool": {
                    "driver": {
                        "name": "dblinter",
                        "informationUri": "https://github.com/decathlon/dblinter",
                        "version": "0.1",
                    }
                },
                "results": [],
            },
        ],
        "version": "2.1.0",
        "$schema": "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0-rtm.5.json",
    }
    output = tempfile.mktemp()
    save_report(output=output, content=json.dumps(source), append=False)
    save_report(output=output, content=json.dumps(source), append=True)
    assert exists(output)


@patch("dblinter.scan.Client")
def test_append_report_to_bucket(client) -> None:
    """Test --append output report to disk
    step:
    1 - call save_report
    result: should_pass
    """
    output = "gs://bucketname/test.sarif"
    bucket = client().get_bucket
    source = {
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "dblinter",
                        "informationUri": "https://github.com/decathlon/dblinter",
                        "version": "0.1",
                    }
                },
                "results": [],
            },
            {
                "tool": {
                    "driver": {
                        "name": "dblinter",
                        "informationUri": "https://github.com/decathlon/dblinter",
                        "version": "0.1",
                    }
                },
                "results": [],
            },
        ],
        "version": "2.1.0",
        "$schema": "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0-rtm.5.json",
    }
    mock_bucket = Mock()
    mock_bucket.blob.return_value.download_as_string.return_value = json.dumps(
        source
    ).encode("utf-8")
    bucket.return_value = mock_bucket
    blob = bucket().blob
    save_report(output=output, content=json.dumps(source), append=True)
    bucket.assert_called_with("bucketname")
    blob = bucket().blob
    blob.assert_called_with("test.sarif")


@patch("dblinter.scan.Client")
def test_save_report_to_bucket(client) -> None:
    """Test --Wrie output report to disk
    step:
    1 - call save_report
    result: should_pass
    """
    output = "gs://bucketname/test.sarif"
    bucket = client().get_bucket
    source = {
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "dblinter",
                        "informationUri": "https://github.com/decathlon/dblinter",
                        "version": "0.1",
                    }
                },
                "results": [],
            },
            {
                "tool": {
                    "driver": {
                        "name": "dblinter",
                        "informationUri": "https://github.com/decathlon/dblinter",
                        "version": "0.1",
                    }
                },
                "results": [],
            },
        ],
        "version": "2.1.0",
        "$schema": "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0-rtm.5.json",
    }
    save_report(output=output, content=json.dumps(source), append=False)
    bucket.assert_called_with("bucketname")
    blob = bucket().blob
    blob.assert_called_with("test.sarif")


def test_main_ouput_null() -> None:
    """Test --Simple check to prove dblinter can run
    step:
    1 - call __main__
    result: should_pass
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "mytestdb"
    ) as postgres:
        dblinter(
            user="postgres",
            password="postgres",
            host="localhost",
            port=postgres.get_exposed_port(5432),
            dbname="mytestdb",
        )
        assert True


def test_main_filename_empty() -> None:
    """Test --Simple check to prove dblinter can run
    step:
    1 - call __main__
    result: should_pass
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "mytestdb"
    ) as postgres:
        dblinter(
            user="postgres",
            password="postgres",
            host="localhost",
            port=postgres.get_exposed_port(5432),
            dbname="mytestdb",
        )
        assert True


def test_main_with_schema_only_ok() -> None:
    """Test --schema-only with schema1.
    create 2 tables 1 in public and 1 in schema1, only the table in schema1 should be checked
    because --schema-only=schema1
    step:
    1 - call perform_table_check
    result: should_pass
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        function_library = FunctionLibrary()
        uri = {
            "user": postgres.POSTGRES_USER,
            "password": postgres.POSTGRES_PASSWORD,
            "host": postgres.get_container_host_ip(),
            "port": postgres.get_exposed_port('5432'),
            "dbname": postgres.POSTGRES_DB,
            "sslmode": "disable"
        }
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
        db.query("CREATE SCHEMA SCHEMA1")
        db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
        db.query("CREATE TABLE public.e_table1 (id integer, field1 text)")
        perform_table_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            schema="schema1",
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[0].message.text
            == "No primary key on table postgres.schema1.e_table1."
        )


def test_main_with_schema_only_not_exists() -> None:
    """Test --schema-only with schema1. but table is created in public
    and schema1 does not exist
    step:
    1 - call perform_table_check
    result: test is empty
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
                    "user": postgres.POSTGRES_USER,
                    "password": postgres.POSTGRES_PASSWORD,
                    "host": postgres.get_container_host_ip(),
                    "port": postgres.get_exposed_port('5432'),
                    "dbname": postgres.POSTGRES_DB,
                    "sslmode": "disable"
                }
        function_library = FunctionLibrary()
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
        db.query("CREATE TABLE public.e_table1 (id integer, field1 text)")
        perform_table_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            schema="schema1",
        )
        assert sarif_document.sarif_doc.runs[0].results == []


def test_main_with_include_table() -> None:
    """Test --include-tables. create 2 tables in schema1, 1 in public look for e_table% everywhere
    step:
    1 - call perform_table_check
    result: test is ok
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
                    "user": postgres.POSTGRES_USER,
                    "password": postgres.POSTGRES_PASSWORD,
                    "host": postgres.get_container_host_ip(),
                    "port": postgres.get_exposed_port('5432'),
                    "dbname": postgres.POSTGRES_DB,
                    "sslmode": "disable"
                }
        function_library = FunctionLibrary()
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
        db.query("CREATE SCHEMA SCHEMA1")
        db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
        db.query("CREATE TABLE schema1.e_table2 (id integer, field1 text)")
        db.query("CREATE TABLE public.e_table1 (id integer, field1 text)")
        perform_table_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            include="e_table%",
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[0].message.text
            == "No primary key on table postgres.schema1.e_table1."
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[1].message.text
            == "No index on table postgres.schema1.e_table1."
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[3].message.text
            == "No primary key on table postgres.schema1.e_table2."
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[4].message.text
            == "No index on table postgres.schema1.e_table2."
        )


def test_main_with_include_table_and_schema() -> None:
    """Test --include-tables. create 2 tables in schema1, 1 in public look for e_table% in schema1 only.

    step:
    1 - call perform_table_check
    result: test is ok
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
                    "user": postgres.POSTGRES_USER,
                    "password": postgres.POSTGRES_PASSWORD,
                    "host": postgres.get_container_host_ip(),
                    "port": postgres.get_exposed_port('5432'),
                    "dbname": postgres.POSTGRES_DB,
                    "sslmode": "disable"
                }
        function_library = FunctionLibrary()
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
        db.query("CREATE SCHEMA SCHEMA1")
        db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
        db.query("CREATE TABLE schema1.e_table2 (id integer, field1 text)")
        db.query("CREATE TABLE public.e_table1 (id integer, field1 text)")
        perform_table_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            include="e_table%",
            schema="schema1",
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[0].message.text
            == "No primary key on table postgres.schema1.e_table1."
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[1].message.text
            == "No index on table postgres.schema1.e_table1."
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[3].message.text
            == "No primary key on table postgres.schema1.e_table2."
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[4].message.text
            == "No index on table postgres.schema1.e_table2."
        )


def test_main_with_exclude_table() -> None:
    """Test --exclude-tables. create 2 tables in schema1, 1 in public exclude all tables e_table2%.
    step:
    1 - call perform_table_check
    result: test is ok
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
                    "user": postgres.POSTGRES_USER,
                    "password": postgres.POSTGRES_PASSWORD,
                    "host": postgres.get_container_host_ip(),
                    "port": postgres.get_exposed_port('5432'),
                    "dbname": postgres.POSTGRES_DB,
                    "sslmode": "disable"
                }
        function_library = FunctionLibrary()
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
        db.query("CREATE SCHEMA SCHEMA1")
        db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
        db.query("CREATE TABLE schema1.e_table2 (id integer, field1 text)")
        db.query("CREATE TABLE public.e_table1 (id integer, field1 text)")
        perform_table_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            exclude="e_table2%",
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[0].message.text
            == "No primary key on table postgres.schema1.e_table1."
        )
        assert (
            sarif_document.sarif_doc.runs[0].results[1].message.text
            == "No index on table postgres.schema1.e_table1."
        )


def test_main_with_schema_and_role_without_default_role_nok() -> None:
    """Test --schema-only with schema1.
    create 1 table in schema1, with role schema1_ro granted on schema1
    but without default role for futur table.
    step:
    1 - call perform_schema_check
    result: should_fail
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
                    "user": postgres.POSTGRES_USER,
                    "password": postgres.POSTGRES_PASSWORD,
                    "host": postgres.get_container_host_ip(),
                    "port": postgres.get_exposed_port('5432'),
                    "dbname": postgres.POSTGRES_DB,
                    "sslmode": "disable"
                }
        function_library = FunctionLibrary()
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)

        db.query("CREATE SCHEMA SCHEMA1")
        db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
        db.query("CREATE ROLE schema1_ro")
        db.query("GRANT SELECT ON ALL TABLES IN SCHEMA schema1 to schema1_ro")
        perform_schema_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            schema="schema1",
        )
        assert (
            "No default role grantee on schema postgres.schema1. It means that each time a table is created, you must grant it to roles."
            in sarif_document.sarif_doc.runs[0].results[0].message.text
        )


def test_main_with_schema_and_role_without_default_role_ok() -> None:
    """Test --schema-only with schema1.
    create 1 table in schema1, with role schema1_ro granted on schema1
    and with default role set for futur table.
    step:
    1 - call perform_schema_check
    result: should_pass
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
                    "user": postgres.POSTGRES_USER,
                    "password": postgres.POSTGRES_PASSWORD,
                    "host": postgres.get_container_host_ip(),
                    "port": postgres.get_exposed_port('5432'),
                    "dbname": postgres.POSTGRES_DB,
                    "sslmode": "disable"
                }
        function_library = FunctionLibrary()
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
        db.query("CREATE SCHEMA SCHEMA1")
        db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
        db.query("CREATE ROLE schema1_ro")
        db.query("GRANT SELECT ON ALL TABLES IN SCHEMA schema1 to schema1_ro")
        db.query(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA schema1 for user postgres GRANT SELECT ON TABLES TO schema1_ro"
        )
        perform_schema_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            schema="schema1",
        )
        assert sarif_document.sarif_doc.runs[0].results == []


def test_main_with_schema_and_role_not_exist_nok() -> None:
    """Test --schema-only with schema1.
    create 1 table in schema1, with role created but not granted on schema1
    abd with a default role exists on role schema1_ro
    step:
    1 - call perform_schema_check
    result: should_fail
    """
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "postgres"
    ) as postgres:
        uri = {
                    "user": postgres.POSTGRES_USER,
                    "password": postgres.POSTGRES_PASSWORD,
                    "host": postgres.get_container_host_ip(),
                    "port": postgres.get_exposed_port('5432'),
                    "dbname": postgres.POSTGRES_DB,
                    "sslmode": "disable"
                }
        function_library = FunctionLibrary()
        db = DatabaseConnection(uri)
        sarif_document = SarifDocument()
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            "dblinter", "default_config.yaml"
        )
        configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
        db.query("CREATE SCHEMA SCHEMA1")
        db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
        db.query("CREATE ROLE schema1_ro")
        db.query(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA schema1 for user postgres GRANT SELECT ON TABLES TO schema1_ro"
        )
        perform_schema_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            schema="schema1",
        )

        assert sarif_document.sarif_doc.runs[0].results == []

        perform_table_check(
            function_library=function_library,
            db=db,
            config_file=configuration.config_file,
            sarif_document=sarif_document,
            schema="schema1",
        )

        assert (
            "No role grantee on table postgres.schema1.e_table1. It means that except owner."
            in sarif_document.sarif_doc.runs[0].results[2].message.text
        )
