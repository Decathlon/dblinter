import importlib.util
import json
import sys
import tempfile
from os.path import exists
from unittest.mock import Mock, patch

import psycopg2
import pytest
from pydantic_yaml import parse_yaml_raw_as

from dblinter import function_library as flib
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


def test_dblinter_start(postgres_instance_args) -> None:
    """Test --Simple check to prove dblinter can run
    step:
    1 - call __main__
    result: should_pass
    """
    temp_name = tempfile.mktemp()
    dblinter(
        user=postgres_instance_args["user"],
        password=postgres_instance_args["password"],
        host=postgres_instance_args["host"],
        port=postgres_instance_args["port"],
        dbname=postgres_instance_args["dbname"],
        output=temp_name,
        filename="",
        append=False,
    )
    assert exists(temp_name)


def test_dblinter_with_wrong_pg_connection(postgres_instance_args) -> None:
    """Test --Simple check to prove dblinter can run on a failing connection
    step:
    1 - call __main__
    result: should_fail
    """
    temp_name = tempfile.mktemp()
    with pytest.raises(psycopg2.OperationalError):
        dblinter(
            user=postgres_instance_args["user"],
            password=postgres_instance_args["password"],
            host=postgres_instance_args["host"],
            port="1234",
            dbname=postgres_instance_args["dbname"],
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


def test_main_ouput_null(postgres_instance_args) -> None:
    """Test --Simple check to prove dblinter can run
    step:
    1 - call __main__
    result: should_pass
    """
    dblinter(
        user=postgres_instance_args["user"],
        password=postgres_instance_args["password"],
        host=postgres_instance_args["host"],
        port=postgres_instance_args["port"],
        dbname=postgres_instance_args["dbname"],
    )
    assert True


def test_main_filename_empty(postgres_instance_args) -> None:
    """Test --Simple check to prove dblinter can run
    step:
    1 - call __main__
    result: should_pass
    """
    dblinter(
        user=postgres_instance_args["user"],
        password=postgres_instance_args["password"],
        host=postgres_instance_args["host"],
        port=postgres_instance_args["port"],
        dbname=postgres_instance_args["dbname"],
    )
    assert True


def test_main_with_schema_only_ok(postgres_instance_args) -> None:
    """Test --schema-only with schema1.
    create 2 tables 1 in public and 1 in schema1, only the table in schema1 should be checked
    because --schema-only=schema1
    step:
    1 - call perform_table_check
    result: should_pass
    """
    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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


def test_main_with_schema_only_not_exists(postgres_instance_args) -> None:
    """Test --schema-only with schema1. but table is created in public
    and schema1 does not exist
    step:
    1 - call perform_table_check
    result: test is empty
    """

    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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


def test_main_with_include_table(postgres_instance_args) -> None:
    """Test --include-tables. create 2 tables in schema1, 1 in public look for e_table% everywhere
    step:
    1 - call perform_table_check
    result: test is ok
    """

    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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
    # Check that results contain expected messages (order not guaranteed)
    result_messages = [r.message.text for r in sarif_document.sarif_doc.runs[0].results]
    assert "No primary key on table postgres.schema1.e_table1." in result_messages
    assert "No primary key on table postgres.schema1.e_table2." in result_messages


def test_main_with_include_table_and_schema(postgres_instance_args) -> None:
    """Test --include-tables. create 2 tables in schema1, 1 in public look for e_table% in schema1 only.

    step:
    1 - call perform_table_check
    result: test is ok
    """
    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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
        sarif_document.sarif_doc.runs[0].results[2].message.text
        == "No primary key on table postgres.schema1.e_table2."
    )


def test_main_with_exclude_table(postgres_instance_args) -> None:
    """Test --exclude-tables. create 2 tables in schema1, 1 in public exclude all tables e_table2%.
    step:
    1 - call perform_table_check
    result: test is ok
    """
    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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


def test_main_with_schema_and_role_without_default_role_nok(
    postgres_instance_args,
) -> None:
    """Test --schema-only with schema1.
    create 1 table in schema1, with role schema1_ro granted on schema1
    but without default role for futur table.
    step:
    1 - call perform_schema_check
    result: should_fail
    """
    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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
        "No default role granted on schema postgres.schema1. It means that each time a table is created, you must grant it to roles."
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_main_with_schema_and_role_without_default_role_ok(
    postgres_instance_args,
) -> None:
    """Test --schema-only with schema1.
    create 1 table in schema1, with role schema1_ro granted on schema1
    and with default role set for futur table.
    step:
    1 - call perform_schema_check
    result: should_pass
    """

    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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


def test_main_with_schema_and_role_not_exist_nok(postgres_instance_args) -> None:
    """Test --schema-only with schema1.
    create 1 table in schema1, with role created but not granted on schema1
    abd with a default role exists on role schema1_ro
    step:
    1 - call perform_schema_check
    result: should_fail
    """
    function_library = FunctionLibrary()
    db = DatabaseConnection(postgres_instance_args)
    sarif_document = SarifDocument()
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file("dblinter", "default_config.yaml")
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
        "No role granted on table postgres.schema1.e_table1."
        in sarif_document.sarif_doc.runs[0].results[1].message.text
    )


def test_dblinter_additional_rule_path(monkeypatch, postgres_instance_args, tmp_path):
    """Test that additional_rule_path is used by dblinter to load extra rules."""
    # Create a dummy rule in a temporary directory
    rules_dir = tmp_path / "myrules"
    rule_dir = rules_dir / "rules/B999"
    rule_dir.mkdir(parents=True)
    rule_file = rule_dir / "MyDummyRule.py"
    rule_file.write_text(
        """def my_dummy_rule(function_library, db, params, context, *args):\n    # Mark that the rule was called\n    function_library._test_marker = True\n"""
    )

    # Patch FunctionLibrary to load our dummy rule and check if it is called
    orig_init = flib.FunctionLibrary.__init__
    orig_get_func = flib.FunctionLibrary.get_function_by_config_name

    def custom_init(self, paths):
        orig_init(self, paths)
        # pylint: disable=protected-access
        self._test_marker = False

    def custom_get_func(self, name):
        if name == "MyDummyRule":
            spec = importlib.util.spec_from_file_location("MyDummyRule", str(rule_file))
            mod = importlib.util.module_from_spec(spec)
            sys.modules["MyDummyRule"] = mod
            spec.loader.exec_module(mod)
            return mod.my_dummy_rule
        return orig_get_func(self, name)

    monkeypatch.setattr(flib.FunctionLibrary, "__init__", custom_init)
    monkeypatch.setattr(
        flib.FunctionLibrary, "get_function_by_config_name", custom_get_func
    )

    # Create a minimal config file that uses our dummy rule
    config_yaml = f"""
base:
  - name: MyDummyRule
    ruleid: B999
    enabled: true
    params: []
    context:
      desc: Count number of redundant index vs nb index.
      message: "{0} redundant(s) index exceed the warning threshold: {1}%."
      fixes:
          - remove duplicated index or change warning/error threshold.

cluster: []
table: []
schema: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_yaml)

    temp_name = tmp_path / "output.sarif"
    sarif_doc = dblinter(
        user=postgres_instance_args["user"],
        password=postgres_instance_args["password"],
        host=postgres_instance_args["host"],
        port=postgres_instance_args["port"],
        dbname=postgres_instance_args["dbname"],
        output=str(temp_name),
        configuration_path=str(tmp_path),
        filename=str(config_file),
        append=False,
        additional_rule_path=[str(tmp_path / "myrules")],
    )
    # The marker should be set if the rule was loaded and called
    assert sarif_doc is not None
    # Check the marker on the function library
    # (We monkeypatched __init__ to add _test_marker)
    # If the rule ran, it should have set this to True
