from testcontainers.postgres import PostgresContainer

from dblinter import __version__
from dblinter.configuration_model import Context
from dblinter.sarif_document import SarifDocument
from dblinter.scan import dblinter


def test_init_sarif_document():
    sd = SarifDocument()
    assert (
        sd.sarif_doc.schema_uri
        == "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0-rtm.5.json"
    )


def test_add_run_information():
    sd = SarifDocument()

    assert sd.sarif_doc.runs[0].tool.driver.name == "dblinter"
    assert (
        sd.sarif_doc.runs[0].tool.driver.information_uri
        == "https://github.com/decathlon/dblinter"
    )
    assert sd.sarif_doc.runs[0].tool.driver.version == __version__


def test_add_check():
    sd = SarifDocument()
    context = Context(
        desc="Number of cx (max_connections * work_mem) is not greater than memory.",
        message="work_mem * max_connections is bigger than ram.",
        fixes=[
            "downsize max_connections or upsize memory.",
        ],
    )

    sd.add_check("C001", (), "cluster", context)
    assert (
        sd.sarif_doc.runs[0].results[0].message.text
        == "work_mem * max_connections is bigger than ram."
    )
    assert sd.sarif_doc.runs[0].results[0].rule_id == "C001"
    assert (
        sd.sarif_doc.runs[0]
        .results[0]
        .locations[0]
        .physical_location.artifact_location.uri
        == "cluster"
    )
    assert sd.sarif_doc.runs[0].results[0].fixes[0] == "downsize max_connections or upsize memory."

def test_add_check_output(capsys):
    sd = SarifDocument()
    context = Context(
        desc="Number of cx (max_connections * work_mem) is not greater than memory.",
        message="work_mem * max_connections is bigger than ram.",
        fixes=[
            "downsize max_connections or upsize memory.",
        ],
    )
    sd.add_check("C001", (), "cluster", context)
    captured = capsys.readouterr()
    assert captured.out == '  ⚠ - C001 cluster work_mem * max_connections is bigger than ram.\n    ↪ Fix:  downsize max_connections or upsize memory.\n'

def test_add_check_output_no_fix(capsys):
    sd = SarifDocument()
    context = Context(
        desc="Number of cx (max_connections * work_mem) is not greater than memory.",
        message="work_mem * max_connections is bigger than ram.",
        fixes=[],
    )
    sd.add_check("C001", (), "cluster", context)
    captured = capsys.readouterr()
    assert captured.out == '  ⚠ - C001 cluster work_mem * max_connections is bigger than ram.\n'

def test_add_check_output_no_message(capsys):
    sd = SarifDocument()
    context = Context(
        desc="Number of cx (max_connections * work_mem) is not greater than memory.",
        message=None,
        fixes=[
            "downsize max_connections or upsize memory.",
        ],
    )
    sd.add_check("C001", (), "cluster", context)
    captured = capsys.readouterr()
    assert captured.out == ""

def test_json_format():
    sd = SarifDocument()
    sd.sarif_doc.runs[0].invocations[0].start_time_utc = None
    js = sd.json_format()
    assert (
        js
        == '{\n  "runs": [\n    {\n      "tool": {\n        "driver": {\n          "name": "dblinter",\n          "version": "0.0.0",\n          "informationUri": "https://github.com/decathlon/dblinter"\n        }\n      },\n      "invocations": [\n        {\n          "executionSuccessful": true\n        }\n      ],\n      "results": []\n    }\n  ],\n  "version": "2.1.0",\n  "$schema": "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0-rtm.5.json"\n}'
    )


def test_quiet_mode_on(capsys):
    with PostgresContainer(
        "postgres:14", 5432, "postgres", "postgres", "mytestdb"
    ) as postgres:
        dblinter(
            user="postgres",
            password="postgres",
            host="localhost",
            port=postgres.get_exposed_port(5432),
            dbname="mytestdb",
            quiet=True,
        )
        captured = capsys.readouterr()
        assert captured.out == ""


def test_quiet_mode_off(capsys):
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
        captured = capsys.readouterr()
        assert captured.out != ""
