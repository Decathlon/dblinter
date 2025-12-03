from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_how_many_table_without_primary_key(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE a_table1 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE a_table2 (id integer,field1 text)")
    db.query("CREATE TABLE a_table3 (id integer,field1 text)")
    param = [{"warning": "50%"}]
    context = Context(
        desc="Count number of tables without primary key.",
        message="{0} table without primary key exceed the warning threshold: {1}%. Object list [{2}]",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "how_many_table_without_primary_key"
    )(function_library, db, param, context, sarif_document)
    assert (
        "2 table without primary key exceed the warning threshold: 50%."
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_how_many_table_with_primary_key(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE a_table1 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE a_table2 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE a_table3 (id integer PRIMARY KEY,field1 text)")
    param = [{"warning": "50%"}]
    context = [
        {
            "desc": "Count number of tables without primary key.",
            "message": "{0} table without primary key exceed the warning threshold: {1}%.",
            "fixes": [
                "create a index on foreign key or change warning/error threshold.",
            ],
        }
    ]
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "how_many_table_without_primary_key"
    )(function_library, db, param, context, sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []


def test_how_many_table_with_primary_key_without_table(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"warning": "50%"}]
    context = Context(
        desc="Count number of tables without primary key.",
        message="{0} table without primary key exceed the warning threshold: {1}%.",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "how_many_table_without_primary_key"
    )(function_library, db, param, context, sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []
