from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_how_many_tables_with_uppercase(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE test (id integer)")
    db.query('CREATE TABLE "Test" (id integer,field1 text)')
    db.query('CREATE TABLE "TEST" (id integer,field1 text)')
    db.query('CREATE TABLE test2 ("Id" integer,field1 text)')
    param = [{"warning": "1%"}]
    context = Context(
        desc="Count number of tables with uppercase characters.",
        message="{0} table(s) with uppercase characters exceed the warning threshold: {1}%. list [{2}]",
        fixes=[
            "Only use lowercase for tables, columns, views, schemas, etc...",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_tables_with_uppercase")(
        function_library, db, param, context, sarif_document
    )
    assert (
        "3 table(s) with uppercase characters exceed the warning threshold: 1%."
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_how_many_tables_with_lowercase(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE a_table1 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE a_table2 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE a_table3 (id integer PRIMARY KEY,field1 text)")
    param = [{"warning": "1%"}]
    context = Context(
        desc="Count number of tables with uppercase.",
        message="{0} table without uppercase exceed the warning threshold: {1}%.",
        fixes=[
            "Only use lowercase for tables, columns, views, schemas, etc...",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_tables_with_uppercase")(
        function_library, db, param, context, sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
