from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_uppercase(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables with uppercase.",
        fixes=[
            "Only use lowercase for tables, columns, views, schemas, etc...",
        ],
        message="Uppercase detected on table {0}.{1}.{2}.",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query('CREATE TABLE "Test" (id integer ,field1 text)')
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_uppercase")(
        function_library, db, [], context, ("public", "Test"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "Uppercase detected on table postgres.public.Test."
    )


def test_table_with_uppercase_column(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables with uppercase.",
        fixes=[
            "Only use lowercase for tables, columns, views, schemas, etc...",
        ],
        message="Uppercase detected on table {0}.{1}.{2}.",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query('CREATE TABLE test2 ("Id" integer ,field1 text)')
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_uppercase")(
        function_library, db, [], context, ("public", "test2"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "Uppercase detected on table postgres.public.test2."
    )


def test_table_without_uppercase(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables with uppercase.",
        fixes=[
            "Only use lowercase for tables, columns, views, schemas, etc...",
        ],
        message="Uppercase detected on table {0}.{1}.{2}.",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE l_table1 (id integer PRIMARY KEY ,field1 text)")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_uppercase")(
        function_library, db, [], context, ("public", "l_table1"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
