from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_without_primary_key(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
        message="No primary key on table {0}.{1}.{2}.",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE k_table1 (id integer ,field1 text)")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_without_primary_key")(
        function_library, db, [], context, ("public", "k_table1"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "No primary key on table postgres.public.k_table1."
    )


def test_table_with_primary_key(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
        message="No primary key on table {0}.",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE l_table1 (id integer PRIMARY KEY ,field1 text)")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_without_primary_key")(
        function_library, db, [], context, ("public", "l_table1"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []