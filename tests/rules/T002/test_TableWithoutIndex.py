from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_without_index(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        message="No index on table {0}.{1}.{2}",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
    )
    db = DatabaseConnection(args)
    sarif_document = SarifDocument()
    function_library = FunctionLibrary()
    db.query("CREATE TABLE e_table1 (id integer, field1 text)")
    function_library.get_function_by_function_name("table_without_index")(
        function_library, db, [], context, ("public", "e_table1"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "No index on table postgres.public.e_table1"
    )


def test_table_with_index(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        message="No index on table {0}.{1}.{2}.",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
    )
    db = DatabaseConnection(args)
    sarif_document = SarifDocument()
    function_library = FunctionLibrary()
    db.query("CREATE TABLE f_table1 (id integer, field1 text)")
    db.query("CREATE index on f_table1(id)")
    function_library.get_function_by_function_name("table_without_index")(
        function_library, db, [], context, ("public", "f_table1"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
