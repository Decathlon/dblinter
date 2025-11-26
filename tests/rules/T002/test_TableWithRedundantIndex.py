from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_redundant_index(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        message="{0} redundant(s) index found on {1}.{2} idx {3} column {4}.",
        fixes=["create a index on foreign key or change warning/error threshold."],
    )
    db = DatabaseConnection(args)
    sarif_document = SarifDocument()
    function_library = FunctionLibrary()
    db.query("CREATE TABLE g_table1 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE INDEX g_idx1_table1 ON g_table1 (id)")
    db.query("CREATE INDEX g_idx2_table1 ON g_table1 (field1, id)")
    function_library.get_function_by_function_name("table_with_redundant_index")(
        function_library, db, [], context, ("public", "g_table1"), sarif_document
    )
    print(sarif_document.sarif_doc.runs[0].results[0].message.text)
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "2 redundant(s) index found on public.g_table1 idx g_table1_pkey column id."
    )


def test_table_without_redundant_index(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        message="{0} redundant(s) index found on {1}.{2} idx {3} column {4}.",
        fixes=["create a index on foreign key or change warning/error threshold."],
    )
    db = DatabaseConnection(args)
    sarif_document = SarifDocument()
    function_library = FunctionLibrary()
    db.query("CREATE TABLE h_table1 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE INDEX h_idx2_table1 ON h_table1 (field1, id)")
    function_library.get_function_by_function_name("table_with_redundant_index")(
        function_library, db, [], context, ("public", "h_table1"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
