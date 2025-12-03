from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_how_many_non_redundant_index(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        "CREATE TABLE c_table1 (id integer PRIMARY KEY, field1 smallint, field2 smallint)"
    )
    db.query(
        "CREATE TABLE c_table2 (id integer,field1 smallint, field2 smallint, field3 smallint)"
    )
    param = [{"warning": "10%"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        message="{0} redundant(s) index exceed the warning threshold: {1}%.",
        fixes=["create a index on foreign key or change warning/error threshold."],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_redundant_index")(
        function_library, db, param, context, sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []


def test_how_many_redundant_index(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        "CREATE TABLE c_table1 (id integer PRIMARY KEY, field1 smallint, field2 smallint)"
    )
    db.query(
        "CREATE TABLE c_table2 (id integer,field1 smallint, field2 smallint, field3 smallint)"
    )
    db.query("CREATE INDEX ON c_table1 (id)")
    db.query("CREATE INDEX ON c_table2 (field1, id)")
    db.query("CREATE INDEX ON c_table2 (field1, field3)")
    param = [{"warning": "10%"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} redundant(s) index exceed the warning threshold: {1}%. Object list [{2}]",
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_redundant_index")(
        function_library, db, param, context, sarif_document
    )
    assert (
        "2 redundant(s) index exceed the warning threshold: 10%."
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_how_many_redundant_index_with_2_redundant_index(
    postgres_instance_args,
) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        "CREATE TABLE c_table1 (id integer PRIMARY KEY, field1 smallint, field2 smallint)"
    )
    db.query(
        "CREATE TABLE c_table2 (id integer,field1 smallint, field2 smallint, field3 smallint)"
    )
    db.query("CREATE INDEX c_idx1_table1 ON c_table1 (id)")
    db.query("CREATE INDEX c_idx2_table1 ON c_table1 (id, field1)")
    db.query("CREATE INDEX c_idx1_table2 ON c_table2 (field2)")
    db.query("CREATE INDEX c_idx2_table2 ON c_table2 (field2, field1)")
    param = [{"warning": "10%"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} redundant(s) index exceed the warning threshold: {1}%.",
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_redundant_index")(
        function_library, db, param, context, sarif_document
    )
    assert (
        "2 redundant(s) index exceed the warning threshold: 10%."
        == sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_how_many_non_redundant_index_without_table(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"warning": "10%"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} redundant(s) index exceed the warning threshold: {1}%.",
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_redundant_index")(
        function_library, db, param, context, sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
