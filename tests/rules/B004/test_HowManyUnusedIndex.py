from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_how_many_table_with_unused_idx(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE b_table1 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE b_table2 (id integer,field1 text)")
    db.query("CREATE TABLE b_table3 (id integer,field1 text)")
    db.query("CREATE INDEX b_idx1 on b_table1 (field1)")
    db.query("CREATE INDEX b_idx2 on b_table2 (field1)")
    db.query(
        """
        INSERT INTO b_table1 (id, field1)
        SELECT i, 'source ' || i
        FROM generate_series(1, 100000) AS i
    """
    )
    db.query(
        """
        INSERT INTO b_table2 (id, field1)
        SELECT i, 'source ' || i
        FROM generate_series(1, 100000) AS i
    """
    )
    db.query("analyze b_table1")
    db.query("analyze b_table2")
    db.query("analyze b_table3")
    db.query("select * from b_table1 where id=1")
    db.query("select * from b_table2 where id=1")
    db.query("select * from b_table3 where id=1")
    param = [{"warning": "50%"}, {"size_mo": "1"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} table(s) with unused index exceed the warning threshold: {1}%.",
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_table_with_unused_index")(
        function_library, db, param, context, sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "2 table(s) with unused index exceed the warning threshold: 50%."
    )


def test_how_many_table_with_used_idx(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE TABLE b_table1 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE b_table2 (id integer PRIMARY KEY,field1 text)")
    db.query("CREATE TABLE b_table3 (id integer PRIMARY KEY,field1 text)")
    db.query(
        """
        INSERT INTO b_table1 (id, field1)
        SELECT i, 'source ' || i
        FROM generate_series(1, 100000) AS i
    """
    )
    db.query(
        """
        INSERT INTO b_table2 (id, field1)
        SELECT i, 'source ' || i
        FROM generate_series(1, 100000) AS i
    """
    )
    db.query("analyze b_table1")
    db.query("analyze b_table2")
    db.query("analyze b_table3")
    db.query("select * from b_table1 where id=1")
    db.query("select * from b_table2 where id=1")
    db.query("select * from b_table3 where id=1")
    param = [{"warning": "50%"}, {"size_mo": "1"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} table(s) with unused index exceed the warning threshold: {1}%.",
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_table_with_unused_index")(
        function_library, db, param, context, sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []


def test_how_many_table_with_used_idx_without_table(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"warning": "50%"}, {"size_mo": "1"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} table(s) with unused index exceed the warning threshold: {1}%.",
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("how_many_table_with_unused_index")(
        function_library, db, param, context, sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
