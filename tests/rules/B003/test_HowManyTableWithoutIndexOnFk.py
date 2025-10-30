from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_how_many_table_without_fk_indexed(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """CREATE TABLE d_groups (
        id SERIAL,
        group_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)) """
    )

    db.query(
        """CREATE TABLE d_users (
        id SERIAL,
        username VARCHAR(50) NOT NULL,
        group_id INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (group_id) REFERENCES d_groups (id))"""
    )
    param = [{"warning": "50%"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} table without index on foreign key exceed the warning threshold: {1}%. list [{2}]",
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "how_many_table_without_index_on_fk"
    )(function_library, db, param, context, sarif_document)
    assert (
        "1 table without index on foreign key exceed the warning threshold: 50%." in
        sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_how_many_table_with_fk_indexed(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """CREATE TABLE d_groups (
        id SERIAL,
        group_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)) """
    )

    db.query(
        """CREATE TABLE d_users (
        id SERIAL,
        username VARCHAR(50) NOT NULL,
        group_id INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (group_id) REFERENCES d_groups (id))"""
    )
    db.query("CREATE INDEX on d_users (group_id)")
    param = [{"warning": "50%"}]
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} table without index on foreign key exceed the warning threshold: {1}%.",
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "how_many_table_without_index_on_fk"
    )(function_library, db, param, context, sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []
