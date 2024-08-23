from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_fk_not_indexed(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="unindexed fk {0}.{1}.{2} ddl:{3}",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """CREATE TABLE i_groups (
        id SERIAL,
        group_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)) """
    )

    db.query(
        """CREATE TABLE i_users (
        id SERIAL,
        username VARCHAR(50) NOT NULL,
        group_id INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (group_id) REFERENCES i_groups (id))"""
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_without_index_on_fk")(
        function_library, db, [], context, ("public", "i_users"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "unindexed fk postgres.public.i_users ddl:create index on i_users(group_id)"
    )


def test_table_with_fk_indexed(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """CREATE TABLE j_groups (
        id SERIAL,
        group_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)) """
    )

    db.query(
        """CREATE TABLE j_users (
        id SERIAL,
        username VARCHAR(50) NOT NULL,
        group_id INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (group_id) REFERENCES j_groups (id))"""
    )
    db.query("create index on public.j_users(group_id)")

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_without_index_on_fk")(
        function_library, db, [], context, ("public", "j_users"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []


def test_table_with_fk_indexed_without_table(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=[
            "create a index on foreign key or change warning/error threshold.",
        ],
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_without_index_on_fk")(
        function_library, db, [], context, ("public", "j_users"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
