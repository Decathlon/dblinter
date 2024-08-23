from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_fk_in_other_schema(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="fk {0} on {1} is in schema {2}",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE SCHEMA schema1")
    db.query(
        """CREATE TABLE schema1.o_groups (
        id SERIAL,
        group_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)) """
    )
    db.query("CREATE SCHEMA schema2")
    db.query(
        """CREATE TABLE schema2.o_users (
        id SERIAL,
        username VARCHAR(50) NOT NULL,
        group_id INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (group_id) REFERENCES schema1.o_groups (id))"""
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_fk_in_other_schema")(
        function_library, db, [], context, ("schema2", "o_users"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "fk o_users_group_id_fkey on schema2.o_users is in schema schema1"
    )


def test_table_with_fk_the_same_schema(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="fk {0} on {1} is in schema {2}",
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query("CREATE SCHEMA schema1")
    db.query(
        """CREATE TABLE schema1.p_groups (
        id SERIAL,
        group_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)) """
    )
    db.query(
        """CREATE TABLE schema1.p_users (
        id SERIAL,
        username VARCHAR(50) NOT NULL,
        group_id INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (group_id) REFERENCES schema1.p_groups (id))"""
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_fk_in_other_schema")(
        function_library, db, [], context, ("schema1", "p_users"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
