from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_fk_mismatch_integer(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["Consider rewrite your model, ask a DBA"],
        message="Type constraint mismatch: {0} on {1} column {2} (type {3}/{4}) ref {5} column {6} type ({7}/{8})",
    )
    param = [{"size_mo": 1}]
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """
        CREATE TABLE groups_1 (
            id SERIAL,
            id_txt smallint not null,
            group_name VARCHAR(50) NOT NULL,
            PRIMARY KEY (id,id_txt)
        )
    """
    )
    db.query(
        """
        CREATE TABLE users_1 (
            id SERIAL,
            username VARCHAR(50) NOT NULL,
            group_id INTEGER,
            group_id_txt INTEGER not null,
            PRIMARY KEY (id),
            FOREIGN KEY (group_id,group_id_txt) REFERENCES groups_1 (id,id_txt)
        )
    """
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_fk_type_mixmatch")(
        function_library, db, param, context, ("public", "users_1"), sarif_document
    )
    assert (
        "Type constraint mismatch: users_1_group_id_group_id_txt_fkey on postgres.public.users_1 column group_id, group_id_txt"
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_table_with_fk_mismatch_varchar(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["consider rewrite your model.", "ask a dba."],
        message="Type constraint mismatch: {0} on {1} column {2} (type {3}/{4}) ref {5} column {6} type ({7}/{8})",
    )
    param = [{"size_mo": 1}]
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """
        CREATE TABLE groups_1 (
            id SERIAL,
            id_txt VARCHAR(50) not null,
            group_name VARCHAR(50) NOT NULL,
            PRIMARY KEY (id,id_txt)
        )
    """
    )
    db.query(
        """
        CREATE TABLE users_1 (
            id SERIAL,
            username VARCHAR(50) NOT NULL,
            group_id INTEGER,
            group_id_txt VARCHAR(10) not null,
            PRIMARY KEY (id),
            FOREIGN KEY (group_id,group_id_txt) REFERENCES groups_1 (id,id_txt)
        )
    """
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_fk_type_mixmatch")(
        function_library, db, param, context, ("public", "users_1"), sarif_document
    )
    assert (
        "Type constraint mismatch: users_1_group_id_group_id_txt_fkey on postgres.public.users_1 column group_id, group_id_txt"
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_table_with_fk_match_int(postgres_instance_args) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["consider rewrite your model.", "ask a dba."],
        message="Type constraint mismatch: {0} on {1} column {2} (type {3}/{4}) ref {5} column {6} type ({7}/{8})",
    )
    param = [{"size_mo": 1}]

    db.query(
        """
        CREATE TABLE groups_1 (
            id SERIAL,
            id_txt integer not null,
            group_name VARCHAR(50) NOT NULL,
            PRIMARY KEY (id,id_txt)
        )
    """
    )
    db.query(
        """
        CREATE TABLE users_1 (
            id SERIAL,
            username VARCHAR(50) NOT NULL,
            group_id INTEGER,
            group_id_txt integer not null,
            PRIMARY KEY (id),
            FOREIGN KEY (group_id,group_id_txt) REFERENCES groups_1 (id,id_txt)
        )
    """
    )

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_fk_type_mixmatch")(
        function_library, db, param, context, ("public", "users_1"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
