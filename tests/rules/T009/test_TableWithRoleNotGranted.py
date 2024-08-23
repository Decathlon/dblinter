from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_without_role_granted_on(postgres_instance_args) -> None:
    """Test --table without role.
    create 1 table in schema1, without any grants
    step:
    1 - call table_with_role_not_granted
    result: should_fail
    """
    args = postgres_instance_args
    context = Context(
        desc="Table has no roles grantee. Meaning that users will need direct access on it (not through a role)",
        message="No role grantee on table {0}.{1}.{2}. It means that except owner. Others will need a direct grant on this table, not through a role (unusual at dkt).",
        fixes=[
            "create roles (myschema_ro & myschema_rw) and grant it on table with appropriate privileges."
        ],
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"size_mo": 1}]
    db.query("CREATE SCHEMA schema1")
    db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
    sarif_document = SarifDocument()

    function_library.get_function_by_function_name("table_with_role_not_granted")(
        function_library, db, param, context, ("schema1", "e_table1"), sarif_document
    )
    assert (
        "No role grantee on table postgres.schema1.e_table1"
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_table_with_role_granted_on(postgres_instance_args) -> None:
    """Test --table with role.
    create 1 table in schema1, with role schema1_ro and grant select on it
    step:
    1 - call table_with_role_not_granted
    result: should_pass
    """
    args = postgres_instance_args
    context = Context(
        desc="Table has no roles grantee. Meaning that users will need direct access on it (not through a role)",
        message="No role grantee on table {0}.{1}.{2}. It means that except owner. Others will need a direct grant on this table, not through a role (unusual at dkt).",
        fixes=[
            "create roles (myschema_ro & myschema_rw) and grant it on table with appropriate privileges."
        ],
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"size_mo": 1}]
    db.query("CREATE SCHEMA schema1")
    db.query("CREATE ROLE schema1_ro")
    db.query("CREATE TABLE schema1.e_table1 (id integer, field1 text)")
    db.query("GRANT SELECT ON ALL TABLES IN SCHEMA schema1 to schema1_ro")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_role_not_granted")(
        function_library, db, param, context, ("schema1", "e_table1"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
