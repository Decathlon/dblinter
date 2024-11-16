from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_schema_without_default_role_granted_on(postgres_instance_args) -> None:
    """Test --schema without default role.
    create 1 schema1, without any default roles granted
    step:
    1 - call schema_with_default_role_not_granted
    result: should_fail
    """
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"size_mo": 1}]
    db.query("CREATE role role1")
    db.query("CREATE SCHEMA schema1 authorization role1")
    sarif_document = SarifDocument()
    context = Context(
        desc="The schema ha no default role. Means that futur table will not be granted through a role. So you will have to re-execute grants on it.",
        message="No default role grantee on schema {0}.{1}. It means that each time a table is created, you must grant it to roles.",
        fixes=[
            "add a default privilege=> ALTER DEFAULT PRIVILEGES IN SCHEMA <schema> for user <schema's owner>",
        ],
    )

    function_library.get_function_by_function_name(
        "schema_with_default_role_not_granted"
    )(function_library, db, param, context, ("schema1", ""), sarif_document)
    assert (
        "No default role grantee on schema postgres.schema1. It means that each time a table is created, you must grant it to roles."
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_schema_with_default_role_granted_on(postgres_instance_args) -> None:
    """Test --schema with default role.
    create 1 schema1, creatz 1 role with default grant
    step:
    1 - call schema_with_default_role_not_granted
    result: should_pass
    """
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"size_mo": 1}]
    db.query("CREATE SCHEMA schema1")
    db.query("CREATE ROLE schema1_ro")
    db.query(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA schema1 for user postgres GRANT SELECT ON TABLES TO schema1_ro"
    )
    sarif_document = SarifDocument()
    context = Context(
        desc="The schema ha no default role. Means that futur table will not be granted through a role. So you will have to re-execute grants on it.",
        fixes=[
            "add a default privilege=> ALTER DEFAULT PRIVILEGES IN SCHEMA <schema> for user <schema's owner>",
        ],
        message="No default role grantee on schema {0}. It means that each time a table is created, you must grant it to roles.",
    )
    function_library.get_function_by_function_name(
        "schema_with_default_role_not_granted"
    )(function_library, db, param, context, ("schema1", ""), sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []
