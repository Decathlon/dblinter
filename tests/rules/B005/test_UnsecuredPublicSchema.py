from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_unsecured_public_schema_nok(postgres_instance_args) -> None:
    """Test database with unsecured public schema
    step:
    1 - call unsecured_public_schema
    result: should_fail
    """
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = None
    context = Context(
        desc="Only authorized users should be allowed to create new objects.",
        message="All schemas on search_path are unsecured, all users can create objects",
        fixes=["REVOKE CREATE ON SCHEMA public FROM PUBLIC"],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("unsecured_public_schema")(
        function_library, db, param, context, sarif_document
    )
    assert (
        "All schemas on search_path are unsecured, all users can create objects"
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_unsecured_public_schema_ok(postgres_instance_args) -> None:
    """Test database with secured public schema
    step:
    1 - call unsecured_public_schema
    result: should_pass
    """
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = None
    context = Context(
        desc="Only authorized users should be allowed to create new objects.",
        message="All schemas on search_path are unsecured, all users can create objects",
        fixes=["REVOKE CREATE ON SCHEMA public FROM PUBLIC"],
    )
    db.query("REVOKE CREATE ON SCHEMA public from PUBLIC")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("unsecured_public_schema")(
        function_library, db, param, context, sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
