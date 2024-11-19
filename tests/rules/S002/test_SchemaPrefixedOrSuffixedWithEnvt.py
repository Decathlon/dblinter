from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_schema_with_prefixed_envt_nok(postgres_instance_args) -> None:
    """Test --schema prefixec with dev_.
    create 1 dev_schema1, create 1 role with default grant
    step:
    1 - call schema_prefixed_or_suffixed_with_envt
    result: should_fail
    """
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"size_mo": 1}]
    db.query("CREATE SCHEMA dev_schema1")
    db.query("CREATE ROLE dev_schema1_ro")
    sarif_document = SarifDocument()
    context = Context(
        desc="The schema is prefixed with one of staging,stg,preprod,prod,sandbox,sbox string. Means that when you refresh your preprod, staging environments from production, you have to rename the target schema from prod_ to stg_ or something like. It is possible, but it is never easy.",
        fixes=[
            "Keep the same schema name across environments. Prefer prefix or suffix the database name.",
        ],
        message="You should not prefix or suffix the schema name with {0}. You may have difficulties when refreshing environments. Prefer prefix or suffix the database name.",
    )
    function_library.get_function_by_function_name(
        "schema_prefixed_or_suffixed_with_envt"
    )(function_library, db, param, context, ("dev_schema1", ""), sarif_document)
    assert (
        "You should not prefix or suffix the schema name with"
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )


def test_schema_with_prefixed_envt_ok(postgres_instance_args) -> None:
    """Test --schema prefixec with dev_.
    create 1 schema1, create 1 role with default grant
    step:
    1 - call schema_prefixed_or_suffixed_with_envt
    result: should_pass
    """
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"size_mo": 1}]
    db.query("CREATE SCHEMA schema1")
    db.query("CREATE ROLE schema1_ro")
    sarif_document = SarifDocument()
    context = Context(
        desc="The schema is prefixed with one of staging,stg,preprod,prod,sandbox,sbox string. Means that when you refresh your preprod, staging environments from production, you have to rename the target schema from prod_ to stg_ or something like. It is possible, but it is never easy.",
        fixes=[
            "Keep the same schema name across environments. Prefer prefix or suffix the database name.",
        ],
        message="You should not prefix or suffix the schema name with {0}. You may have difficulties when refreshing environments. Prefer prefix or suffix the database name.",
    )
    function_library.get_function_by_function_name(
        "schema_prefixed_or_suffixed_with_envt"
    )(function_library, db, param, context, ("schema1", ""), sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []
