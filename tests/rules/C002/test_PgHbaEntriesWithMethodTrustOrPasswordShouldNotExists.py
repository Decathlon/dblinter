from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_pg_hba_entries_with_trust_or_password_method_exceed(
    postgres_instance_args,
) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"warning": 1}]
    context = Context(
        desc="This configuration is extremely insecure and should only be used in a controlled, non-production environment for testing purposes. In a production environment, you should use more secure authentication methods such as md5, scram-sha-256, or cert, and restrict access to trusted IP addresses only.",
        message="{0} entries in pg_hba.conf with trust or password authentication method exceed the warning threshold: {1}.",
        fixes=[
            "change trust or password method in pg_hba.conf.",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "pg_hba_entries_with_trust_or_password_method"
    )(function_library, db, param, context, sarif_document)
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "6 entries in pg_hba.conf with trust or password authentication method exceed the warning threshold: 1."
    )


def test_pg_hba_entries_with_trust_or_password_method_not_exceed(
    postgres_instance_args,
) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"warning": 7}]
    context = Context(
        desc="This configuration is extremely insecure and should only be used in a controlled, non-production environment for testing purposes. In a production environment, you should use more secure authentication methods such as md5, scram-sha-256, or cert, and restrict access to trusted IP addresses only.",
        message="{0} entries in pg_hba.conf with trust or password authentication method exceed the warning threshold: {1}.",
        fixes=[
            "change trust or password method in pg_hba.conf.",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "pg_hba_entries_with_trust_or_password_method"
    )(function_library, db, param, context, sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []
