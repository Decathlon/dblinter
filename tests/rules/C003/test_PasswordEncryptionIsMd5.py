from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_password_encryption_with_md5(
    postgres12_instance_args,
) -> None:
    args = postgres12_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"password_encryption": "md5"}]
    context = Context(
        desc="password_encryption is set to md5",
        message="password_encryption is set to md5, this will prevent upgrade to Postgres 18",
        fixes=[
            "change password_encryption to scram-sha-256 and rotate users passwords",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "password_encryption_is_md5"
    )(function_library, db, param, context, sarif_document)
    assert (
        len(sarif_document.sarif_doc.runs[0].results)>0 and
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "password_encryption is set to md5, this will prevent upgrade to Postgres 18"
    )

def test_password_encryption_without_md5(
    postgres_instance_args,
) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"password_encryption": "md5"}]
    context = Context(
        desc="password_encryption is set to md5",
        message="password_encryption is set to md5, this will prevent upgrade to Postgres 18",
        fixes=[
            "change password_encryption to scram-sha-256 and rotate users passwords",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "password_encryption_is_md5"
    )(function_library, db, param, context, sarif_document)
    assert (
        len(sarif_document.sarif_doc.runs[0].results)==0
    )
