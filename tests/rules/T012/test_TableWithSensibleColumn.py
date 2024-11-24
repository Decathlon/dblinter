from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_sensitive_column(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Base on the extension anon (https://postgresql-anonymizer.readthedocs.io/en/stable/detection), show sensitive column.",
        fixes=[
            "Install extension anon, and create some masking rules on.",
        ],
        message="{0} have column {1} (category {2}) that can be consider has sensitive. They should be masked, in non-prod environment."
    )
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query('select anon.init()')
    db.query('CREATE TABLE test (id integer, creditcard text)')
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_sensible_column")(
        function_library, db, [], context, ("public", "test"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "postgres.public.test have column creditcard (category creditcard) that can be consider has sensitive. They should be masked, in non-prod environment."
    )
