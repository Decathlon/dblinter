from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_sensitive_column(postgres_instance_args) -> None:
    args = postgres_instance_args
    db = DatabaseConnection(args)
    CHECK_EXTENSION = "select count(*) as nb from pg_extension where extname='anon'"
    anon = db.query(CHECK_EXTENSION)[0][0]
    if anon == 0:
        assert True
        return
    context = Context(
        desc="Base on the extension anon (https://postgresql-anonymizer.readthedocs.io/en/stable/detection), show sensitive column.",
        fixes=[
            "Install extension anon, and create some masking rules on.",
        ],
        message="{0} have column {1} (category {2}) that can be consider has sensitive. It should be masked for non data-operator users.",
    )
    function_library = FunctionLibrary()
    db.query("select anon.init()")
    db.query("CREATE TABLE test (id integer, creditcard text)")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_sensible_column")(
        function_library, db, [], context, ("public", "test"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "postgres.public.test have column creditcard (category creditcard) that can be consider has sensitive. It should be masked for non data-operator users."
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[1].message.text
        == "postgres.public.test have column id (category account_id) that can be consider has sensitive. It should be masked for non data-operator users."
    )


def test_table_without_sensitive_column(postgres_instance_args) -> None:
    args = postgres_instance_args
    db = DatabaseConnection(args)
    CHECK_EXTENSION = "select count(*) as nb from pg_extension where extname='anon'"
    anon = db.query(CHECK_EXTENSION)[0][0]
    if anon == 0:
        assert True
        return
    context = Context(
        desc="Base on the extension anon (https://postgresql-anonymizer.readthedocs.io/en/stable/detection), show sensitive column.",
        fixes=[
            "Install extension anon, and create some masking rules on.",
        ],
        message="{0} have column {1} (category {2}) that can be consider has sensitive. It should be masked for non data-operator users.",
    )
    function_library = FunctionLibrary()
    db.query("select anon.init()")
    db.query("CREATE TABLE test (test_id integer, description text)")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_sensible_column")(
        function_library, db, [], context, ("public", "test"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
