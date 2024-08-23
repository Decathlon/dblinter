from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_using_retricted_keyword(postgres_instance_args) -> None:
    """Test
    create 1 table with forbiden name
    step:
    1 - call reserved_keyword
    result: the check should trigger
    """
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    context = Context(
        desc="A table, his column or indexes use reserved keywords.",
        message="{0} {1}.{2}.{3}.{4} violate retricted keyword rule",
        fixes=["Rename the object to use a non reserved keyword."],
    )
    db.query(
        """
        CREATE TABLE "select" ("desc" text);
        CREATE INDEX "current_time" ON "select" ("desc");
        ALTER TABLE public."select" ADD CONSTRAINT "from" UNIQUE ("desc");
        """
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("reserved_keyword")(
        function_library, db, [], context, ("public", """select"""), sarif_document
    )
    assert (
        "Table postgres.public.select. violate retricted keyword rule"
        in sarif_document.sarif_doc.runs[0].results[0].message.text
    )
    assert (
        "Column postgres.public.select.desc violate retricted keyword rule"
        in sarif_document.sarif_doc.runs[0].results[1].message.text
    )
    assert (
        "Index postgres.public.select.current_time violate retricted keyword rule"
        in sarif_document.sarif_doc.runs[0].results[2].message.text
    )
    assert (
        "Constraint postgres.public.select.from violate retricted keyword rule"
        in sarif_document.sarif_doc.runs[0].results[4].message.text
    )
