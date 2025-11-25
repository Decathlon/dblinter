from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_missing_idx(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} table with seq scan exceed the threshold: {1}.",
    )
    param = [{"threshold": 1}]
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """
        DROP TABLE if exists m_source
    """
    )
    db.query(
        """
        CREATE TABLE m_source (
        s_id integer NOT NULL,
        t_id integer NOT NULL,
        s_name text NOT NULL
        )
    """
    )
    db.query(
        """
        INSERT INTO m_source (s_id, t_id, s_name)
        SELECT i, (i - 1) % 500000 + 1, 'source ' || i
        FROM generate_series(1, 3000000) AS i
    """
    )
    db.query("analyze m_source")
    db.query("SELECT * FROM m_source where s_id = 1000")
    db.query("SELECT * FROM m_source where s_id = 1000")

    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_missing_index")(
        function_library, db, param, context, ("public", "m_source"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "m_source table with seq scan exceed the threshold: 1."
    )


def test_table_without_missing_idx(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="{0} table with seq scan exceed the threshold: {1}.",
    )
    param = [{"threshold": "10000000"}]
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """
        CREATE TABLE n_source (
        s_id integer NOT NULL,
        t_id integer NOT NULL,
        s_name text NOT NULL
        )
    """
    )
    db.query(
        """
        INSERT INTO n_source (s_id, t_id, s_name)
        SELECT i, (i - 1) % 500000 + 1, 'source ' || i
        FROM generate_series(1, 10000) AS i
    """
    )
    db.query("create index on n_source(s_id)")
    db.query("analyze n_source")
    db.query("SELECT * FROM n_source where s_id = 10")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_missing_index")(
        function_library, db, param, context, ("public", "n_source"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
