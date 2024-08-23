from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_table_with_unused_idx(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="Index {0} on {1} size {2} Mo seems to be unused (idx_scan=0).",
    )
    param = [{"size_mo": 1}]
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """
        CREATE TABLE q_source (
        s_id integer NOT NULL,
        t_id integer NOT NULL,
        s_name text NOT NULL
        )
    """
    )
    db.query(
        """
        INSERT INTO q_source (s_id, t_id, s_name)
        SELECT i, (i - 1) % 500000 + 1, 'source ' || i
        FROM generate_series(1, 50000) AS i
    """
    )
    db.query("CREATE INDEX q_idx1 on q_source(s_name)")
    db.query("CREATE INDEX q_idx2 on q_source(t_id)")
    db.query("analyze q_source")
    db.query("SELECT * FROM q_source where s_id = 10")
    db.query("SELECT * FROM q_source where s_id = 11")
    db.query("SELECT * FROM q_source where s_id = 12")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_unused_index")(
        function_library, db, param, context, ("public", "q_source"), sarif_document
    )
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "Index q_idx1 on postgres.public.q_source size 2 Mo seems to be unused (idx_scan=0)."
    )


def test_table_with_used_idx(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="Index {0} on {1} size {2} Mo seems to be unused (idx_scan=0).",
    )
    param = [{"size_mo": 1}]
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """
        CREATE TABLE r_source (
        s_id integer NOT NULL,
        t_id integer NOT NULL,
        s_name text NOT NULL
        )
    """
    )
    db.query(
        """
        INSERT INTO r_source (s_id, t_id, s_name)
        SELECT i, (i - 1) % 500000 + 1, 'source ' || i
        FROM generate_series(1, 1000) AS i
    """
    )
    db.query("CREATE INDEX r_idx1 on r_source(s_name)")
    db.query("analyze r_source")
    db.query("select * from r_source where s_name='source foso'")
    db.query("select * from r_source where s_name='source food'")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_unused_index")(
        function_library, db, param, context, ("public", "r_source"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []


def test_table_with_unused_idx_not_exists(postgres_instance_args) -> None:
    args = postgres_instance_args
    context = Context(
        desc="Count number of tables without index on foreign key.",
        fixes=["create a index on foreign key or change warning/error threshold."],
        message="Index {0} on {1} size {2} Mo seems to be unused (idx_scan=0).",
    )
    param = [{"size_mo": 1}]
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    db.query(
        """
        CREATE TABLE s_source (
        s_id integer NOT NULL,
        t_id integer NOT NULL,
        s_name text NOT NULL
        )
    """
    )
    db.query(
        """
        INSERT INTO s_source (s_id, t_id, s_name)
        SELECT i, (i - 1) % 500000 + 1, 'source ' || i
        FROM generate_series(1, 10000) AS i
    """
    )
    db.query("analyze s_source")
    db.query("select * from s_source where s_name='source foso'")
    db.query("select * from s_source where s_name='source food'")
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name("table_with_unused_index")(
        function_library, db, param, context, ("public", "s_source"), sarif_document
    )
    assert sarif_document.sarif_doc.runs[0].results == []
