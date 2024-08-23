from dblinter.configuration_model import Context
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument


def test_max_connection_by_workmem_is_not_larger_than_memory(
    postgres_instance_args,
) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"ram": 1048576}]
    context = Context(
        desc="Number of cx (max_connections * work_mem) is not greater than memory.",
        message="work_mem * max_connections is bigger than ram.",
        fixes=[
            "downsize max_connections or upsize memory.",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "max_connection_by_workmem_is_not_larger_than_memory"
    )(function_library, db, param, context, sarif_document)
    assert (
        sarif_document.sarif_doc.runs[0].results[0].message.text
        == "work_mem * max_connections is bigger than ram."
    )


def test_max_connection_by_workmem_is_larger_than_memory(
    postgres_instance_args,
) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"ram": 50000000000000}]
    context = Context(
        desc="Number of cx (max_connections * work_mem) is not greater than memory.",
        message="work_mem * max_connections is bigger than ram.",
        fixes=[
            "downsize max_connections or upsize memory.",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "max_connection_by_workmem_is_not_larger_than_memory"
    )(function_library, db, param, context, sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []


def test_max_connection_by_workmem_ram_is_wrong(postgres_instance_args, caplog) -> None:
    args = postgres_instance_args
    function_library = FunctionLibrary()
    db = DatabaseConnection(args)
    param = [{"rom": 50000000000000}]
    context = Context(
        desc="Number of cx (max_connections * work_mem) is not greater than memory.",
        message="work_mem * max_connections is bigger than ram.",
        fixes=[
            "downsize max_connections or upsize memory.",
        ],
    )
    sarif_document = SarifDocument()
    function_library.get_function_by_function_name(
        "max_connection_by_workmem_is_not_larger_than_memory"
    )(function_library, db, param, context, sarif_document)
    assert sarif_document.sarif_doc.runs[0].results == []
    assert (
        caplog.record_tuples[0][2]
        == "Error: param ram is missing in the configuration file"
    )
