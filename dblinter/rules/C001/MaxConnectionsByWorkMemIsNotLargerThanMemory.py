import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import extract_param

LOGGER = logging.getLogger("dblinter")


def max_connection_by_workmem_is_not_larger_than_memory(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("max_connection_by_workmem_is_not_larger_than_memory")
    work_mem = db.query(
        "SELECT setting FROM pg_catalog.pg_settings WHERE name='work_mem'"
    )[0][0]
    max_connections = db.query(
        "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_connections'"
    )[0][0]
    ram = extract_param(param, "ram")

    if ram is not None:
        if (int(work_mem) * int(max_connections)) > (int(ram) / 1000):
            message_args = "work_mem * max_connections is bigger than ram"
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, "cluster", context
            )
    else:
        LOGGER.error("Error: param ram is missing in the configuration file")
