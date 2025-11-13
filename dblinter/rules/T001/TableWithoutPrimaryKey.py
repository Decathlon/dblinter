import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def table_without_primary_key(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_without_primary_key for %s.%s in db %s", table[0], table[1], db.database
    )

    NB_PK = """SELECT count(*) FROM pg_catalog.pg_class pc
        JOIN pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
        JOIN pg_catalog.pg_index pi ON pi.indrelid = pc.oid
        WHERE pi.indisprimary=true AND nspname='{}' AND relname='{}' and nspname NOT IN ('{}')
        """
    uri = f"{db.database}.{table[0]}.{table[1]}"
    pk_count = db.query(NB_PK.format(table[0], table[1], EXCLUDED_SCHEMAS_STR))[0][0]
    if pk_count == 0:
        message_args = (db.database, table[0], table[1])
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, uri, context
        )
