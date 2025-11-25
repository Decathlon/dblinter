import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def table_with_uppercase(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_uppercase for %s.%s in db %s", table[0], table[1], db.database
    )
    NB_UPPERCASE_COLS = f"""SELECT count(distinct table_name)
        FROM information_schema.columns
        WHERE table_schema NOT IN ('{EXCLUDED_SCHEMAS_STR}') AND table_schema='{table[0]}' AND table_name='{table[1]}'
        AND (lower(table_name) <> table_name
        OR  lower(column_name) <> column_name)
        """

    uri = f"{db.database}.{table[0]}.{table[1]}"
    uc_count = db.query(NB_UPPERCASE_COLS)[0][0]
    if uc_count == 1:
        message_args = (db.database, table[0], table[1])
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, uri, context
        )
