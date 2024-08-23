import logging

from dblinter.database_connection import DatabaseConnection

LOGGER = logging.getLogger("dblinter")


def table_without_index(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_without_index for %s.%s in db %s", table[0], table[1], db.database
    )
    NB_TABLE_WITHOUT_INDEX = """SELECT count(*) FROM pg_catalog.pg_indexes WHERE schemaname='{}' AND tablename='{}'"""

    uri = f"{db.database}.{table[0]}.{table[1]}"
    index_count = db.query(NB_TABLE_WITHOUT_INDEX.format(table[0], table[1]))[0][0]
    if index_count == 0:
        message_args = (db.database, table[0], table[1])
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, uri, context
        )
