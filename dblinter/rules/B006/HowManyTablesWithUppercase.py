import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_tables_with_uppercase(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("how_many_tables_with_uppercase for %s", db.database)

    NB_TABLE_AND_COLS_WITH_UPPERCASE = """SELECT count(distinct table_name)
        FROM information_schema.columns
        WHERE table_schema not in ('pg_toast', 'pg_catalog', 'information_schema')
        AND (lower(table_name) <> table_name
        OR  lower(column_name) <> column_name)"""

    NB_TABLE_TABLE = """SELECT count(*)
        FROM pg_catalog.pg_tables pt
        WHERE schemaname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')"""
    total_number_of_table = db.query(NB_TABLE_TABLE)[0][0]
    number_of_table_with_uppercase = db.query(NB_TABLE_AND_COLS_WITH_UPPERCASE)[0][0]
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    try:
        if (
            int((number_of_table_with_uppercase) / total_number_of_table * 100)
            > warning
        ):
            message_args = (number_of_table_with_uppercase, warning)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
