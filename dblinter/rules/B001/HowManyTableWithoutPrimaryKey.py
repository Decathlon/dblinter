import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_table_without_primary_key(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("how_many_table_without_primary_key for %s", db.database)
    NB_TABLE_WITH_PK = """SELECT count(distinct(pg_class.relname))
        FROM pg_index, pg_class, pg_attribute, pg_namespace
        WHERE indrelid = pg_class.oid AND
        nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema') AND
        pg_class.relnamespace = pg_namespace.oid AND
        pg_attribute.attrelid = pg_class.oid AND
        pg_attribute.attnum = any(pg_index.indkey)
        AND indisprimary"""

    NB_TABLE_TABLE = """SELECT count(*)
        FROM pg_catalog.pg_tables pt
        WHERE schemaname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')"""
    total_number_of_table = db.query(NB_TABLE_TABLE)[0][0]
    number_of_table_with_pk = db.query(NB_TABLE_WITH_PK)[0][0]
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    try:
        if (
            int(
                (total_number_of_table - number_of_table_with_pk)
                / total_number_of_table
                * 100
            )
            > warning
        ):
            message_args = (total_number_of_table - number_of_table_with_pk, warning)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
