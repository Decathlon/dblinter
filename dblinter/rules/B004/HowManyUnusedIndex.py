import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR, extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_table_with_unused_index(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("how_many_table_with_unused_index for %s", db.database)
    size_mini_bytes = int(extract_param(param, "size_mo")) * 1024 * 1024
    NB_TABLE_WITH_UNUSED_IDX = f"""SELECT count(distinct(tablename))
        FROM pg_stat_user_indexes AS idstat
        join pg_indexes
        ON
        indexrelname = indexname
        AND
        idstat.schemaname = pg_indexes.schemaname
        WHERE pg_indexes.schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}') AND
        indexdef !~* 'unique' AND
        pg_relation_size(indexrelid)> {size_mini_bytes}"""

    NB_TABLE_TABLE = f"""SELECT count(*)
        FROM pg_catalog.pg_tables pt
        WHERE schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')"""
    total_number_of_table = db.query(NB_TABLE_TABLE)[0][0]
    number_of_table_with_unused_idx = db.query(NB_TABLE_WITH_UNUSED_IDX)[0][0]
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    try:
        tx = int(number_of_table_with_unused_idx / total_number_of_table * 100)
        if tx > warning:
            message_args = (number_of_table_with_unused_idx, warning)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
