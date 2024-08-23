import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import extract_param

LOGGER = logging.getLogger("dblinter")


def table_with_unused_index(
    self, db: DatabaseConnection, param, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_missing_index for %s.%s in db %s", table[0], table[1], db.database
    )
    size_mini_bytes = int(extract_param(param, "size_mo")) * 1024 * 1024
    IDX_STAT_USAGE = f"""SELECT
        tablename,
        indexname,
        pg_relation_size(indexrelid) AS index_size
    FROM
        pg_stat_user_indexes AS idstat
    JOIN
        pg_indexes
        ON
        indexrelname = indexname
        AND
        idstat.schemaname = pg_indexes.schemaname
    JOIN
        pg_stat_user_tables AS tabstat
        ON
        idstat.relid = tabstat.relid
    WHERE
        indexdef !~* 'unique'
        and idstat.idx_scan = 0
        and pg_indexes.schemaname = '{table[0]}'
        and idstat.relname = '{table[1]}'
        and pg_relation_size(indexrelid)> {size_mini_bytes}
        """
    uri = f"{db.database}.{table[0]}.{table[1]}"
    idx_scan_count = db.query(IDX_STAT_USAGE)

    if idx_scan_count:
        for elt in idx_scan_count:
            idx_scan_size = round(elt[2] / 1024 / 1024)
            message_args = (elt[1], uri, idx_scan_size)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
