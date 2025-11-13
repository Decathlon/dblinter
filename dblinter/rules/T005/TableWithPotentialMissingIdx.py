import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR, extract_param

LOGGER = logging.getLogger("dblinter")


def table_with_missing_index(
    self, db: DatabaseConnection, param, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_missing_index for %s.%s in db %s", table[0], table[1], db.database
    )
    STAT_USAGE = f""" SELECT schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        seq_tup_read / seq_scan AS avg
        FROM pg_stat_user_tables
        WHERE seq_scan > 0 and
        schemaname='{table[0]}' and relname='{table[1]}' and schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
        """
    uri = f"{db.database}.{table[0]}.{table[1]}"
    threshold = int(extract_param(param, "threshold"))

    seq_scan_count = db.query(STAT_USAGE)

    if seq_scan_count:
        seq_scan_size = int(seq_scan_count[0][5])
        if seq_scan_size >= threshold:
            message_args = (table[1], threshold)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
