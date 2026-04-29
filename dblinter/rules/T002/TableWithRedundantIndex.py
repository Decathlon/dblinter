import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def table_with_redundant_index(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_redundant_index for %s.%s in db %s", table[0], table[1], db.database
    )
    REDUNDANT_INDEX_QUERY = f"""
        WITH IndexDetails AS (
            SELECT
                indrelid::regclass AS table_name,
                indexrelid::regclass AS index_name,
                regexp_replace(
                    pg_get_indexdef(indexrelid),
                    '.*USING [^ ]+ \\((.*?)\\)',
                    '\\1'
                ) AS column_list,
                indpred AS filter_node
            FROM pg_index
            JOIN pg_class c ON c.oid = indrelid
            JOIN pg_namespace ns ON ns.oid = c.relnamespace
            WHERE ns.nspname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
              AND ns.nspname = '{table[0]}'
              AND c.relname = '{table[1]}'
        ),
        DuplicateGroups AS (
            SELECT
                table_name,
                column_list,
                min(index_name::text) AS primary_index,
                string_agg(index_name::text, ', ' ORDER BY index_name::text) AS redundant_indexes,
                count(*) AS total_duplicates
            FROM IndexDetails
            GROUP BY table_name, column_list, filter_node
            HAVING count(*) > 1
        )
        SELECT
            table_name,
            column_list,
            primary_index,
            replace(redundant_indexes, primary_index || ', ', '') AS duplicates_to_drop
        FROM DuplicateGroups
        ORDER BY table_name
    """
    uri = f"{db.database}.{table[0]}.{table[1]}"
    results = db.query(REDUNDANT_INDEX_QUERY)
    if results:
        for row in results:
            _, column_list, primary_index, duplicates_to_drop = row
            message_args = (
                db.database,
                table[0],
                table[1],
                column_list,
                primary_index,
                duplicates_to_drop,
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
