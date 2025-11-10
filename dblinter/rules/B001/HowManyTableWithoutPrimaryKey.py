import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR, extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_table_without_primary_key(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("how_many_table_without_primary_key for %s", db.database)
    NB_TABLE_WITHOUT_PK = f"""SELECT
    count(1)
    FROM
        pg_class c
    JOIN
        pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN
        pg_index i ON i.indrelid = c.oid AND i.indisprimary
    WHERE
        n.nspname NOT IN ('{EXCLUDED_SCHEMAS_STR}') -- Exclude system schemas
        AND c.relkind = 'r' -- Only include regular tables
        AND i.indrelid IS NULL"""

    NB_TABLE_TABLE = f"""SELECT count(*)
        FROM pg_catalog.pg_tables pt
        WHERE schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')"""
    total_number_of_table = db.query(NB_TABLE_TABLE)[0][0]
    number_of_table_without_pk = db.query(NB_TABLE_WITHOUT_PK)[0][0]
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    # Query to get the list of tables without primary key
    TABLES_WITHOUT_PK = f"""
        SELECT pt.schemaname, pt.tablename
        FROM pg_catalog.pg_tables pt
        WHERE pt.schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
        AND pt.tablename NOT IN (
            SELECT DISTINCT(pg_class.relname)
            FROM pg_index, pg_class, pg_attribute, pg_namespace
            WHERE indrelid = pg_class.oid AND
            nspname NOT IN ('{EXCLUDED_SCHEMAS_STR}') AND
            pg_class.relnamespace = pg_namespace.oid AND
            pg_attribute.attrelid = pg_class.oid AND
            pg_attribute.attnum = any(pg_index.indkey)
            AND indisprimary
        )
    """
    tables_without_pk_rows = db.query(TABLES_WITHOUT_PK)
    tables_without_pk_str = ";".join(
        f"{row[0]}.{row[1]}" for row in tables_without_pk_rows
    )
    try:
        if int((number_of_table_without_pk) / total_number_of_table * 100) > warning:
            message_args = (
                number_of_table_without_pk,
                warning,
                tables_without_pk_str,
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
