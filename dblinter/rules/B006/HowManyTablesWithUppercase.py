import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR, extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_tables_with_uppercase(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("how_many_tables_with_uppercase for %s", db.database)

    NB_TABLE_AND_COLS_WITH_UPPERCASE = f"""SELECT count(distinct table_name)
        FROM information_schema.columns
        WHERE table_schema not in ('{EXCLUDED_SCHEMAS_STR}')
        AND (lower(table_name) <> table_name
        OR  lower(column_name) <> column_name)"""
    # Query to get schemaname and tablename for tables with uppercase
    TABLES_WITH_UPPERCASE = f"""
        SELECT DISTINCT table_schema, table_name
        FROM information_schema.columns
        WHERE table_schema not in ('{EXCLUDED_SCHEMAS_STR}')
        AND (lower(table_name) <> table_name
        OR  lower(column_name) <> column_name)
    """

    NB_TABLE_TABLE = f"""SELECT count(*)
        FROM pg_catalog.pg_tables pt
        WHERE schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')"""
    total_number_of_table = db.query(NB_TABLE_TABLE)[0][0]
    number_of_table_with_uppercase = db.query(NB_TABLE_AND_COLS_WITH_UPPERCASE)[0][0]
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    tables_with_uppercase_rows = db.query(TABLES_WITH_UPPERCASE)
    tables_with_uppercase_str = ";".join(
        f"{row[0]}.{row[1]}" for row in tables_with_uppercase_rows
    )
    try:
        if (
            int((number_of_table_with_uppercase) / total_number_of_table * 100)
            > warning
        ):
            message_args = (
                number_of_table_with_uppercase,
                warning,
                tables_with_uppercase_str,
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
