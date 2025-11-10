import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR, extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_redundant_index(
    self, db: DatabaseConnection, param, context, sarif_document
):
    """The number of redundant index."""
    LOGGER.debug("how_many_redundant_index for %s", db.database)
    NB_REDUNDANT_INDEX = f"""SELECT COUNT(*) AS redundant_indexes
    FROM (
        SELECT DISTINCT i1.indexrelid
        FROM pg_index i1, pg_index i2
        WHERE
            i1.indrelid = i2.indrelid
            AND i1.indexrelid != i2.indexrelid
            AND i1.indkey = i2.indkey
            AND EXISTS (
                SELECT 1 FROM pg_indexes pi1
                WHERE
                    pi1.indexname
                    = (
                        SELECT relname FROM pg_class
                        WHERE oid = i1.indexrelid
                    )
                    AND pi1.schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
            )
    ) redundant"""
    # Query to get the list of redundant indexes
    REDUNDANT_INDEX_LIST = f"""
    WITH index_info AS (
        -- This CTE gets the name and order of all columns for each index.
        SELECT
            ind.indrelid AS table_oid,
            ind.indexrelid AS index_oid,
            att.attname AS column_name,
            array_position(ind.indkey, att.attnum) AS column_order
        FROM pg_index ind
        JOIN pg_attribute att ON att.attrelid = ind.indrelid AND att.attnum = ANY(ind.indkey)
        WHERE NOT ind.indisexclusion
    ),
    indexed_columns AS (
        -- This CTE aggregates the columns for each index into an ordered string.
        SELECT
            table_oid,
            index_oid,
            string_agg(column_name, ',' ORDER BY column_order) AS indexed_columns_string
        FROM index_info
        GROUP BY table_oid, index_oid
    ),
    table_info AS (
        -- Joins to pg_class and pg_namespace to get table names and schema names.
        SELECT
            oid AS table_oid,
            relname AS tablename,
            relnamespace
        FROM pg_class
    )
    SELECT
        pg_namespace.nspname::TEXT ||'.'||table_info.tablename::TEXT||
        ' idx '||redundant_index.relname::TEXT||
        ' columns ('||i1.indexed_columns_string||') is a subset of '||
        'idx '||superset_index.relname::TEXT||
        ' columns ('||i2.indexed_columns_string||')' AS problematic_object
    FROM indexed_columns AS i1
    JOIN indexed_columns AS i2 ON i1.table_oid = i2.table_oid
    JOIN pg_class redundant_index ON i1.index_oid = redundant_index.oid
    JOIN pg_class superset_index ON i2.index_oid = superset_index.oid
    JOIN table_info ON i1.table_oid = table_info.table_oid
    JOIN pg_namespace ON table_info.relnamespace = pg_namespace.oid
    WHERE
        pg_namespace.nspname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
        AND redundant_index.oid <> superset_index.oid -- Ensure the indexes are not the same
        -- Checks if the smaller index's column string is a prefix of the larger index's string.
        AND i2.indexed_columns_string LIKE i1.indexed_columns_string || '%'
    ORDER BY 1
    """
    NB_INDEX = f"""SELECT count(*) FROM pg_indexes
        WHERE
        schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')"""
    total_number_of_index = db.query(NB_INDEX)[0][0]
    try:
        number_of_redundant_index = db.query(NB_REDUNDANT_INDEX)[0][0]
        redundant_index_list = db.query(REDUNDANT_INDEX_LIST)
        # Format the list for readability: (schema, table, column, amname, indkey)
        # Build a comma-separated string for each redundant index
        redundant_index_str = ";".join(f"{row[0]}" for row in redundant_index_list)
    except IndexError:
        number_of_redundant_index = 0
        redundant_index_str = ""
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    try:
        if int(number_of_redundant_index / total_number_of_index * 100) > warning:
            message_args = (number_of_redundant_index, warning, redundant_index_str)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
