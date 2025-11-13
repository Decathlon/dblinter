import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def reserved_keyword(self, db: DatabaseConnection, _, context, table, sarif_document):

    KeyworldList = [
        "ALL",
        "ANALYSE",
        "ANALYZE",
        "AND",
        "ANY",
        "ARRAY",
        "AS",
        "ASC",
        "ASYMMETRIC",
        "AUTHORIZATION",
        "BINARY",
        "BOTH",
        "CASE",
        "CAST",
        "CHECK",
        "COLLATE",
        "COLLATION",
        "COLUMN",
        "CONCURRENTLY",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT_CATALOG",
        "CURRENT_DATE",
        "CURRENT_ROLE",
        "CURRENT_SCHEMA",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DEFAULT",
        "DEFERRABLE",
        "DESC",
        "DISTINCT",
        "DO",
        "ELSE",
        "END",
        "EXCEPT",
        "FALSE",
        "FETCH",
        "FOR",
        "FOREIGN",
        "FREEZE",
        "FROM",
        "FULL",
        "GRANT",
        "GROUP",
        "HAVING",
        "ILIKE",
        "IN",
        "INITIALLY",
        "INNER",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "LATERAL",
        "LEADING",
        "LEFT",
        "LIKE",
        "LIMIT",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "NATURAL",
        "NOT",
        "NOTNULL",
        "NULL",
        "OFFSET",
        "ON",
        "ONLY",
        "OR",
        "ORDER",
        "OUTER",
        "OVERLAPS",
        "PLACING",
        "PRIMARY",
        "REFERENCES",
        "RETURNING",
        "RIGHT",
        "SELECT",
        "SESSION_USER",
        "SIMILAR",
        "SOME",
        "SYMMETRIC",
        "TABLE",
        "TABLESAMPLE",
        "THEN",
        "TO",
        "TRAILING",
        "TRUE",
        "UNION",
        "UNIQUE",
        "USER",
        "USING",
        "VARIADIC",
        "VERBOSE",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
    ]

    LOGGER.debug("reserved_keyword for %s.%s in db %s", table[0], table[1], db.database)
    # Check table name
    if table[1].upper() in KeyworldList:
        uri = f"{db.database}.{table[0]}.{table[1]}"
        message_args = ("Table", db.database, table[0], table[1], "")
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, uri, context
        )

    # Check column name
    COLUMNS_NAME = f"""SELECT
        attname
    FROM
        pg_attribute pa
    JOIN
        pg_class pc
        ON
        pc.oid = pa.attrelid
    JOIN
        pg_namespace pn
        ON
        pn.oid = pc.relnamespace
    WHERE
        pn.nspname = '{table[0]}'
        AND pn.nspname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
        AND relname = '{table[1]}'
        AND attnum > 0
        """

    list_column_names = db.query(COLUMNS_NAME)
    for each_column_name in list_column_names:
        if each_column_name[0].upper() in KeyworldList:
            # print("reserved_keyword used by column {}.{} in db {}".format(
            # table[0], table[1], db.database
            # ))
            uri = f"{db.database}.{table[0]}.{table[1]}.{each_column_name[0]}"
            message_args = (
                "Column",
                db.database,
                table[0],
                table[1],
                each_column_name[0],
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )

    # Check index name
    INDEXES_NAME = f"""SELECT
        indexname
    FROM
        pg_indexes
    WHERE
        schemaname = '{table[0]}'
        AND tablename = '{table[1]}'
    """
    list_index_names = db.query(INDEXES_NAME)
    for each_index_name in list_index_names:
        if each_index_name[0].upper() in KeyworldList:
            uri = f"{db.database}.{table[0]}.{table[1]}.{each_index_name[0]}"
            message_args = (
                "Index",
                db.database,
                table[0],
                table[1],
                each_index_name[0],
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )

    # Check contraints name
    CONSTRAINT_NAME = f"""SELECT conname FROM pg_catalog.pg_constraint pconstraint
        JOIN pg_class pclass ON pclass.oid = pconstraint.conrelid
        JOIN pg_catalog.pg_namespace pn ON pn."oid" = pconstraint.connamespace
        WHERE pclass.relname = '{table[1]}'
        AND pn.nspname = '{table[0]}'
    """
    list_constraint_names = db.query(CONSTRAINT_NAME)
    for each_constraint_name in list_constraint_names:
        if each_constraint_name[0].upper() in KeyworldList:
            uri = f"{db.database}.{table[0]}.{table[1]}.{each_constraint_name[0]}"
            message_args = (
                "Constraint",
                db.database,
                table[0],
                table[1],
                each_constraint_name[0],
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
