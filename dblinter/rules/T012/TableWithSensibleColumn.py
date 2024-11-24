import logging

from dblinter.database_connection import DatabaseConnection

LOGGER = logging.getLogger("dblinter")


def table_with_sensible_column(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_sensible_column for %s.%s in db %s", table[0], table[1], db.database
    )
    SENSITIVE_COLS = f"""with coltable as (SELECT column_name,
            identifiers_category from
            anon.detect('en_US')
            join pg_class c on oid=table_name
            where c.relname='{table[1]}'
            union
            SELECT column_name,
            identifiers_category from
            anon.detect('fr_FR')
            join pg_class c on oid=table_name
            where c.relname='{table[1]}')
            select distinct column_name,identifiers_category from coltable
        """

    uri = f"{db.database}.{table[0]}.{table[1]}"
    sensitive_cols = db.query(SENSITIVE_COLS)
    if sensitive_cols:
        for elt in sensitive_cols:
            message_args = (
                uri,
                elt[0],
                elt[1]
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
